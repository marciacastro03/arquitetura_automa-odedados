"""
================================================================================
DAG: anomalias_contratos_ceara
Aula 3 — MBA Ciência de Dados | Arquitetura e Automação de Pipeline de Dados
Prof. Daniel Teófilo — Universidade de Fortaleza (UNIFOR)
================================================================================

PROBLEMÁTICA (solicitação da área de negócio):
    A Controladoria-Geral do Estado solicitou ao time de dados um pipeline
    automatizado capaz de identificar, diariamente, contratos públicos com
    comportamento financeiro atípico. O critério não é uma regra fixa de valor
    — é detectar padrões que fujam do comportamento histórico do conjunto,
    permitindo priorizar auditorias humanas onde o risco é maior.

SOLUÇÃO TÉCNICA:
    Pipeline ETL orquestrado pelo Apache Airflow composto por 4 tasks:
      1. extrair_contratos   → Coleta dados da API paginada do Ceará Transparente
      2. salvar_postgres     → Persiste os contratos brutos no PostgreSQL
      3. detectar_anomalias  → Aplica Isolation Forest sobre features numéricas
      4. salvar_anomalias    → Salva contratos anômalos com score de risco

FONTE DE DADOS:
    https://api-dados-abertos.cearatransparente.ce.gov.br/
    Endpoint: /transparencia/contratos/contratos

DEPENDÊNCIAS (instalar no ambiente Airflow):
    pip install scikit-learn pandas psycopg2-binary requests numpy
================================================================================
"""

# ─────────────────────────────────────────────────────────────────────────────
# IMPORTS PADRÃO
# ─────────────────────────────────────────────────────────────────────────────
import logging
import json
from datetime import datetime, timedelta

import numpy as np
import pandas as pd
import psycopg2
import requests
from psycopg2.extras import execute_values
from sklearn.ensemble import IsolationForest
from sklearn.preprocessing import StandardScaler

from airflow import DAG
from airflow.operators.python import PythonOperator

# Logger padrão do Airflow — os logs aparecem na interface web
logger = logging.getLogger(__name__)


# ─────────────────────────────────────────────────────────────────────────────
# CONFIGURAÇÕES GLOBAIS
# ─────────────────────────────────────────────────────────────────────────────

# Conexão com o PostgreSQL
# Ajuste host/porta/credenciais conforme seu ambiente Docker
DB_CONFIG = {
    "host": "host.docker.internal",  # ou "localhost" fora do Docker
    "port": 5432,
    "database": "aula",
    "user": "postgres",
    "password": "1234",
}

# URL base da API do Ceará Transparente
API_BASE_URL = (
    "https://api-dados-abertos.cearatransparente.ce.gov.br"
    "/transparencia/contratos/contratos"
)

# Janela de datas: busca contratos assinados nos últimos 30 dias
# Em produção, você pode parametrizar isso via Airflow Variables
DIAS_RETROATIVOS = 30

# Parâmetro de contaminação do Isolation Forest
# 0.05 = esperamos que ~5% dos contratos sejam anômalos
# Ajuste conforme o contexto do negócio (entre 0.01 e 0.5)
CONTAMINACAO = 0.05


# ─────────────────────────────────────────────────────────────────────────────
# FUNÇÕES AUXILIARES
# ─────────────────────────────────────────────────────────────────────────────

def get_db_connection():
    """
    Retorna uma conexão psycopg2 com o PostgreSQL.
    Centralizar aqui facilita trocar a configuração em um só lugar.
    """
    return psycopg2.connect(**DB_CONFIG)


def criar_tabelas_se_nao_existirem():
    """
    Cria as tabelas necessárias caso não existam.
    Usar IF NOT EXISTS torna a operação idempotente (pode rodar N vezes sem erro).
    """
    ddl_contratos = """
        CREATE TABLE IF NOT EXISTS contratos (
            id                  BIGSERIAL PRIMARY KEY,
            numero_contrato     TEXT,
            objeto              TEXT,
            fornecedor_nome     TEXT,
            fornecedor_cnpj     TEXT,
            orgao_nome          TEXT,
            modalidade          TEXT,
            valor_inicial       NUMERIC(18, 2),
            valor_global        NUMERIC(18, 2),
            data_assinatura     DATE,
            data_inicio_vigencia DATE,
            data_fim_vigencia    DATE,
            prazo_vigencia_dias  INTEGER,       -- feature derivada: duração em dias
            json_original        JSONB,          -- dado bruto completo para rastreabilidade
            inserido_em          TIMESTAMP DEFAULT NOW(),
            UNIQUE(numero_contrato, data_assinatura)  -- evita duplicatas em re-execuções
        );
    """

    ddl_anomalias = """
        CREATE TABLE IF NOT EXISTS anomalias_contratos (
            id                  BIGSERIAL PRIMARY KEY,
            numero_contrato     TEXT,
            objeto              TEXT,
            fornecedor_nome     TEXT,
            orgao_nome          TEXT,
            valor_global        NUMERIC(18, 2),
            prazo_vigencia_dias INTEGER,
            score_anomalia      NUMERIC(10, 6),  -- quanto mais negativo, mais anômalo
            percentil_risco     INTEGER,          -- 0-100; 100 = mais anômalo
            nivel_risco         TEXT,             -- ALTO / MÉDIO / BAIXO
            data_assinatura     DATE,
            detectado_em        TIMESTAMP DEFAULT NOW()
        );
    """

    with get_db_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(ddl_contratos)
            cur.execute(ddl_anomalias)
        conn.commit()
    logger.info("Tabelas verificadas/criadas com sucesso.")


# ─────────────────────────────────────────────────────────────────────────────
# TASK 1 — EXTRAÇÃO
# ─────────────────────────────────────────────────────────────────────────────

def extrair_contratos(**context):
    """
    Consome a API paginada do Ceará Transparente e retorna todos os contratos
    dentro da janela de datas configurada.

    CONCEITOS ABORDADOS:
      - Paginação de APIs REST (parâmetro `page`)
      - Tratamento de erros HTTP
      - Uso do XCom para passar dados entre tasks no Airflow
      - Backoff simples para resiliência

    O XCom (Cross-Communication) é o mecanismo do Airflow para tasks
    trocarem dados entre si. Dados pequenos (< 1MB) podem ser passados
    diretamente; dados grandes devem ir para um banco ou S3.
    """

    # Calcula o intervalo de datas dinamicamente
    hoje = datetime.now()
    data_fim = hoje.strftime("%d/%m/%Y")
    data_inicio = (hoje - timedelta(days=DIAS_RETROATIVOS)).strftime("%d/%m/%Y")

    logger.info(f"Buscando contratos de {data_inicio} até {data_fim}")

    todos_contratos = []
    pagina_atual = 1
    total_paginas = None  # Será descoberto na primeira requisição

    while True:
        params = {
            "page": pagina_atual,
            "data_assinatura_inicio": data_inicio,
            "data_assinatura_fim": data_fim,
        }

        try:
            response = requests.get(
                API_BASE_URL,
                params=params,
                timeout=30,  # segundos — importante para não travar o worker
            )
            response.raise_for_status()  # levanta exceção para status 4xx/5xx

        except requests.exceptions.Timeout:
            logger.error(f"Timeout na página {pagina_atual}. Encerrando extração.")
            break
        except requests.exceptions.HTTPError as e:
            logger.error(f"Erro HTTP {e.response.status_code} na página {pagina_atual}: {e}")
            break

        dados = response.json()

        # A API retorna um envelope com metadados de paginação
        # Estrutura típica: { "data": [...], "meta": { "total": N, "last_page": M } }
        registros = dados.get("data", [])
        meta = dados.get("sumary", {})  # API retorna "sumary" (sem 'm' duplo)

        if total_paginas is None:
            total_paginas = meta.get("total_pages", 1)
            total_registros = meta.get("total_records", 0)
            logger.info(
                f"Total de registros: {total_registros} | "
                f"Total de páginas: {total_paginas}"
            )

        todos_contratos.extend(registros)
        logger.info(
            f"Página {pagina_atual}/{total_paginas} — "
            f"{len(registros)} registros coletados"
        )

        # Condição de parada: última página atingida
        if pagina_atual >= total_paginas or not registros:
            break

        pagina_atual += 1

    logger.info(f"Extração concluída: {len(todos_contratos)} contratos no total.")

    # Empurra os dados para o XCom — serão recuperados pela próxima task
    # ti = Task Instance (objeto que o Airflow injeta via **context)
    context["ti"].xcom_push(key="contratos_raw", value=todos_contratos)

    return len(todos_contratos)  # retorno também vai para XCom (key='return_value')


# ─────────────────────────────────────────────────────────────────────────────
# TASK 2 — ARMAZENAMENTO
# ─────────────────────────────────────────────────────────────────────────────

def salvar_postgres(**context):
    """
    Recupera os contratos do XCom e os persiste no PostgreSQL.

    CONCEITOS ABORDADOS:
      - execute_values: inserção em lote (muito mais rápido que INSERT um a um)
      - ON CONFLICT DO NOTHING: idempotência (re-executar não gera duplicatas)
      - Derivação de features: calcular prazo_vigencia_dias a partir de datas
    """

    # Recupera os dados que a task anterior colocou no XCom
    ti = context["ti"]
    contratos_raw = ti.xcom_pull(task_ids="extrair_contratos", key="contratos_raw")

    if not contratos_raw:
        logger.warning("Nenhum contrato recebido para salvar.")
        return 0

    criar_tabelas_se_nao_existirem()

    # Prepara os registros para inserção em lote
    registros = []
    for c in contratos_raw:
        # ── Parsing e limpeza de datas ────────────────────────────────────────
        def parse_data(valor):
            """Tenta converter string de data para objeto date.
            Trata formatos: 'YYYY-MM-DD', 'DD/MM/YYYY' e ISO com timezone
            (ex: '2024-01-02T00:00:00.000-03:00').
            """
            if not valor:
                return None
            s = str(valor)
            # ISO com timestamp: pega apenas a parte da data
            if "T" in s:
                s = s.split("T")[0]
            for fmt in ("%Y-%m-%d", "%d/%m/%Y"):
                try:
                    d = datetime.strptime(s, fmt).date()
                    # Descarta datas inválidas como "0001-01-01"
                    if d.year < 1900:
                        return None
                    return d
                except ValueError:
                    continue
            return None

        data_assinatura = parse_data(c.get("data_assinatura"))
        data_inicio = parse_data(c.get("data_inicio"))
        data_fim = parse_data(c.get("data_termino"))

        # ── Feature derivada: duração do contrato em dias ─────────────────────
        # Essa é uma das principais features para o modelo de anomalia
        prazo_dias = None
        if data_inicio and data_fim and data_fim > data_inicio:
            prazo_dias = (data_fim - data_inicio).days

        # ── Parsing de valores monetários ─────────────────────────────────────
        def parse_valor(v):
            """Remove formatação BR e converte para float."""
            if v is None:
                return None
            try:
                return float(str(v).replace("R$", "").replace(".", "").replace(",", ".").strip())
            except (ValueError, AttributeError):
                return None

        # valor_contrato = valor original; valor_atualizado_concedente = com aditivos
        valor_inicial = parse_valor(c.get("valor_contrato"))
        valor_global = parse_valor(c.get("valor_atualizado_concedente") or c.get("valor_contrato"))

        registros.append((
            c.get("num_contrato"),
            c.get("descricao_objeto"),
            c.get("descricao_nome_credor"),
            c.get("plain_cpf_cnpj_financiador") or c.get("cpf_cnpj_financiador"),
            c.get("cod_orgao"),
            c.get("descricao_modalidade"),
            valor_inicial,
            valor_global,
            data_assinatura,
            data_inicio,
            data_fim,
            prazo_dias,
            json.dumps(c, ensure_ascii=False),  # serializa o JSON original
        ))

    # ── Inserção em lote com ON CONFLICT para idempotência ────────────────────
    sql = """
        INSERT INTO contratos (
            numero_contrato, objeto, fornecedor_nome, fornecedor_cnpj,
            orgao_nome, modalidade, valor_inicial, valor_global,
            data_assinatura, data_inicio_vigencia, data_fim_vigencia,
            prazo_vigencia_dias, json_original
        ) VALUES %s
        ON CONFLICT (numero_contrato, data_assinatura) DO NOTHING
    """

    with get_db_connection() as conn:
        with conn.cursor() as cur:
            execute_values(cur, sql, registros, page_size=500)
            inseridos = cur.rowcount
        conn.commit()

    logger.info(
        f"{len(registros)} contratos processados | "
        f"{inseridos} novos inseridos no PostgreSQL."
    )

    return inseridos


# ─────────────────────────────────────────────────────────────────────────────
# TASK 3 — DETECÇÃO DE ANOMALIAS
# ─────────────────────────────────────────────────────────────────────────────

def detectar_anomalias(**context):
    """
    Aplica Isolation Forest sobre os contratos dos últimos 90 dias para
    identificar comportamentos financeiros atípicos.

    CONCEITOS ABORDADOS:
      - Isolation Forest: algoritmo não supervisionado baseado em árvores
        que isola anomalias por serem pontos "fáceis de separar" do restante
      - StandardScaler: normalização das features (necessário pois valor e
        prazo têm escalas muito diferentes)
      - Score de anomalia: quanto mais negativo, mais isolado/anômalo
      - Percentil de risco: transformamos o score em percentil 0-100 para
        facilitar comunicação com o negócio

    POR QUE ISOLATION FOREST?
      - Não precisa de dados rotulados (não supervisionado)
      - Eficiente em grandes volumes (complexidade O(n log n))
      - Funciona bem com features numéricas de escalas diferentes
      - Resistente a outliers extremos (não se baseia em distância)
    """

    logger.info("Carregando contratos do PostgreSQL para detecção de anomalias...")

    # Carrega contratos dos últimos 90 dias — janela maior garante modelo mais robusto
    sql_leitura = """
        SELECT
            id,
            numero_contrato,
            objeto,
            fornecedor_nome,
            orgao_nome,
            valor_global,
            valor_inicial,
            prazo_vigencia_dias,
            data_assinatura,
            modalidade
        FROM contratos
        WHERE
            data_assinatura >= CURRENT_DATE - INTERVAL '90 days'
            AND valor_global IS NOT NULL
            AND valor_global > 0
        ORDER BY data_assinatura DESC
    """

    with get_db_connection() as conn:
        df = pd.read_sql(sql_leitura, conn)

    logger.info(f"Contratos carregados para análise: {len(df)} registros")

    if len(df) < 10:
        logger.warning(
            "Poucos contratos para treinar o modelo (mínimo recomendado: 10). "
            "Abortando detecção."
        )
        return 0

    # ── Feature Engineering ───────────────────────────────────────────────────
    # Criamos features que representam o "comportamento" financeiro do contrato

    # Feature 1: valor_global (já existe)
    # Feature 2: prazo_vigencia_dias (já existe)
    # Feature 3: valor por dia — detecta contratos baratos/caros por unidade de tempo
    df["valor_por_dia"] = df.apply(
        lambda row: row["valor_global"] / row["prazo_vigencia_dias"]
        if row["prazo_vigencia_dias"] and row["prazo_vigencia_dias"] > 0
        else row["valor_global"],
        axis=1,
    )

    # Feature 4: log do valor (reduz a influência de outliers extremos no treinamento)
    # log1p = log(1 + x), evita log(0)
    df["log_valor_global"] = np.log1p(df["valor_global"])
    df["log_valor_por_dia"] = np.log1p(df["valor_por_dia"])

    # Seleciona as features para o modelo
    features_modelo = [
        "log_valor_global",    # magnitude do contrato (escala logarítmica)
        "log_valor_por_dia",   # eficiência financeira diária (escala logarítmica)
        "prazo_vigencia_dias", # duração do contrato
    ]

    # Remove linhas com NaN nas features (contratos sem prazo definido)
    df_modelo = df[features_modelo + ["id", "numero_contrato"]].dropna()
    X = df_modelo[features_modelo].values

    logger.info(
        f"Features: {features_modelo} | "
        f"Contratos válidos para o modelo: {len(X)}"
    )

    # ── Normalização ──────────────────────────────────────────────────────────
    # StandardScaler: transforma cada feature para média=0, desvio=1
    # Necessário porque valor (R$ milhões) e prazo (dias) têm escalas muito diferentes
    scaler = StandardScaler()
    X_scaled = scaler.fit_transform(X)

    # ── Treinamento do Isolation Forest ──────────────────────────────────────
    modelo = IsolationForest(
        n_estimators=200,        # número de árvores — mais = mais estável, mais lento
        contamination=CONTAMINACAO,  # fração esperada de anomalias
        random_state=42,         # reprodutibilidade
        n_jobs=-1,               # usa todos os núcleos disponíveis
    )
    modelo.fit(X_scaled)

    # ── Scoring ───────────────────────────────────────────────────────────────
    # predict: 1 = normal, -1 = anomalia
    # score_samples: quanto mais negativo = mais anômalo
    df_modelo = df_modelo.copy()
    df_modelo["predicao"] = modelo.predict(X_scaled)
    df_modelo["score_anomalia"] = modelo.score_samples(X_scaled)

    # Junta as informações originais ao resultado
    # Nota: prazo_vigencia_dias NÃO é incluído aqui porque já existe em df_modelo;
    # incluir causaria colunas duplicadas (_x/_y) e quebraria o acesso posterior.
    df_resultado = df_modelo.merge(
        df[["id", "objeto", "fornecedor_nome", "orgao_nome",
            "valor_global", "data_assinatura"]],
        on="id",
        how="left",
    )

    # Filtra apenas os contratos marcados como anômalos pelo modelo
    df_anomalias = df_resultado[df_resultado["predicao"] == -1].copy()

    # ── Percentil de risco ────────────────────────────────────────────────────
    # O percentil é calculado ENTRE AS ANOMALIAS, não sobre todo o conjunto.
    # Motivo: as anomalias são por definição o top ~5% do modelo (contamination=0.05),
    # então se calculado sobre todos os contratos elas ficariam todas acima do
    # percentil 95 → todas "ALTO". Ranquear entre si distribui ALTO/MÉDIO/BAIXO
    # de forma significativa para priorização de auditorias.
    scores_invertidos = -df_anomalias["score_anomalia"]
    df_anomalias["percentil_risco"] = (
        scores_invertidos.rank(pct=True) * 100
    ).astype(int)

    # ── Classificação de risco ────────────────────────────────────────────────
    def classificar_risco(percentil):
        if percentil >= 90:
            return "ALTO"
        elif percentil >= 70:
            return "MÉDIO"
        else:
            return "BAIXO"

    df_anomalias["nivel_risco"] = df_anomalias["percentil_risco"].apply(classificar_risco)

    qtd_anomalias = len(df_anomalias)
    logger.info(
        f"Anomalias detectadas: {qtd_anomalias} de {len(df_modelo)} contratos analisados "
        f"({qtd_anomalias/len(df_modelo)*100:.1f}%)"
    )

    # Passa o resultado para a próxima task via XCom
    context["ti"].xcom_push(
        key="anomalias",
        value=df_anomalias.to_dict(orient="records"),
    )

    return qtd_anomalias


# ─────────────────────────────────────────────────────────────────────────────
# TASK 4 — SALVAR ANOMALIAS
# ─────────────────────────────────────────────────────────────────────────────

def salvar_anomalias(**context):
    """
    Persiste os contratos anômalos na tabela anomalias_contratos.

    A cada execução, a tabela é limpa e recarregada (truncate + insert).
    Isso garante que reclassificações do modelo reflitam sempre o estado atual.

    ALTERNATIVA PRODUÇÃO: usar INSERT ... ON CONFLICT com data de detecção
    para manter histórico de quando cada contrato foi considerado anômalo.
    """

    ti = context["ti"]
    anomalias = ti.xcom_pull(task_ids="detectar_anomalias", key="anomalias")

    if not anomalias:
        logger.info("Nenhuma anomalia para salvar.")
        return 0

    registros = [
        (
            a.get("numero_contrato"),
            a.get("objeto"),
            a.get("fornecedor_nome"),
            a.get("orgao_nome"),
            a.get("valor_global"),
            a.get("prazo_vigencia_dias"),
            float(a.get("score_anomalia", 0)),
            int(a.get("percentil_risco", 0)),
            a.get("nivel_risco"),
            a.get("data_assinatura"),
        )
        for a in anomalias
    ]

    sql_truncate = "TRUNCATE TABLE anomalias_contratos RESTART IDENTITY;"

    sql_insert = """
        INSERT INTO anomalias_contratos (
            numero_contrato, objeto, fornecedor_nome, orgao_nome,
            valor_global, prazo_vigencia_dias, score_anomalia,
            percentil_risco, nivel_risco, data_assinatura
        ) VALUES %s
    """

    with get_db_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(sql_truncate)
            execute_values(cur, sql_insert, registros)
        conn.commit()

    # Resumo por nível de risco para o log
    df = pd.DataFrame(anomalias)
    resumo = df["nivel_risco"].value_counts().to_dict() if "nivel_risco" in df else {}
    logger.info(
        f"{len(registros)} anomalias salvas. "
        f"Resumo por risco: ALTO={resumo.get('ALTO', 0)}, "
        f"MÉDIO={resumo.get('MÉDIO', 0)}, "
        f"BAIXO={resumo.get('BAIXO', 0)}"
    )

    return len(registros)


# ─────────────────────────────────────────────────────────────────────────────
# DEFINIÇÃO DA DAG
# ─────────────────────────────────────────────────────────────────────────────

with DAG(
    dag_id="anomalias_contratos_ceara",
    description=(
        "Pipeline diário: extrai contratos da API do Ceará Transparente, "
        "armazena no PostgreSQL e detecta anomalias financeiras com Isolation Forest."
    ),
    schedule="0 6 * * *",       # todo dia às 06:00 UTC
    start_date=datetime(2025, 1, 1),
    catchup=False,               # não executa datas passadas retroativamente
    tags=["contratos", "anomalias", "ceara-transparente", "ml"],
    # Parâmetros de retry configurados diretamente na DAG (padrão Airflow 2.6+)
    default_args={
        "owner": "daniel_teofilo",
        "retries": 2,
        "retry_delay": timedelta(minutes=5),
        "email_on_failure": False,
        "email_on_retry": False,
    },
) as dag:

    # ── Task 1: Extração ──────────────────────────────────────────────────────
    task_extrair = PythonOperator(
        task_id="extrair_contratos",
        python_callable=extrair_contratos,
        doc_md="""
        **Extração paginada da API de contratos do Ceará Transparente.**
        Coleta todos os contratos assinados nos últimos 30 dias.
        Dados passados para próxima task via XCom (key: `contratos_raw`).
        """,
    )

    # ── Task 2: Armazenamento ─────────────────────────────────────────────────
    task_salvar = PythonOperator(
        task_id="salvar_postgres",
        python_callable=salvar_postgres,
        doc_md="""
        **Persistência no PostgreSQL.**
        Normaliza e insere os contratos na tabela `contratos`.
        Operação idempotente via ON CONFLICT DO NOTHING.
        """,
    )

    # ── Task 3: Detecção de Anomalias ─────────────────────────────────────────
    task_anomalias = PythonOperator(
        task_id="detectar_anomalias",
        python_callable=detectar_anomalias,
        doc_md="""
        **Detecção de anomalias com Isolation Forest.**
        Analisa os contratos dos últimos 90 dias.
        Features: log(valor_global), log(valor/dia), prazo_vigencia_dias.
        Resultado passado para próxima task via XCom (key: `anomalias`).
        """,
    )

    # ── Task 4: Salvar Anomalias ──────────────────────────────────────────────
    task_salvar_anomalias = PythonOperator(
        task_id="salvar_anomalias",
        python_callable=salvar_anomalias,
        doc_md="""
        **Persistência das anomalias detectadas.**
        Salva na tabela `anomalias_contratos` com score e nível de risco.
        Tabela é recarregada a cada execução (truncate + insert).
        """,
    )

    # ── Dependências (ordem de execução) ──────────────────────────────────────
    # >> é o operador de dependência do Airflow (define ordem left → right)
    task_extrair >> task_salvar >> task_anomalias >> task_salvar_anomalias


# ─────────────────────────────────────────────────────────────────────────────
# CONSULTAS ÚTEIS PARA ANÁLISE DOS RESULTADOS
# (Cole no DBeaver / pgAdmin após executar a DAG)
# ─────────────────────────────────────────────────────────────────────────────
"""
-- 1. Ver todos os contratos com risco ALTO, ordenados por score
SELECT
    numero_contrato,
    fornecedor_nome,
    orgao_nome,
    TO_CHAR(valor_global, 'FM999,999,999.00') AS valor_formatado,
    prazo_vigencia_dias,
    ROUND(score_anomalia::numeric, 4) AS score,
    percentil_risco,
    nivel_risco,
    data_assinatura
FROM anomalias_contratos
WHERE nivel_risco = 'ALTO'
ORDER BY score_anomalia ASC  -- mais negativo = mais anômalo
LIMIT 20;

-- 2. Distribuição de anomalias por órgão
SELECT
    orgao_nome,
    COUNT(*) AS qtd_anomalias,
    SUM(valor_global) AS valor_total_anomalo,
    AVG(percentil_risco) AS percentil_medio
FROM anomalias_contratos
GROUP BY orgao_nome
ORDER BY qtd_anomalias DESC;

-- 3. Comparar valores medianos: contratos normais vs. anômalos
SELECT
    'Normal' AS tipo,
    PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY valor_global) AS mediana_valor,
    AVG(prazo_vigencia_dias) AS prazo_medio_dias
FROM contratos c
WHERE NOT EXISTS (
    SELECT 1 FROM anomalias_contratos a
    WHERE a.numero_contrato = c.numero_contrato
)
UNION ALL
SELECT
    'Anômalo' AS tipo,
    PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY valor_global) AS mediana_valor,
    AVG(prazo_vigencia_dias) AS prazo_medio_dias
FROM anomalias_contratos;
"""
