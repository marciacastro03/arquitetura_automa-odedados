"""
================================================================================
TRABALHO FINAL — Arquitetura e Automação de Pipeline de Dados
UNIFOR — MBA em Ciência de Dados | Prof. MSc. Daniel Teófilo | 2026
================================================================================

Pipeline completo com 5 tasks orquestradas no Apache Airflow:

  Task 1 — extrair_contratos     : Coleta da API Ceará Transparente (paginada)
  Task 2 — salvar_postgres       : Persistência na tabela contratos_api
  Task 3 — classificar_com_llm   : Classifica os 30 maiores contratos via Groq
  Task 4 — salvar_classificados  : Persiste em contratos_classificados
  Task 5 — gerar_relatorio       : Gera relatório HTML automatizado

CONFIGURAÇÃO NECESSÁRIA:
  1. No Airflow UI > Admin > Variables, crie:
       GROQ_API_KEY = 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
       DB_HOST      = 172.17.0.3   (IP do container aula_postgres)
       
       DB_NAME      = aula
       DB_USER      = postgres
       DB_PASSWORD  = postgres

  2. Dependências no container aula_airflow (já instaladas):
       pip install groq psycopg2-binary requests

FONTES:
  API: https://api-dados-abertos.cearatransparente.ce.gov.br/transparencia/contratos/contratos
================================================================================
"""

import json
import logging
import re
import time
from datetime import datetime, timedelta

import psycopg2
import requests
from groq import Groq
from psycopg2.extras import execute_values

from airflow import DAG
from airflow.models import Variable
from airflow.operators.python import PythonOperator

logger = logging.getLogger(__name__)

# ─────────────────────────────────────────────────────────────────────────────
# CONFIGURAÇÕES — lidas das Airflow Variables (seguro para produção)
# ─────────────────────────────────────────────────────────────────────────────

def get_db_config():
    """Lê configurações do banco das Airflow Variables."""
    return {
        "host":     Variable.get("DB_HOST",     default_var="172.17.0.3"),
        "port":     int(Variable.get("DB_PORT", default_var="5432")),
        "database": Variable.get("DB_NAME",     default_var="aula"),
        "user":     Variable.get("DB_USER",     default_var="postgres"),
        "password": Variable.get("DB_PASSWORD", default_var="postgres"),
    }


def get_db_connection():
    return psycopg2.connect(**get_db_config())


# Configurações da API
API_BASE_URL = (
    "https://api-dados-abertos.cearatransparente.ce.gov.br"
    "/transparencia/contratos/contratos"
)
DIAS_RETROATIVOS = 30   # janela de busca em dias
MAX_PAGINAS = 20        # limite de paginação (seguro para não exceder rate limit)

# Configurações do LLM
GROQ_MODEL  = "llama-3.3-70b-versatile"
TOP_N       = 30        # classificar os N maiores contratos por valor

# Categorias definidas no trabalho (alinhadas com Aula 4)
CATEGORIAS = [
    "Saude",
    "Educacao",
    "Infraestrutura e Obras",
    "Tecnologia da Informacao",
    "Alimentacao e Nutricao",
    "Seguranca Publica",
    "Meio Ambiente e Saneamento",
    "Transporte e Logistica",
    "Administrativo e Material de Escritorio",
    "Servicos Gerais",
    "Outros",
]


# ─────────────────────────────────────────────────────────────────────────────
# TASK 1 — EXTRAÇÃO DA API CEARÁ TRANSPARENTE
# ─────────────────────────────────────────────────────────────────────────────

def extrair_contratos(**context):
    """
    Coleta contratos públicos da API do Ceará Transparente.

    - Paginação automática até MAX_PAGINAS
    - Janela de DIAS_RETROATIVOS dias a partir de hoje
    - Dados passados para próxima task via XCom (key: contratos_raw)
    """
    hoje      = datetime.now()
    data_fim  = hoje.strftime("%d/%m/%Y")
    data_ini  = (hoje - timedelta(days=DIAS_RETROATIVOS)).strftime("%d/%m/%Y")

    logger.info(f"Extraindo contratos | {data_ini} -> {data_fim}")

    todos, pagina, total_paginas = [], 1, None

    while True:
        params = {
            "page":                   pagina,
            "data_assinatura_inicio": data_ini,
            "data_assinatura_fim":    data_fim,
        }

        try:
            resp = requests.get(API_BASE_URL, params=params, timeout=30)
            resp.raise_for_status()
        except requests.exceptions.RequestException as e:
            logger.error(f"Erro na pagina {pagina}: {e}")
            break

        dados    = resp.json()
        registros = dados.get("data", [])
        meta     = dados.get("sumary", {})

        if total_paginas is None:
            total_paginas   = meta.get("total_pages", 1)
            total_registros = meta.get("total_records", 0)
            logger.info(f"Total: {total_registros} contratos | {total_paginas} paginas")

        todos.extend(registros)
        logger.info(f"Pagina {pagina}/{total_paginas} — {len(registros)} registros")

        if pagina >= total_paginas or pagina >= MAX_PAGINAS or not registros:
            break

        pagina += 1
        time.sleep(0.5)

    logger.info(f"Extracao concluida: {len(todos)} contratos")

    # Envia para XCom — próxima task vai consumir daqui
    context["ti"].xcom_push(key="contratos_raw", value=todos)
    return len(todos)


# ─────────────────────────────────────────────────────────────────────────────
# TASK 2 — PERSISTÊNCIA NO POSTGRESQL (tabela: contratos_api)
# ─────────────────────────────────────────────────────────────────────────────

def salvar_postgres(**context):
    """
    Persiste os contratos brutos na tabela contratos_api.

    - Cria a tabela se não existir (idempotente)
    - ON CONFLICT DO NOTHING evita duplicatas em re-execuções
    - Retorna lista de objetos para classificação via XCom
    """
    ti = context["ti"]
    contratos_raw = ti.xcom_pull(task_ids="extrair_contratos", key="contratos_raw")

    if not contratos_raw:
        logger.warning("Nenhum contrato recebido.")
        ti.xcom_push(key="contratos_para_classificar", value=[])
        return 0

    # ── Cria tabela se não existir ────────────────────────────────────────────
    ddl = """
        CREATE TABLE IF NOT EXISTS contratos_api (
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
            prazo_vigencia_dias  INTEGER,
            json_original        JSONB,
            inserido_em          TIMESTAMP DEFAULT NOW(),
            UNIQUE(numero_contrato, data_assinatura)
        );
    """

    with get_db_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(ddl)
        conn.commit()

    # ── Normaliza e insere ────────────────────────────────────────────────────
    def parse_data(valor):
        if not valor:
            return None
        s = str(valor)
        if "T" in s:
            s = s.split("T")[0]
        for fmt in ("%Y-%m-%d", "%d/%m/%Y"):
            try:
                d = datetime.strptime(s, fmt).date()
                return d if d.year >= 1900 else None
            except ValueError:
                continue
        return None

    def parse_valor(v):
        if v is None:
            return None
        try:
            return float(str(v).replace("R$", "").replace(".", "").replace(",", ".").strip())
        except (ValueError, AttributeError):
            return None

    registros = []
    for c in contratos_raw:
        data_ass   = parse_data(c.get("data_assinatura"))
        data_ini_v = parse_data(c.get("data_inicio"))
        data_fim_v = parse_data(c.get("data_termino"))
        prazo_dias = (
            (data_fim_v - data_ini_v).days
            if data_ini_v and data_fim_v and data_fim_v > data_ini_v
            else None
        )
        val_ini    = parse_valor(c.get("valor_contrato"))
        val_global = parse_valor(c.get("valor_atualizado_concedente") or c.get("valor_contrato"))

        registros.append((
            c.get("num_contrato"),
            c.get("descricao_objeto"),
            c.get("descricao_nome_credor"),
            c.get("plain_cpf_cnpj_financiador") or c.get("cpf_cnpj_financiador"),
            c.get("cod_orgao"),
            c.get("descricao_modalidade"),
            val_ini,
            val_global,
            data_ass,
            data_ini_v,
            data_fim_v,
            prazo_dias,
            json.dumps(c, ensure_ascii=False),
        ))

    sql_insert = """
        INSERT INTO contratos_api (
            numero_contrato, objeto, fornecedor_nome, fornecedor_cnpj,
            orgao_nome, modalidade, valor_inicial, valor_global,
            data_assinatura, data_inicio_vigencia, data_fim_vigencia,
            prazo_vigencia_dias, json_original
        ) VALUES %s
        ON CONFLICT (numero_contrato, data_assinatura) DO NOTHING
    """

    with get_db_connection() as conn:
        with conn.cursor() as cur:
            execute_values(cur, sql_insert, registros, page_size=500)
            inseridos = cur.rowcount
        conn.commit()

    logger.info(f"{len(registros)} processados | {inseridos} novos inseridos em contratos_api")

    # ── Prepara lista para classificação ──────────────────────────────────────
    contratos_para_classificar = [
        {
            "numero_contrato": c.get("num_contrato"),
            "objeto":          c.get("descricao_objeto", ""),
            "orgao_nome":      c.get("cod_orgao", ""),
            "fornecedor_nome": c.get("descricao_nome_credor", ""),
            "valor_global":    parse_valor(
                c.get("valor_atualizado_concedente") or c.get("valor_contrato")
            ),
            "data_assinatura": str(parse_data(c.get("data_assinatura"))),
        }
        for c in contratos_raw
        if c.get("descricao_objeto")
    ]

    ti.xcom_push(key="contratos_para_classificar", value=contratos_para_classificar)
    return inseridos


# ─────────────────────────────────────────────────────────────────────────────
# TASK 3 — CLASSIFICAÇÃO COM GROQ LLM (top 30 por valor)
# ─────────────────────────────────────────────────────────────────────────────

def classificar_com_llm(**context):
    """
    Classifica semanticamente os 30 maiores contratos por valor usando Groq.

    - Seleciona TOP_N contratos com maior valor_global
    - Chama Groq com prompt estruturado (zero-shot)
    - Parsing defensivo do JSON retornado (regex fallback)
    - Resultado enviado para XCom (key: classificacoes)
    """
    ti = context["ti"]
    contratos = ti.xcom_pull(task_ids="salvar_postgres", key="contratos_para_classificar")

    if not contratos:
        logger.warning("Nenhum contrato para classificar.")
        ti.xcom_push(key="classificacoes", value=[])
        return 0

    # Seleciona os TOP_N por valor
    top_contratos = sorted(
        [c for c in contratos if c.get("valor_global")],
        key=lambda x: float(x["valor_global"] or 0),
        reverse=True,
    )[:TOP_N]

    logger.info(f"Classificando {len(top_contratos)} contratos (top {TOP_N} por valor)")

    groq_api_key = Variable.get("GROQ_API_KEY", default_var="")
    client = Groq(api_key=groq_api_key)

    cats_fmt = "\n".join(f"  - {c}" for c in CATEGORIAS)

    sistema = f"""Voce e um especialista em transparencia publica e licitacoes governamentais brasileiras.

Analise o objeto de um contrato publico e retorne APENAS um JSON com:
{{
  "categoria":          "uma das categorias listadas",
  "confianca":          "ALTA | MEDIA | BAIXA",
  "objeto_vago":        true | false,
  "justificativa_vago": "motivo se vago, vazio se nao",
  "resumo":             "1 frase curta descrevendo o contrato"
}}

CATEGORIAS:
{cats_fmt}

REGRAS:
- Responda SOMENTE com o JSON, sem texto adicional
- objeto_vago=true se o objeto for generico, vago ou menos de 5 palavras informativas
- confianca ALTA so quando o objeto e claro e a categoria e obvia
- resumo com no maximo 15 palavras"""

    classificacoes, tokens_total, erros = [], 0, 0

    for i, contrato in enumerate(top_contratos):
        objeto = (contrato.get("objeto") or "").strip()
        if not objeto or len(objeto) < 5:
            continue

        try:
            resp = client.chat.completions.create(
                model=GROQ_MODEL,
                messages=[
                    {"role": "system", "content": sistema},
                    {"role": "user",   "content": f'Analise este contrato:\n\n"{objeto}"'},
                ],
                temperature=0,
                max_tokens=300,
                response_format={"type": "json_object"},
            )

            conteudo = resp.choices[0].message.content
            tokens   = resp.usage.total_tokens
            tokens_total += tokens

            # Parsing defensivo — extrai JSON mesmo com texto ao redor
            match = re.search(r"\{.*\}", conteudo.strip(), re.DOTALL)
            if not match:
                raise ValueError(f"Sem JSON na resposta: {conteudo[:100]}")

            dados = json.loads(match.group())

            # Valida e normaliza campos
            categoria = dados.get("categoria", "Outros")
            if categoria not in CATEGORIAS:
                categoria = "Outros"

            confianca = dados.get("confianca", "BAIXA").upper()
            if confianca not in ("ALTA", "MEDIA", "BAIXA"):
                confianca = "BAIXA"

            classificacoes.append({
                **contrato,
                "categoria":          categoria,
                "confianca":          confianca,
                "objeto_vago":        bool(dados.get("objeto_vago", False)),
                "justificativa_vago": dados.get("justificativa_vago", ""),
                "resumo":             dados.get("resumo", ""),
                "tokens_usados":      tokens,
                "modelo_usado":       GROQ_MODEL,
                "provider_llm":       "groq",
            })

            logger.info(f"[{i+1}/{len(top_contratos)}] {categoria} ({confianca}) | {objeto[:60]}")

        except Exception as e:
            erros += 1
            logger.error(f"[{i+1}] Erro ao classificar '{objeto[:60]}': {e}")
            classificacoes.append({
                **contrato,
                "categoria":          "Outros",
                "confianca":          "BAIXA",
                "objeto_vago":        False,
                "justificativa_vago": "",
                "resumo":             f"Erro na classificacao: {str(e)[:80]}",
                "tokens_usados":      0,
                "modelo_usado":       GROQ_MODEL,
                "provider_llm":       "groq",
            })

        # Pausa a cada 10 para respeitar rate limit
        if (i + 1) % 10 == 0:
            time.sleep(2)

    vagos = sum(1 for c in classificacoes if c.get("objeto_vago"))
    logger.info(
        f"Classificacao concluida: {len(classificacoes)} contratos | "
        f"Vagos: {vagos} | Erros: {erros} | Tokens: {tokens_total}"
    )

    ti.xcom_push(key="classificacoes", value=classificacoes)
    return len(classificacoes)


# ─────────────────────────────────────────────────────────────────────────────
# TASK 4 — SALVAR CLASSIFICAÇÕES (tabela: contratos_classificados)
# ─────────────────────────────────────────────────────────────────────────────

def salvar_classificados(**context):
    """
    Persiste os contratos classificados na tabela contratos_classificados.

    - TRUNCATE + INSERT: tabela sempre reflete o estado mais recente
    - Inclui metadados do LLM (tokens, modelo, provider)
    """
    ti = context["ti"]
    classificacoes = ti.xcom_pull(task_ids="classificar_com_llm", key="classificacoes")

    if not classificacoes:
        logger.info("Nenhuma classificacao para salvar.")
        return 0

    # ── Cria tabela se não existir ────────────────────────────────────────────
    ddl = """
        CREATE TABLE IF NOT EXISTS contratos_classificados (
            id                   BIGSERIAL PRIMARY KEY,
            numero_contrato      TEXT,
            objeto               TEXT,
            orgao_nome           TEXT,
            fornecedor_nome      TEXT,
            valor_global         NUMERIC(18, 2),
            data_assinatura      DATE,
            categoria            TEXT,
            confianca            TEXT,
            objeto_vago          BOOLEAN,
            justificativa_vago   TEXT,
            resumo               TEXT,
            tokens_usados        INTEGER,
            modelo_usado         TEXT,
            provider_llm         TEXT,
            classificado_em      TIMESTAMP DEFAULT NOW()
        );
    """

    sql_truncate = "TRUNCATE TABLE contratos_classificados RESTART IDENTITY;"

    sql_insert = """
        INSERT INTO contratos_classificados (
            numero_contrato, objeto, orgao_nome, fornecedor_nome,
            valor_global, data_assinatura, categoria, confianca,
            objeto_vago, justificativa_vago, resumo,
            tokens_usados, modelo_usado, provider_llm
        ) VALUES %s
    """

    registros = []
    for c in classificacoes:
        # Converte data_assinatura de string para objeto date
        data_ass = None
        if c.get("data_assinatura") and c["data_assinatura"] != "None":
            try:
                data_ass = datetime.strptime(c["data_assinatura"], "%Y-%m-%d").date()
            except ValueError:
                pass

        registros.append((
            c.get("numero_contrato"),
            c.get("objeto"),
            c.get("orgao_nome"),
            c.get("fornecedor_nome"),
            c.get("valor_global"),
            data_ass,
            c.get("categoria"),
            c.get("confianca"),
            c.get("objeto_vago", False),
            c.get("justificativa_vago", ""),
            c.get("resumo", ""),
            c.get("tokens_usados", 0),
            c.get("modelo_usado"),
            c.get("provider_llm"),
        ))

    with get_db_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(ddl)
            cur.execute(sql_truncate)
            execute_values(cur, sql_insert, registros)
        conn.commit()

    vagos = sum(1 for c in classificacoes if c.get("objeto_vago"))
    logger.info(
        f"{len(registros)} classificacoes salvas em contratos_classificados | "
        f"Vagos: {vagos}"
    )
    return len(registros)


# ─────────────────────────────────────────────────────────────────────────────
# TASK 5 — RELATÓRIO HTML AUTOMÁTICO
# ─────────────────────────────────────────────────────────────────────────────

def gerar_relatorio(**context):
    """
    Gera relatório HTML com os resultados do pipeline.

    - Cards com métricas principais
    - Distribuição por categoria (barras)
    - Tabela dos 30 contratos classificados com valor
    - Tabela de objetos vagos detectados
    """
    ti = context["ti"]
    classificacoes = ti.xcom_pull(task_ids="classificar_com_llm", key="classificacoes")
    total_extraidos = ti.xcom_pull(task_ids="extrair_contratos")  # return_value = count

    if not classificacoes:
        logger.info("Sem dados para relatorio.")
        return

    total   = len(classificacoes)
    vagos   = [c for c in classificacoes if c.get("objeto_vago")]
    tokens  = sum(c.get("tokens_usados", 0) for c in classificacoes)
    modelo  = classificacoes[0].get("modelo_usado", GROQ_MODEL)
    data_ex = datetime.now().strftime("%d/%m/%Y %H:%M")

    # Valor total classificado
    valor_total = sum(float(c.get("valor_global") or 0) for c in classificacoes)
    valor_fmt   = f"R$ {valor_total:,.2f}".replace(",", "X").replace(".", ",").replace("X", ".")

    # Distribuição por categoria
    por_cat = {}
    for c in classificacoes:
        cat = c.get("categoria", "Outros")
        por_cat[cat] = por_cat.get(cat, 0) + 1
    cats_ord = sorted(por_cat.items(), key=lambda x: x[1], reverse=True)

    # Distribuição por confiança
    por_conf = {}
    for c in classificacoes:
        conf = c.get("confianca", "BAIXA")
        por_conf[conf] = por_conf.get(conf, 0) + 1

    # Barras de categoria
    max_cat = cats_ord[0][1] if cats_ord else 1
    cores   = [
        "#1565C0", "#00838F", "#2E7D32", "#6A1B9A", "#E65100",
        "#AD1457", "#00695C", "#4527A0", "#BF360C", "#1B5E20", "#37474F",
    ]
    barras = ""
    for idx, (cat, qtd) in enumerate(cats_ord):
        pct    = round(qtd / total * 100, 1)
        larg   = round(qtd / max_cat * 100)
        cor    = cores[idx % len(cores)]
        barras += f"""
        <div class="bar-row">
          <span class="bar-label">{cat}</span>
          <div class="bar-wrap"><div class="bar-fill" style="width:{larg}%;background:{cor}"></div></div>
          <span class="bar-count">{qtd} ({pct}%)</span>
        </div>"""

    # Tabela dos 30 contratos
    linhas_contratos = ""
    for c in sorted(classificacoes, key=lambda x: float(x.get("valor_global") or 0), reverse=True):
        val     = c.get("valor_global")
        val_fmt = (
            f"R$ {float(val):,.2f}".replace(",", "X").replace(".", ",").replace("X", ".")
            if val else "-"
        )
        vago_badge = '<span style="color:#C62828;font-weight:600"> ⚠ VAGO</span>' if c.get("objeto_vago") else ""
        conf_cor   = {"ALTA": "#2E7D32", "MEDIA": "#F57F17", "BAIXA": "#C62828"}.get(
            c.get("confianca", "BAIXA"), "#C62828"
        )
        linhas_contratos += f"""
        <tr>
          <td>{(c.get('numero_contrato') or '-')[:25]}</td>
          <td>{(c.get('objeto') or '')[:70]}{"..." if len(c.get('objeto') or '') > 70 else ""}{vago_badge}</td>
          <td>{(c.get('orgao_nome') or '')[:35]}</td>
          <td>{c.get('categoria', '')}</td>
          <td style="color:{conf_cor};font-weight:600">{c.get('confianca', '')}</td>
          <td style="text-align:right;font-weight:600;color:#0D2B55">{val_fmt}</td>
        </tr>"""

    # Tabela de vagos
    linhas_vagos = ""
    for v in sorted(vagos, key=lambda x: float(x.get("valor_global") or 0), reverse=True):
        val     = v.get("valor_global")
        val_fmt = (
            f"R$ {float(val):,.2f}".replace(",", "X").replace(".", ",").replace("X", ".")
            if val else "-"
        )
        linhas_vagos += f"""
        <tr>
          <td>{(v.get('orgao_nome') or '')[:40]}</td>
          <td>{(v.get('objeto') or '')[:80]}</td>
          <td style="text-align:right;color:#C62828;font-weight:600">{val_fmt}</td>
          <td>{(v.get('justificativa_vago') or '')[:80]}</td>
        </tr>"""

    if not linhas_vagos:
        linhas_vagos = "<tr><td colspan='4' style='text-align:center;color:#888'>Nenhum objeto vago detectado</td></tr>"

    alta  = por_conf.get("ALTA",  0)
    media = por_conf.get("MEDIA", 0)
    baixa = por_conf.get("BAIXA", 0)

    html = f"""<!DOCTYPE html>
<html lang="pt-BR">
<head>
<meta charset="UTF-8">
<title>Pipeline de Contratos — Relatorio Final</title>
<style>
  * {{ box-sizing:border-box; }}
  body {{ font-family:Arial,sans-serif; margin:0; padding:20px; background:#f0f2f5; color:#333; }}
  .header {{ background:linear-gradient(135deg,#0D2B55,#1565C0); color:white;
             padding:28px 32px; border-radius:10px; margin-bottom:24px; }}
  .header h1 {{ margin:0; font-size:20px; }}
  .header p  {{ margin:6px 0 0; opacity:.8; font-size:12px; }}
  .cards {{ display:flex; gap:14px; margin-bottom:24px; flex-wrap:wrap; }}
  .card {{ background:white; border-radius:10px; padding:18px 22px; flex:1; min-width:140px;
           box-shadow:0 2px 8px rgba(0,0,0,.08); border-top:4px solid #1565C0; }}
  .card.green {{ border-color:#2E7D32; }}
  .card.red   {{ border-color:#C62828; }}
  .card.teal  {{ border-color:#00838F; }}
  .card.gray  {{ border-color:#455A64; }}
  .card .num  {{ font-size:26px; font-weight:700; color:#0D2B55; }}
  .card.green .num {{ color:#2E7D32; }}
  .card.red   .num {{ color:#C62828; }}
  .card.teal  .num {{ color:#00838F; }}
  .card .lbl  {{ font-size:11px; color:#777; margin-top:4px; text-transform:uppercase; letter-spacing:.5px; }}
  .section {{ background:white; border-radius:10px; padding:24px; margin-bottom:20px;
              box-shadow:0 2px 8px rgba(0,0,0,.08); }}
  .section h2 {{ margin-top:0; font-size:14px; color:#0D2B55;
                 border-bottom:2px solid #E3F2FD; padding-bottom:10px; margin-bottom:16px; }}
  .bar-row {{ display:flex; align-items:center; gap:10px; margin-bottom:8px; }}
  .bar-label {{ width:230px; font-size:12px; text-align:right; color:#555; flex-shrink:0; }}
  .bar-wrap  {{ flex:1; background:#ECEFF1; border-radius:4px; height:15px; }}
  .bar-fill  {{ border-radius:4px; height:100%; }}
  .bar-count {{ width:90px; font-size:11px; color:#888; flex-shrink:0; }}
  .conf {{ display:flex; gap:12px; margin-bottom:14px; }}
  .badge {{ padding:5px 14px; border-radius:20px; font-size:12px; font-weight:600; }}
  .alta  {{ background:#E8F5E9; color:#2E7D32; }}
  .media {{ background:#FFF8E1; color:#F57F17; }}
  .baixa {{ background:#FFEBEE; color:#C62828; }}
  table {{ width:100%; border-collapse:collapse; font-size:11px; }}
  th {{ background:#0D2B55; color:white; padding:8px 10px; text-align:left; font-weight:500; }}
  td {{ padding:7px 10px; border-bottom:1px solid #F0F0F0; vertical-align:top; }}
  tr:hover td {{ background:#F5F9FF; }}
  .footer {{ text-align:center; font-size:11px; color:#AAA; margin-top:20px; padding:10px; }}
  .stack {{ background:#E3F2FD; border-radius:8px; padding:12px 18px; font-size:12px;
            color:#1565C0; margin-bottom:20px; }}
</style>
</head>
<body>

<div class="header">
  <h1>Pipeline de Dados — Contratos Publicos do Ceara</h1>
  <p>
    Trabalho Final | Arquitetura e Automacao de Pipeline de Dados | UNIFOR 2026 |
    Prof. MSc. Daniel Teofilo | Gerado em {data_ex}
  </p>
</div>

<div class="stack">
  <strong>Stack:</strong> Apache Airflow &nbsp;·&nbsp; Python 3 &nbsp;·&nbsp;
  PostgreSQL &nbsp;·&nbsp; API Ceara Transparente &nbsp;·&nbsp;
  Groq LLM ({modelo}) &nbsp;·&nbsp; 5 Tasks (DAG)
</div>

<div class="cards">
  <div class="card"><div class="num">{total_extraidos or "N/A"}</div><div class="lbl">Contratos extraidos</div></div>
  <div class="card teal"><div class="num">{total}</div><div class="lbl">Contratos classificados (top {TOP_N})</div></div>
  <div class="card red"><div class="num">{len(vagos)}</div><div class="lbl">Objetos vagos detectados</div></div>
  <div class="card green"><div class="num">{valor_fmt}</div><div class="lbl">Valor total classificado</div></div>
  <div class="card gray"><div class="num">{tokens:,}</div><div class="lbl">Tokens LLM consumidos</div></div>
</div>

<div class="section">
  <h2>Confianca das Classificacoes</h2>
  <div class="conf">
    <span class="badge alta">ALTA: {alta} ({round(alta/total*100,1) if total else 0}%)</span>
    <span class="badge media">MEDIA: {media} ({round(media/total*100,1) if total else 0}%)</span>
    <span class="badge baixa">BAIXA: {baixa} ({round(baixa/total*100,1) if total else 0}%)</span>
  </div>
</div>

<div class="section">
  <h2>Distribuicao por Categoria Tematica</h2>
  {barras}
</div>

<div class="section">
  <h2>Top {TOP_N} Contratos Classificados por Valor</h2>
  <table>
    <thead>
      <tr>
        <th>N. Contrato</th><th>Objeto</th><th>Orgao</th>
        <th>Categoria</th><th>Confianca</th><th>Valor</th>
      </tr>
    </thead>
    <tbody>{linhas_contratos}</tbody>
  </table>
</div>

<div class="section">
  <h2>Objetos com Descricao Vaga ({len(vagos)} contratos)</h2>
  <table>
    <thead>
      <tr><th>Orgao</th><th>Objeto Original</th><th>Valor</th><th>Motivo</th></tr>
    </thead>
    <tbody>{linhas_vagos}</tbody>
  </table>
</div>

<div class="footer">
  Pipeline dag_trabalho_final_marcinha | Apache Airflow | Tabelas: contratos_api, contratos_classificados | {data_ex}
</div>

</body>
</html>"""

    # Salva o relatório no container
    caminho = "/root/relatorio_trabalho_final_marcinha.html"
    with open(caminho, "w", encoding="utf-8") as f:
        f.write(html)

    logger.info(f"Relatorio salvo em {caminho} ({len(html)} bytes)")
    return caminho


# ─────────────────────────────────────────────────────────────────────────────
# DEFINIÇÃO DA DAG
# ─────────────────────────────────────────────────────────────────────────────

with DAG(
    dag_id="dag_trabalho_final_marcinha",
    description=(
        "Trabalho Final UNIFOR 2026 — Pipeline: API Ceara Transparente → "
        "PostgreSQL → Groq LLM → Relatorio HTML"
    ),
    schedule="0 6 * * *",           # todo dia as 06h UTC
    start_date=datetime(2025, 1, 1),
    catchup=False,
    tags=["trabalho-final", "unifor", "contratos", "llm", "groq"],
    default_args={
        "owner":          "marcia_castro",
        "retries":        2,
        "retry_delay":    timedelta(minutes=5),
        "email_on_failure": False,
        "email_on_retry":   False,
    },
) as dag:

    # Task 1 — Extração
    task_extrair = PythonOperator(
        task_id="extrair_contratos",
        python_callable=extrair_contratos,
        doc_md="""
        **Task 1 — Extração**
        Coleta contratos da API do Ceara Transparente nos ultimos 30 dias.
        Dados passados para proxima task via XCom (key: `contratos_raw`).
        """,
    )

    # Task 2 — Persistência bruta
    task_salvar = PythonOperator(
        task_id="salvar_postgres",
        python_callable=salvar_postgres,
        doc_md="""
        **Task 2 — Persistencia**
        Normaliza e insere contratos na tabela `contratos_api`.
        Operacao idempotente (ON CONFLICT DO NOTHING).
        """,
    )

    # Task 3 — Classificação LLM
    task_classificar = PythonOperator(
        task_id="classificar_com_llm",
        python_callable=classificar_com_llm,
        execution_timeout=timedelta(hours=2),  # LLM pode demorar
        doc_md="""
        **Task 3 — Classificacao com LLM**
        Classifica os 30 maiores contratos por valor usando Groq (llama-3.3-70b).
        Zero-shot com prompt estruturado. Parsing defensivo via regex.
        """,
    )

    # Task 4 — Salvar classificações
    task_salvar_class = PythonOperator(
        task_id="salvar_classificados",
        python_callable=salvar_classificados,
        doc_md="""
        **Task 4 — Salvar Classificacoes**
        Persiste resultados do LLM na tabela `contratos_classificados`.
        Truncate + insert garante estado sempre atualizado.
        """,
    )

    # Task 5 — Relatório HTML
    task_relatorio = PythonOperator(
        task_id="gerar_relatorio",
        python_callable=gerar_relatorio,
        doc_md="""
        **Task 5 — Relatorio HTML**
        Gera relatorio automatizado com metricas, distribuicao por categoria
        e lista de objetos vagos detectados pelo LLM.
        """,
    )

    # ── Ordem de execução ─────────────────────────────────────────────────────
    # >> é o operador de dependência do Airflow: esquerda executa antes da direita
    task_extrair >> task_salvar >> task_classificar >> task_salvar_class >> task_relatorio


# ─────────────────────────────────────────────────────────────────────────────
# CONSULTAS SQL PARA ANÁLISE DOS RESULTADOS
# (Cole no DBeaver após executar a DAG)
# ─────────────────────────────────────────────────────────────────────────────
"""
-- 1. Distribuicao por categoria (com percentual)
SELECT
    categoria,
    COUNT(*) AS qtd,
    ROUND(COUNT(*) * 100.0 / SUM(COUNT(*)) OVER(), 1) AS pct
FROM contratos_classificados
GROUP BY categoria
ORDER BY qtd DESC;

-- 2. Orgaos com mais objetos vagos
SELECT
    orgao_nome,
    COUNT(*) AS total,
    SUM(CASE WHEN objeto_vago THEN 1 ELSE 0 END) AS vagos
FROM contratos_classificados
GROUP BY orgao_nome
HAVING COUNT(*) >= 2
ORDER BY vagos DESC;

-- 3. Top 10 contratos por valor com classificacao
SELECT
    numero_contrato,
    orgao_nome,
    LEFT(objeto, 80) AS objeto,
    categoria,
    confianca,
    TO_CHAR(valor_global, 'FM999G999G999D00') AS valor,
    objeto_vago
FROM contratos_classificados
ORDER BY valor_global DESC
LIMIT 10;

-- 4. Custo total de tokens LLM
SELECT
    modelo_usado,
    provider_llm,
    COUNT(*) AS contratos_classificados,
    SUM(tokens_usados) AS total_tokens
FROM contratos_classificados
GROUP BY modelo_usado, provider_llm;

-- 5. Total extraido vs classificado
SELECT
    (SELECT COUNT(*) FROM contratos_api)            AS total_extraidos,
    (SELECT COUNT(*) FROM contratos_classificados)  AS total_classificados,
    (SELECT COUNT(*) FROM contratos_classificados
     WHERE objeto_vago = true)                      AS total_vagos;
"""
