"""
Pipeline local de classificacao de licitacoes via Groq.
Execucao: python classificacao_licitacoes_groq_v2.py

Configuracao via variaveis de ambiente:
  GROQ_API_KEY=gsk_...
  DB_HOST=172.17.0.3
  DB_NAME=aula
  DB_PASSWORD=postgres

Melhorias v2:
  - Sem filtro de modalidade (busca todas)
  - Janela de 90 dias (mais recente e mais denso)
  - Ate 200 paginas x 50 registros = ate 10.000 licitacoes
  - Relatorio HTML melhorado com tabela de top licitacoes por valor
"""

import json
import logging
import os
import re
import time
from datetime import datetime, timedelta
from pathlib import Path

import psycopg2
import requests
from groq import Groq
from psycopg2.extras import execute_values
from requests.exceptions import JSONDecodeError as RequestsJSONDecodeError

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger(__name__)

# ─────────────────────────────────────────────────────────────────────────────
# CONFIGURACOES
# ─────────────────────────────────────────────────────────────────────────────

DB_CONFIG = {
    "host": os.getenv("DB_HOST", "172.17.0.3"),       # IP fixo do container postgres
    "port": int(os.getenv("DB_PORT", 5432)),
    "database": os.getenv("DB_NAME", "aula"),
    "user": os.getenv("DB_USER", "postgres"),
    "password": os.getenv("DB_PASSWORD", "postgres"),  # senha correta
}

PNCP_BASE_URL = "https://pncp.gov.br/api/consulta/v1/contratacoes/publicacao"
UF_FILTRO = "CE"

# v2: sem filtro de modalidade = todas as modalidades
# Modalidades comuns: 1=Pregao Eletronico, 2=Pregao Presencial, 6=Dispensa,
#                     7=Inexigibilidade, 8=Concorrencia, etc.
MODALIDADES = [1, 2, 6, 7, 8]  # busca cada uma separadamente

TAMANHO_PAG = 50
DIAS_JANELA = 90          # v2: janela menor = dados mais recentes e mais densos
MAX_PAGINAS = 200         # v2: de 20 para 200 paginas por modalidade
PNCP_TIMEOUT = 90
PNCP_MAX_TENTATIVAS = 3

GROQ_API_KEY = os.getenv("GROQ_API_KEY", "")
GROQ_MODEL = "llama-3.3-70b-versatile"
BATCH_SIZE = 10
MAX_LICITACOES_LLM = 500  # limite para nao explodir tokens/custo

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
# HELPERS
# ─────────────────────────────────────────────────────────────────────────────

def get_db_connection():
    return psycopg2.connect(**DB_CONFIG)


def criar_tabelas():
    ddl_licitacoes = """
        CREATE TABLE IF NOT EXISTS licitacoes_pncp (
            id                      BIGSERIAL PRIMARY KEY,
            numero_controle_pncp    TEXT UNIQUE,
            objeto_compra           TEXT,
            modalidade_nome         TEXT,
            orgao_nome              TEXT,
            orgao_cnpj              TEXT,
            uf                      TEXT,
            valor_total_estimado    NUMERIC(18,2),
            data_publicacao         DATE,
            data_abertura_proposta  TIMESTAMP,
            situacao                TEXT,
            link_sistema_origem     TEXT,
            json_original           JSONB,
            inserido_em             TIMESTAMP DEFAULT NOW()
        );
    """
    ddl_classificacoes = """
        CREATE TABLE IF NOT EXISTS licitacoes_classificadas (
            id                   BIGSERIAL PRIMARY KEY,
            numero_controle_pncp TEXT,
            objeto_compra        TEXT,
            orgao_nome           TEXT,
            uf                   TEXT,
            modalidade_nome      TEXT,
            categoria            TEXT,
            confianca            TEXT,
            objeto_vago          BOOLEAN,
            justificativa_vago   TEXT,
            resumo               TEXT,
            valor_total_estimado NUMERIC(18,2),
            tokens_usados        INTEGER,
            modelo_usado         TEXT,
            provider_llm         TEXT,
            data_publicacao      DATE,
            classificado_em      TIMESTAMP DEFAULT NOW()
        );
    """
    with get_db_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(ddl_licitacoes)
            cur.execute(ddl_classificacoes)
        conn.commit()
    logger.info("Tabelas verificadas/criadas.")


# ─────────────────────────────────────────────────────────────────────────────
# TASK 1 - EXTRACAO (todas as modalidades)
# ─────────────────────────────────────────────────────────────────────────────

def extrair_por_modalidade(modalidade, data_ini, data_fim):
    """Extrai licitacoes de uma modalidade especifica com paginacao completa."""
    todas, pagina = [], 1

    while True:
        params = {
            "dataInicial": data_ini,
            "dataFinal": data_fim,
            "codigoModalidadeContratacao": modalidade,
            "uf": UF_FILTRO,
            "pagina": pagina,
            "tamanhoPagina": TAMANHO_PAG,
        }

        resp = None
        for tentativa in range(1, PNCP_MAX_TENTATIVAS + 1):
            try:
                resp = requests.get(PNCP_BASE_URL, params=params, timeout=PNCP_TIMEOUT)
                resp.raise_for_status()
                break
            except requests.exceptions.RequestException as e:
                logger.warning(f"Modalidade {modalidade} | Tentativa {tentativa} | Pagina {pagina}: {e}")
                if tentativa == PNCP_MAX_TENTATIVAS:
                    return todas
                time.sleep(2)

        if resp.status_code == 204:
            break

        try:
            payload = resp.json()
        except RequestsJSONDecodeError:
            logger.error(f"Modalidade {modalidade} | Resposta invalida na pagina {pagina}")
            break

        registros = payload.get("data", [])
        total = payload.get("totalRegistros", 0)

        if pagina == 1:
            logger.info(f"  Modalidade {modalidade}: {total} licitacoes encontradas")

        if not registros:
            break

        todas.extend(registros)

        total_paginas = -(-total // TAMANHO_PAG)
        if pagina >= total_paginas or pagina >= MAX_PAGINAS:
            break

        pagina += 1
        time.sleep(0.3)

    return todas


def extrair_licitacoes():
    """Extrai licitacoes de todas as modalidades configuradas."""
    hoje = datetime.now()
    data_fim = hoje.strftime("%Y%m%d")
    data_ini = (hoje - timedelta(days=DIAS_JANELA)).strftime("%Y%m%d")

    logger.info(f"Extracao v2 | UF={UF_FILTRO} | {data_ini} -> {data_fim} | Modalidades: {MODALIDADES}")

    todas = []
    vistos = set()  # evita duplicatas entre modalidades

    for modalidade in MODALIDADES:
        logger.info(f"Buscando modalidade {modalidade}...")
        registros = extrair_por_modalidade(modalidade, data_ini, data_fim)

        novos = 0
        for r in registros:
            chave = r.get("numeroControlePNCP")
            if chave and chave not in vistos:
                vistos.add(chave)
                todas.append(r)
                novos += 1

        logger.info(f"  Modalidade {modalidade}: {novos} novos registros (acumulado: {len(todas)})")
        time.sleep(1)  # pausa entre modalidades

    logger.info(f"Extracao concluida: {len(todas)} licitacoes unicas")
    return todas


# ─────────────────────────────────────────────────────────────────────────────
# TASK 2 - ARMAZENAMENTO
# ─────────────────────────────────────────────────────────────────────────────

def salvar_postgres(licitacoes):
    if not licitacoes:
        logger.warning("Nenhuma licitacao para salvar.")
        return [], 0

    criar_tabelas()

    def parse_data(val):
        if not val:
            return None
        for fmt in ("%Y-%m-%dT%H:%M:%S", "%Y-%m-%d"):
            try:
                return datetime.strptime(val[:19], fmt)
            except ValueError:
                continue
        return None

    registros = []
    for l in licitacoes:
        orgao = l.get("orgaoEntidade", {}) or {}
        unidade = l.get("unidadeOrgao", {}) or {}
        registros.append((
            l.get("numeroControlePNCP"),
            l.get("objetoCompra"),
            l.get("modalidadeNome"),
            orgao.get("razaoSocial"),
            orgao.get("cnpj"),
            unidade.get("ufSigla", UF_FILTRO),
            l.get("valorTotalEstimado"),
            parse_data(l.get("dataPublicacaoPncp")),
            parse_data(l.get("dataAberturaProposta")),
            l.get("situacaoCompraNome"),
            l.get("linkSistemaOrigem"),
            json.dumps(l, ensure_ascii=False),
        ))

    sql = """
        INSERT INTO licitacoes_pncp (
            numero_controle_pncp, objeto_compra, modalidade_nome,
            orgao_nome, orgao_cnpj, uf, valor_total_estimado,
            data_publicacao, data_abertura_proposta, situacao,
            link_sistema_origem, json_original
        ) VALUES %s
        ON CONFLICT (numero_controle_pncp) DO NOTHING
    """

    with get_db_connection() as conn:
        with conn.cursor() as cur:
            execute_values(cur, sql, registros, page_size=500)
            inseridos = cur.rowcount
        conn.commit()

    logger.info(f"{len(registros)} processadas | {inseridos} novas inseridas.")

    # Prepara objetos para classificacao com valor incluido
    objetos = [
        {
            "numero_controle_pncp": l.get("numeroControlePNCP"),
            "objeto_compra": l.get("objetoCompra", ""),
            "orgao_nome": (l.get("orgaoEntidade") or {}).get("razaoSocial", ""),
            "uf": (l.get("unidadeOrgao") or {}).get("ufSigla", UF_FILTRO),
            "modalidade_nome": l.get("modalidadeNome", ""),
            "valor_total_estimado": l.get("valorTotalEstimado"),
            "data_publicacao": l.get("dataPublicacaoPncp", "")[:10] if l.get("dataPublicacaoPncp") else None,
        }
        for l in licitacoes if l.get("objetoCompra")
    ]
    return objetos, inseridos


# ─────────────────────────────────────────────────────────────────────────────
# TASK 3 - CLASSIFICACAO COM GROQ
# ─────────────────────────────────────────────────────────────────────────────

def _chamar_groq(prompt_sistema, prompt_usuario):
    client = Groq(api_key=GROQ_API_KEY)
    response = client.chat.completions.create(
        model=GROQ_MODEL,
        messages=[
            {"role": "system", "content": prompt_sistema},
            {"role": "user", "content": prompt_usuario},
        ],
        temperature=0,
        max_tokens=300,
        response_format={"type": "json_object"},
    )
    return {
        "conteudo": response.choices[0].message.content,
        "tokens": response.usage.total_tokens,
        "modelo": GROQ_MODEL,
    }


def _montar_prompt(objeto, categorias):
    cats_formatadas = "\n".join(f"  - {c}" for c in categorias)
    prompt_sistema = f"""Voce e um especialista em transparencia publica e licitacoes governamentais brasileiras.

Sua tarefa e analisar o objeto de uma licitacao publica e retornar um JSON com:

{{
  "categoria":          "uma das categorias listadas abaixo",
  "confianca":          "ALTA | MEDIA | BAIXA",
  "objeto_vago":        true | false,
  "justificativa_vago": "por que e vago (se objeto_vago=true) ou string vazia",
  "resumo":             "1 frase resumindo o objeto em linguagem simples"
}}

CATEGORIAS DISPONIVEIS:
{cats_formatadas}

CRITERIOS PARA objeto_vago=true:
  - Objeto generico demais (ex: "aquisicao de materiais diversos")
  - Ausencia de especificacao do que sera adquirido/contratado
  - Termos vagos como "conforme termo de referencia" sem mais detalhes
  - Descricao com menos de 5 palavras informativas

REGRAS:
  - Responda APENAS com o JSON, sem texto adicional
  - Se nao souber a categoria, use "Outros"
  - Confianca BAIXA quando o objeto for muito vago para classificar
  - Resumo deve ter no maximo 15 palavras
  - Seja rigoroso: prefira MEDIA a ALTA se houver qualquer duvida"""

    prompt_usuario = f'Analise este objeto de licitacao:\n\n"{objeto}"'
    return prompt_sistema, prompt_usuario


def classificar_com_llm(objetos):
    if not objetos:
        logger.warning("Nenhum objeto para classificar.")
        return []

    # Limita para nao explodir tokens
    if len(objetos) > MAX_LICITACOES_LLM:
        logger.info(f"Limitando classificacao a {MAX_LICITACOES_LLM} de {len(objetos)} licitacoes")
        # Prioriza as de maior valor
        objetos = sorted(
            objetos,
            key=lambda x: float(x.get("valor_total_estimado") or 0),
            reverse=True
        )[:MAX_LICITACOES_LLM]

    logger.info(f"Classificando {len(objetos)} licitacoes | Modelo: {GROQ_MODEL}")

    classificacoes, erros, tokens_total = [], 0, 0

    for i, item in enumerate(objetos):
        objeto = item.get("objeto_compra", "").strip()
        if not objeto or len(objeto) < 5:
            continue

        try:
            prompt_sis, prompt_usr = _montar_prompt(objeto, CATEGORIAS)
            resultado = _chamar_groq(prompt_sis, prompt_usr)

            match = re.search(r"\{.*\}", resultado["conteudo"].strip(), re.DOTALL)
            if not match:
                raise ValueError(f"Nenhum JSON encontrado: {resultado['conteudo'][:100]}")

            dados = json.loads(match.group())
            categoria = dados.get("categoria", "Outros")
            if categoria not in CATEGORIAS:
                categoria = "Outros"

            confianca = dados.get("confianca", "BAIXA").upper().replace("MEDIA", "MEDIA")
            if confianca not in ("ALTA", "MEDIA", "BAIXA"):
                confianca = "BAIXA"

            classificacoes.append({
                **item,
                "categoria": categoria,
                "confianca": confianca,
                "objeto_vago": bool(dados.get("objeto_vago", False)),
                "justificativa_vago": dados.get("justificativa_vago", ""),
                "resumo": dados.get("resumo", ""),
                "tokens_usados": resultado["tokens"],
                "modelo_usado": resultado["modelo"],
                "provider_llm": "groq",
            })
            tokens_total += resultado["tokens"]

            if (i + 1) % 10 == 0:
                vagos_ate_agora = sum(1 for c in classificacoes if c["objeto_vago"])
                logger.info(f"  Progresso: {i+1}/{len(objetos)} | Tokens: {tokens_total} | Vagos: {vagos_ate_agora}")

        except Exception as e:
            erros += 1
            logger.error(f"[{i+1}] Erro ao classificar '{objeto[:60]}': {e}")
            classificacoes.append({
                **item,
                "categoria": "Outros",
                "confianca": "BAIXA",
                "objeto_vago": False,
                "justificativa_vago": "",
                "resumo": f"Erro na classificacao: {str(e)[:100]}",
                "tokens_usados": 0,
                "modelo_usado": GROQ_MODEL,
                "provider_llm": "groq",
            })

        if (i + 1) % BATCH_SIZE == 0:
            time.sleep(1)

    vagos = sum(1 for c in classificacoes if c.get("objeto_vago"))
    logger.info(
        f"Classificacao concluida: {len(classificacoes)} | "
        f"Vagos: {vagos} ({vagos/len(classificacoes)*100:.1f}% se houver) | "
        f"Erros: {erros} | Tokens: {tokens_total}"
    )
    return classificacoes


# ─────────────────────────────────────────────────────────────────────────────
# TASK 4 - SALVAR CLASSIFICACOES
# ─────────────────────────────────────────────────────────────────────────────

def salvar_classificacoes(classificacoes):
    if not classificacoes:
        logger.info("Nenhuma classificacao para salvar.")
        return 0

    sql_truncate = "TRUNCATE TABLE licitacoes_classificadas RESTART IDENTITY;"
    sql_insert = """
        INSERT INTO licitacoes_classificadas (
            numero_controle_pncp, objeto_compra, orgao_nome, uf,
            modalidade_nome, categoria, confianca, objeto_vago,
            justificativa_vago, resumo, valor_total_estimado,
            tokens_usados, modelo_usado, provider_llm, data_publicacao
        ) VALUES %s
    """
    registros = [
        (
            c.get("numero_controle_pncp"),
            c.get("objeto_compra"),
            c.get("orgao_nome"),
            c.get("uf"),
            c.get("modalidade_nome"),
            c.get("categoria"),
            c.get("confianca"),
            c.get("objeto_vago"),
            c.get("justificativa_vago"),
            c.get("resumo"),
            c.get("valor_total_estimado"),
            c.get("tokens_usados", 0),
            c.get("modelo_usado"),
            c.get("provider_llm"),
            c.get("data_publicacao"),
        )
        for c in classificacoes
    ]

    with get_db_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(sql_truncate)
            execute_values(cur, sql_insert, registros)
        conn.commit()

    vagos = sum(1 for c in classificacoes if c.get("objeto_vago"))
    logger.info(f"{len(registros)} classificacoes salvas | {vagos} objetos vagos.")
    return len(registros)


# ─────────────────────────────────────────────────────────────────────────────
# TASK 5 - RELATORIO HTML MELHORADO
# ─────────────────────────────────────────────────────────────────────────────

def gerar_relatorio(classificacoes):
    if not classificacoes:
        logger.info("Sem dados para relatorio.")
        return

    total = len(classificacoes)
    vagos = [c for c in classificacoes if c.get("objeto_vago")]
    tokens_total = sum(c.get("tokens_usados", 0) for c in classificacoes)
    modelo_usado = classificacoes[0].get("modelo_usado", "?")
    data_exec = datetime.now().strftime("%d/%m/%Y %H:%M")

    # Distribuicao por categoria
    por_categoria = {}
    for c in classificacoes:
        cat = c.get("categoria", "Outros")
        por_categoria[cat] = por_categoria.get(cat, 0) + 1
    cats_ordenadas = sorted(por_categoria.items(), key=lambda x: x[1], reverse=True)

    # Distribuicao por modalidade
    por_modalidade = {}
    for c in classificacoes:
        mod = c.get("modalidade_nome") or "Nao informado"
        por_modalidade[mod] = por_modalidade.get(mod, 0) + 1
    mods_ordenadas = sorted(por_modalidade.items(), key=lambda x: x[1], reverse=True)

    # Distribuicao por confianca
    por_confianca = {}
    for c in classificacoes:
        conf = c.get("confianca", "BAIXA")
        por_confianca[conf] = por_confianca.get(conf, 0) + 1

    # Top 20 por valor
    top_valor = sorted(
        [c for c in classificacoes if c.get("valor_total_estimado")],
        key=lambda x: float(x.get("valor_total_estimado") or 0),
        reverse=True
    )[:20]

    # Valor total geral
    valor_total = sum(float(c.get("valor_total_estimado") or 0) for c in classificacoes)

    # Barras de categoria
    max_cat = cats_ordenadas[0][1] if cats_ordenadas else 1
    barras_cat = ""
    cores = ["#1565C0", "#00838F", "#2E7D32", "#6A1B9A", "#E65100",
             "#AD1457", "#00695C", "#4527A0", "#BF360C", "#1B5E20", "#37474F"]
    for idx, (cat, qtd) in enumerate(cats_ordenadas):
        pct = round(qtd / total * 100, 1)
        largura = round(qtd / max_cat * 100)
        cor = cores[idx % len(cores)]
        barras_cat += f"""
        <div class="bar-row">
          <span class="bar-label">{cat}</span>
          <div class="bar-wrap"><div class="bar-fill" style="width:{largura}%;background:{cor}"></div></div>
          <span class="bar-count">{qtd} ({pct}%)</span>
        </div>"""

    # Barras de modalidade
    max_mod = mods_ordenadas[0][1] if mods_ordenadas else 1
    barras_mod = ""
    for mod, qtd in mods_ordenadas:
        pct = round(qtd / total * 100, 1)
        largura = round(qtd / max_mod * 100)
        barras_mod += f"""
        <div class="bar-row">
          <span class="bar-label" style="width:280px">{mod[:40]}</span>
          <div class="bar-wrap"><div class="bar-fill" style="width:{largura}%;background:#455A64"></div></div>
          <span class="bar-count">{qtd} ({pct}%)</span>
        </div>"""

    # Linhas vagos
    linhas_vagos = ""
    for v in sorted(vagos, key=lambda x: float(x.get("valor_total_estimado") or 0), reverse=True)[:50]:
        objeto = (v.get("objeto_compra") or "")[:100]
        valor = v.get("valor_total_estimado")
        valor_fmt = f"R$ {float(valor):,.2f}".replace(",", "X").replace(".", ",").replace("X", ".") if valor else "-"
        linhas_vagos += f"""
        <tr>
          <td>{(v.get('orgao_nome') or '')[:45]}</td>
          <td>{objeto}{"..." if len(v.get("objeto_compra", "")) > 100 else ""}</td>
          <td>{v.get('categoria', '')}</td>
          <td style="color:#C62828;font-weight:500">{valor_fmt}</td>
          <td>{(v.get('justificativa_vago') or '')[:80]}</td>
        </tr>"""

    # Linhas top valor
    linhas_top = ""
    for t in top_valor:
        objeto = (t.get("objeto_compra") or "")[:80]
        valor = t.get("valor_total_estimado")
        valor_fmt = f"R$ {float(valor):,.2f}".replace(",", "X").replace(".", ",").replace("X", ".") if valor else "-"
        vago_badge = '<span style="color:#C62828;font-weight:600">VAGO</span>' if t.get("objeto_vago") else ""
        linhas_top += f"""
        <tr>
          <td>{(t.get('orgao_nome') or '')[:45]}</td>
          <td>{objeto}{"..." if len(t.get("objeto_compra", "")) > 80 else ""} {vago_badge}</td>
          <td>{t.get('categoria', '')}</td>
          <td>{(t.get('modalidade_nome') or '')[:25]}</td>
          <td style="text-align:right;font-weight:600;color:#0D2B55">{valor_fmt}</td>
        </tr>"""

    # Badges confianca
    alta = por_confianca.get("ALTA", 0)
    media = por_confianca.get("MEDIA", 0)
    baixa = por_confianca.get("BAIXA", 0)
    valor_total_fmt = f"R$ {valor_total:,.0f}".replace(",", "X").replace(".", ",").replace("X", ".")

    html = f"""<!DOCTYPE html>
<html lang="pt-BR">
<head>
<meta charset="UTF-8">
<title>Relatorio de Licitacoes - {data_exec}</title>
<style>
  * {{ box-sizing: border-box; }}
  body {{ font-family: Arial, sans-serif; margin: 0; padding: 20px; background:#f0f2f5; color:#333; }}
  .header {{ background:linear-gradient(135deg,#0D2B55,#1565C0); color:white; padding:28px 32px; border-radius:10px; margin-bottom:24px; }}
  .header h1 {{ margin:0; font-size:22px; }}
  .header p  {{ margin:6px 0 0; opacity:.8; font-size:13px; }}
  .cards {{ display:flex; gap:14px; margin-bottom:24px; flex-wrap:wrap; }}
  .card {{ background:white; border-radius:10px; padding:18px 22px; flex:1; min-width:130px;
           box-shadow:0 2px 8px rgba(0,0,0,.08); border-top:4px solid #1565C0; }}
  .card.green {{ border-color:#2E7D32; }}
  .card.red   {{ border-color:#C62828; }}
  .card.teal  {{ border-color:#00838F; }}
  .card.gray  {{ border-color:#455A64; }}
  .card .num {{ font-size:28px; font-weight:700; color:#0D2B55; }}
  .card.green .num {{ color:#2E7D32; }}
  .card.red   .num {{ color:#C62828; }}
  .card.teal  .num {{ color:#00838F; }}
  .card .lbl {{ font-size:11px; color:#777; margin-top:4px; text-transform:uppercase; letter-spacing:.5px; }}
  .section {{ background:white; border-radius:10px; padding:24px; margin-bottom:20px;
              box-shadow:0 2px 8px rgba(0,0,0,.08); }}
  .section h2 {{ margin-top:0; font-size:15px; color:#0D2B55; border-bottom:2px solid #E3F2FD;
                 padding-bottom:10px; margin-bottom:18px; }}
  .two-col {{ display:flex; gap:20px; }}
  .two-col .section {{ flex:1; }}
  .bar-row {{ display:flex; align-items:center; gap:12px; margin-bottom:9px; }}
  .bar-label {{ width:220px; font-size:12px; text-align:right; color:#555; flex-shrink:0; }}
  .bar-wrap  {{ flex:1; background:#ECEFF1; border-radius:4px; height:16px; }}
  .bar-fill  {{ background:#1565C0; border-radius:4px; height:100%; transition:width .3s; }}
  .bar-count {{ width:90px; font-size:11px; color:#888; flex-shrink:0; }}
  .conf-badges {{ display:flex; gap:12px; margin-bottom:16px; }}
  .badge {{ padding:6px 16px; border-radius:20px; font-size:13px; font-weight:600; }}
  .badge.alta  {{ background:#E8F5E9; color:#2E7D32; }}
  .badge.media {{ background:#FFF8E1; color:#F57F17; }}
  .badge.baixa {{ background:#FFEBEE; color:#C62828; }}
  table {{ width:100%; border-collapse:collapse; font-size:12px; }}
  th {{ background:#0D2B55; color:white; padding:9px 10px; text-align:left; font-weight:500; }}
  td {{ padding:8px 10px; border-bottom:1px solid #F0F0F0; vertical-align:top; }}
  tr:hover td {{ background:#F5F9FF; }}
  .footer {{ text-align:center; font-size:11px; color:#AAA; margin-top:24px; padding:12px; }}
</style>
</head>
<body>

<div class="header">
  <h1>Relatorio de Classificacao de Licitacoes - Ceara (v2)</h1>
  <p>Gerado em {data_exec} | Modelo: {modelo_usado} | Janela: {DIAS_JANELA} dias | Modalidades: {len(MODALIDADES)}</p>
</div>

<div class="cards">
  <div class="card"><div class="num">{total}</div><div class="lbl">Licitacoes classificadas</div></div>
  <div class="card red"><div class="num">{len(vagos)}</div><div class="lbl">Objetos vagos</div></div>
  <div class="card teal"><div class="num">{len(cats_ordenadas)}</div><div class="lbl">Categorias</div></div>
  <div class="card green"><div class="num">{valor_total_fmt}</div><div class="lbl">Valor total estimado</div></div>
  <div class="card gray"><div class="num">{tokens_total:,}</div><div class="lbl">Tokens LLM</div></div>
</div>

<div class="section">
  <h2>Confianca das Classificacoes</h2>
  <div class="conf-badges">
    <span class="badge alta">ALTA: {alta} ({round(alta/total*100,1)}%)</span>
    <span class="badge media">MEDIA: {media} ({round(media/total*100,1)}%)</span>
    <span class="badge baixa">BAIXA: {baixa} ({round(baixa/total*100,1)}%)</span>
  </div>
</div>

<div class="two-col">
  <div class="section">
    <h2>Distribuicao por Categoria Tematica</h2>
    {barras_cat}
  </div>
  <div class="section">
    <h2>Distribuicao por Modalidade</h2>
    {barras_mod}
  </div>
</div>

<div class="section">
  <h2>Top 20 Licitacoes por Valor Estimado</h2>
  <table>
    <thead><tr><th>Orgao</th><th>Objeto</th><th>Categoria</th><th>Modalidade</th><th>Valor</th></tr></thead>
    <tbody>{linhas_top}</tbody>
  </table>
</div>

<div class="section">
  <h2>Objetos com Descricao Vaga ({len(vagos)} licitacoes)</h2>
  <table>
    <thead><tr><th>Orgao</th><th>Objeto original</th><th>Categoria</th><th>Valor</th><th>Por que e vago</th></tr></thead>
    <tbody>{linhas_vagos if linhas_vagos else "<tr><td colspan='5' style='text-align:center;color:#888'>Nenhum objeto vago detectado</td></tr>"}</tbody>
  </table>
</div>

<div class="footer">Pipeline classificacao_licitacoes v2 | Execucao local | {data_exec}</div>
</body>
</html>"""

    caminho = Path("relatorio_licitacoes_v2.html")
    caminho.write_text(html, encoding="utf-8")
    logger.info(f"Relatorio salvo em {caminho.resolve()} ({len(html)} bytes)")
    return str(caminho)


# ─────────────────────────────────────────────────────────────────────────────
# EXECUCAO LOCAL
# ─────────────────────────────────────────────────────────────────────────────

if __name__ == "__main__":
    licitacoes = extrair_licitacoes()
    objetos, _ = salvar_postgres(licitacoes)
    classificacoes = classificar_com_llm(objetos)
    salvar_classificacoes(classificacoes)
    gerar_relatorio(classificacoes)
