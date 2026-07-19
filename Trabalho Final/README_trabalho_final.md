# 🏆 Trabalho Final — Pipeline Completo de Contratos Públicos

**Disciplina:** Arquitetura e Automação de Pipeline de Dados — UNIFOR 2026  
**Prof. MSc. Daniel Teófilo · Data de Entrega: 11/07/2026**

---

## 📌 Objetivo

Pipeline de dados end-to-end integrando coleta de dados públicos, armazenamento relacional, classificação com Inteligência Artificial e geração de relatórios — orquestrado pelo Apache Airflow.

## 🏗️ Arquitetura (5 Tasks)

```
extrair_contratos → salvar_postgres → classificar_com_llm → salvar_classificados → gerar_relatorio
```

| Task | Tabela | Descrição |
|------|--------|-----------|
| `extrair_contratos` | — | Coleta paginada da API Ceará Transparente |
| `salvar_postgres` | `contratos_api` | Persistência idempotente dos dados brutos |
| `classificar_com_llm` | — | Classifica top 30 por valor via Groq LLM |
| `salvar_classificados` | `contratos_classificados` | Persiste classificações com metadados do LLM |
| `gerar_relatorio` | — | Gera relatório HTML automatizado |

## 📊 Resultados

| Métrica | Valor |
|---------|-------|
| Contratos extraídos | 2.000 |
| Contratos classificados | 30 (top 30 por valor) |
| Valor total classificado | R$ 15.848.304.609,00 |
| Tokens LLM consumidos | 12.247 |
| Confiança ALTA | 66,7% |
| Objetos vagos detectados | 5 |

## 🏷️ Distribuição por Categoria

| Categoria | Qtd | % |
|-----------|-----|---|
| Infraestrutura e Obras | 12 | 40,0% |
| Serviços Gerais | 4 | 13,3% |
| Saúde | 4 | 13,3% |
| Educação | 3 | 10,0% |
| Meio Ambiente e Saneamento | 1 | 3,3% |
| Tecnologia da Informação | 1 | 3,3% |
| Outros | 5 | 16,7% |

## ⚙️ Variáveis do Airflow necessárias

| Key | Value |
|-----|-------|
| `GROQ_API_KEY` | sua chave gsk_... |
| `DB_HOST` | IP do container postgres |
| `DB_NAME` | aula |
| `DB_USER` | postgres |
| `DB_PASSWORD` | postgres |

## 📦 Dependências

```bash
pip install groq psycopg2-binary requests
```

## 📂 Arquivos

| Arquivo | Descrição |
|---------|-----------|
| `dag_trabalho_final_marcinha.py` | DAG completa com as 5 tasks |
| `relatorio_trabalho_final_marcinha.html` | Relatório gerado automaticamente pelo pipeline |
