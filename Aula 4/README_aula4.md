# 🤖 Aula 4 — Classificação de Licitações com LLM

**Disciplina:** Arquitetura e Automação de Pipeline de Dados — UNIFOR 2026  
**Modelo:** llama-3.3-70b-versatile via Groq · **API:** PNCP

---

## 📌 Contexto

A Secretaria de Planejamento e Gestão do Ceará precisava classificar automaticamente centenas de licitações do PNCP por área temática, sem base de treinamento — usando LLM zero-shot.

## 🏗️ Arquitetura (5 Tasks)

```
extrair_licitacoes → salvar_postgres → classificar_com_llm → salvar_classificacoes → gerar_relatorio
```

## 🧠 Por que LLM?

| NLP Clássico | LLM (Groq) |
|-------------|-----------|
| Precisa de centenas de exemplos rotulados | Zero-shot — sem exemplos |
| Não generaliza para novas categorias | Classifica qualquer categoria |
| Meses de implantação | Implantação em horas |

## 📂 Arquivos

| Arquivo | Descrição |
|---------|-----------|
| `classificacao_licitacoes_groq.py` | Versão inicial — modalidade 1, janela 365 dias |
| `classificacao_licitacoes_groq_v2.py` | Versão melhorada — 5 modalidades, janela 90 dias, até 500 licitações |

## 🏷️ Categorias

`Saude` · `Educacao` · `Infraestrutura e Obras` · `Tecnologia da Informacao` · `Alimentacao e Nutricao` · `Seguranca Publica` · `Meio Ambiente e Saneamento` · `Transporte e Logistica` · `Administrativo e Material de Escritorio` · `Servicos Gerais` · `Outros`

## ⚙️ Configuração

```bash
export GROQ_API_KEY="gsk_..."
export DB_HOST="172.17.0.3"
export DB_NAME="aula"
export DB_PASSWORD="postgres"

python classificacao_licitacoes_groq_v2.py
```

## 📦 Dependências

```bash
pip install groq psycopg2-binary requests pandas numpy
```
