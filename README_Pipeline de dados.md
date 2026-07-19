# 🚀 Arquitetura e Automação de Pipeline de Dados
**MBA em Ciência de Dados — UNIFOR 2026**  
**Prof. MSc. Daniel Teófilo**  
**Aluna: Marcia Maria dos Santos Castro**

---

## 📋 Sobre o Repositório

Este repositório reúne todos os projetos desenvolvidos na disciplina de **Arquitetura e Automação de Pipeline de Dados**, cobrindo desde detecção de anomalias com Machine Learning até classificação de licitações públicas com LLM, tudo orquestrado pelo **Apache Airflow**.

---

## 🗂️ Estrutura do Repositório

```
pipeline-dados-unifor/
│
├── aula_3_anomalias_contratos/
│   ├── dag_anomalias_contratos.py       # DAG completa — Detecção de Anomalias
│   └── README.md                         # Documentação da Aula 3
│
├── aula_4_classificacao_licitacoes/
│   ├── classificacao_licitacoes_groq.py  # Script local — Classificação com Groq
│   ├── classificacao_licitacoes_groq_v2.py # Versão melhorada — múltiplas modalidades
│   ├── relatorio_licitacoes.html         # Relatório gerado pelo pipeline
│   └── README.md                         # Documentação da Aula 4
│
├── trabalho_final/
│   ├── dag_trabalho_final_marcinha.py    # DAG completa — Pipeline Final (5 tasks)
│   ├── relatorio_trabalho_final_marcinha.html # Relatório gerado automaticamente
│   └── README.md                         # Documentação do Trabalho Final
│
└── README.md                             # Este arquivo
```

---

## 📚 Projetos

### 🔍 Aula 3 — Detecção de Anomalias em Contratos Públicos

**Objetivo:** Pipeline ETL automatizado que identifica contratos públicos com comportamento financeiro atípico usando Machine Learning não supervisionado.

**Tecnologias:** Apache Airflow · PostgreSQL · Scikit-learn · Python 3

**Como funciona:**
- Coleta contratos da API paginada do **Ceará Transparente**
- Persiste os dados brutos no **PostgreSQL**
- Aplica **Isolation Forest** sobre features numéricas (valor, prazo, valor/dia)
- Classifica anomalias em risco **ALTO / MÉDIO / BAIXO** por percentil

**Pipeline (4 tasks):**
```
extrair_contratos → salvar_postgres → detectar_anomalias → salvar_anomalias
```

---

### 🤖 Aula 4 — Classificação de Licitações com LLM

**Objetivo:** Pipeline que classifica semanticamente licitações públicas do PNCP usando Large Language Model, sem necessidade de dados de treinamento.

**Tecnologias:** Groq (llama-3.3-70b-versatile) · PostgreSQL · Python 3 · API PNCP

**Como funciona:**
- Coleta licitações da **API do PNCP** (Portal Nacional de Contratações Públicas)
- Classifica cada licitação por categoria temática via **Groq LLM** (zero-shot)
- Detecta objetos com descrição vaga ou genérica
- Gera relatório HTML com distribuição por categoria

**Resultados (v2 — múltiplas modalidades):**
- 18 licitações classificadas · 2 categorias · 10.333 tokens consumidos

---

### 🏆 Trabalho Final — Pipeline Completo de Contratos Públicos

**Objetivo:** Pipeline de dados end-to-end integrando coleta de dados públicos, armazenamento relacional, classificação com IA e geração de relatórios, orquestrado pelo Apache Airflow.

**Tecnologias:** Apache Airflow · PostgreSQL · Groq LLM · Docker · Python 3

**Pipeline (5 tasks):**
```
extrair_contratos → salvar_postgres → classificar_com_llm → salvar_classificados → gerar_relatorio
```

**Resultados obtidos:**
| Métrica | Valor |
|---------|-------|
| Contratos extraídos | 2.000 |
| Contratos classificados (top 30 por valor) | 30 |
| Valor total classificado | R$ 15.848.304.609,00 |
| Tokens LLM consumidos | 12.247 |
| Confiança ALTA | 66,7% |
| Objetos vagos detectados | 5 |

**Categorias identificadas:**
- Infraestrutura e Obras (40%)
- Serviços Gerais (13,3%)
- Saúde (13,3%)
- Educação (10%)
- Meio Ambiente e Saneamento (3,3%)
- Tecnologia da Informação (3,3%)

---

## 🛠️ Stack Tecnológico

| Componente | Tecnologia |
|-----------|-----------|
| Orquestração | Apache Airflow 3.x |
| Linguagem | Python 3.14 |
| Banco de Dados | PostgreSQL 18 |
| Containerização | Docker Desktop |
| LLM | Groq — llama-3.3-70b-versatile |
| ML | Scikit-learn — Isolation Forest |
| Fonte de Dados | API Ceará Transparente · API PNCP |

---

## ⚙️ Como Executar

### Pré-requisitos
- Docker Desktop instalado
- Conta no [Groq](https://console.groq.com) com API Key

### 1. Suba os containers
```bash
# Container do Airflow
docker run -d --name aula_airflow -p 8080:8080 ubuntu:latest

# Container do PostgreSQL
docker run -d --name aula_postgres -p 5432:5432 \
  -e POSTGRES_PASSWORD=postgres postgres
```

### 2. Configure o Airflow
```bash
docker exec -it aula_airflow bash
source /home/airflow-venv/bin/activate
airflow standalone
```

### 3. Configure as variáveis no Airflow UI
Acesse `http://localhost:8080` → Admin → Variables:

| Key | Value |
|-----|-------|
| `GROQ_API_KEY` | sua chave gsk_... |
| `DB_HOST` | IP do container postgres |
| `DB_NAME` | aula |
| `DB_USER` | postgres |
| `DB_PASSWORD` | postgres |

### 4. Copie a DAG e execute
```bash
docker cp dag_trabalho_final_marcinha.py aula_airflow:/root/airflow/dags/
```
Acesse o Airflow UI → ative a DAG → clique em ▶ Trigger DAG.

---

## 📊 Fontes de Dados

- **Ceará Transparente:** https://api-dados-abertos.cearatransparente.ce.gov.br
- **PNCP:** https://pncp.gov.br/api/consulta/v1/contratacoes/publicacao

---

## 👩‍💻 Autora

**Marcia Maria dos Santos Castro**  
Analista de Cultura e Experiência do Colaborador · M. Dias Branco  
MBA em Ciência de Dados — UNIFOR 2026  
GitHub: [@marciacastro03](https://github.com/marciacastro03)

---

*Disciplina: Arquitetura e Automação de Pipeline de Dados · UNIFOR 2026 · Prof. MSc. Daniel Teófilo*
