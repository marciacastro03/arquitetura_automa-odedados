# 🔍 Aula 3 — Detecção de Anomalias em Contratos Públicos

**Disciplina:** Arquitetura e Automação de Pipeline de Dados — UNIFOR 2026  
**Algoritmo:** Isolation Forest · **Orquestração:** Apache Airflow

---

## 📌 Contexto

A Controladoria-Geral do Estado solicitou um pipeline automatizado capaz de identificar, diariamente, contratos públicos com comportamento financeiro atípico — sem regras fixas, aprendendo o padrão histórico.

## 🏗️ Arquitetura (4 Tasks)

```
extrair_contratos → salvar_postgres → detectar_anomalias → salvar_anomalias
```

| Task | Descrição |
|------|-----------|
| `extrair_contratos` | Coleta paginada da API do Ceará Transparente (últimos 30 dias) |
| `salvar_postgres` | Persiste na tabela `contratos` com ON CONFLICT DO NOTHING |
| `detectar_anomalias` | Aplica Isolation Forest sobre features numéricas |
| `salvar_anomalias` | Salva anomalias com score e nível de risco (ALTO/MÉDIO/BAIXO) |

## 🧠 Feature Engineering

| Feature | Derivada de | Transformação |
|---------|------------|---------------|
| `log_valor_global` | valor do contrato | log1p() |
| `log_valor_por_dia` | valor / prazo | log1p() |
| `prazo_vigencia_dias` | data início/fim | bruto |

## ⚙️ Hiperparâmetros do Isolation Forest

```python
IsolationForest(
    n_estimators=200,
    contamination=0.05,  # ~5% de anomalias esperadas
    random_state=42,
    n_jobs=-1
)
```

## 🗄️ Tabelas PostgreSQL

- `contratos` — dados brutos normalizados
- `anomalias_contratos` — contratos anômalos com score e nível de risco

## 📦 Dependências

```bash
pip install scikit-learn pandas psycopg2-binary requests numpy
```
