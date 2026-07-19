# 🖥️ Comandos Utilizados — Do Zero ao HTML

**Disciplina:** Arquitetura e Automação de Pipeline de Dados — UNIFOR 2026  
**Registro completo dos comandos executados durante o desenvolvimento do pipeline**

---

## 1️⃣ CONFIGURAÇÃO DO AMBIENTE DOCKER

### Verificar containers rodando
```bash
# No PowerShell do Windows
docker ps
```

### Entrar no container do Airflow
```bash
# PowerShell — abre terminal bash dentro do container
docker exec -it aula_airflow /bin/bash -c "source /home/airflow-venv/bin/activate && bash"
```

### Ativar o ambiente virtual (dentro do container)
```bash
source /home/airflow-venv/bin/activate
# O prompt muda para: (airflow-venv) root@...
```

---

## 2️⃣ INSTALAÇÃO DE DEPENDÊNCIAS

```bash
# Dentro do container com venv ativo
pip install groq psycopg2-binary requests scikit-learn pandas numpy

# Verificar instalação
python3 -c "import groq; print('groq ok')"
python3 -c "import psycopg2; print('psycopg2 ok')"
```

> ⚠️ **Problema encontrado:** psycopg2 incompatível com Python 3.14  
> **Solução:** Migrar para psycopg3

```bash
pip uninstall psycopg2-binary -y
pip install psycopg[binary]
```

---

## 3️⃣ CONFIGURAÇÃO DO POSTGRESQL

### Descobrir o IP do container postgres
```bash
# PowerShell do Windows
docker inspect aula_postgres --format "{{range .NetworkSettings.Networks}}{{.IPAddress}}{{end}}"
# Resultado: 172.17.0.3
```

### Criar o banco de dados
```bash
docker exec aula_postgres psql -U postgres -c "CREATE DATABASE aula;"
```

### Definir senha do usuário
```bash
docker exec aula_postgres psql -U postgres -c "ALTER USER postgres PASSWORD 'postgres';"
```

### Descobrir caminho do pg_hba.conf
```bash
docker exec aula_postgres psql -U postgres -c "SHOW hba_file;"
# Resultado: /var/lib/postgresql/18/docker/pg_hba.conf
```

### Liberar acesso externo (entre containers)
```bash
docker exec aula_postgres bash -c "echo 'host all all 0.0.0.0/0 md5' >> /var/lib/postgresql/18/docker/pg_hba.conf"
docker exec aula_postgres psql -U postgres -c "SELECT pg_reload_conf();"
```

### Testar conexão (dentro do container airflow)
```bash
python3 -c "
import psycopg as p
conn = p.connect(host='172.17.0.3', port=5432, dbname='aula', user='postgres', password='postgres')
print('Conexão OK!')
conn.close()
"
```

---

## 4️⃣ CÓPIA DE ARQUIVOS PARA O CONTAINER

```bash
# PowerShell do Windows — copiar DAG para a pasta de DAGs do Airflow
docker cp C:\Users\marci\Downloads\dag_trabalho_final_marcinha.py aula_airflow:/root/airflow/dags/

# Confirmar que chegou (dentro do container)
ls /root/airflow/dags/
# Deve mostrar: dag_anomalias_contratos.py  dag_exemplo.py  dag_trabalho_final_marcinha.py
```

---

## 5️⃣ CONFIGURAÇÃO DO AIRFLOW

### Subir o Airflow standalone
```bash
# PowerShell — sobe scheduler + api-server + dag-processor juntos
docker exec -it aula_airflow /bin/bash -c "source /home/airflow-venv/bin/activate && airflow standalone"
```

### Criar variáveis via terminal (mais confiável que pelo UI)
```bash
# Dentro do container com venv ativo
airflow variables set GROQ_API_KEY "gsk_SUA_CHAVE_AQUI"
airflow variables set DB_HOST "172.17.0.3"
airflow variables set DB_NAME "aula"
airflow variables set DB_USER "postgres"
airflow variables set DB_PASSWORD "postgres"

# Verificar se foram criadas
airflow variables get GROQ_API_KEY
airflow variables get DB_HOST
```

### Verificar variáveis via Python
```bash
python3 -c "
from airflow.models import Variable
print('GROQ_API_KEY:', Variable.get('GROQ_API_KEY', default_var='NAO ENCONTRADA'))
print('DB_HOST:', Variable.get('DB_HOST', default_var='NAO ENCONTRADA'))
print('DB_PASSWORD:', Variable.get('DB_PASSWORD', default_var='NAO ENCONTRADA'))
"
```

### Forçar releitura das DAGs
```bash
airflow dags reserialize
```

### Listar DAGs reconhecidas
```bash
airflow dags list
```

---

## 6️⃣ EXECUÇÃO DO SCRIPT LOCAL (Aula 4)

```bash
# Configurar variáveis de ambiente para o script local
export GROQ_API_KEY="gsk_SUA_CHAVE_AQUI"
export DB_HOST="172.17.0.3"
export DB_NAME="aula"
export DB_PASSWORD="postgres"

# Rodar o script
python /root/classificacao_licitacoes_groq.py

# Versão melhorada (v2 — múltiplas modalidades)
python /root/classificacao_licitacoes_groq_v2.py
```

---

## 7️⃣ DIAGNÓSTICO E RESOLUÇÃO DE PROBLEMAS

### Verificar processos do Airflow rodando
```bash
ps aux | grep airflow
# Verificar se scheduler, api-server e dag-processor estão ativos
```

### Verificar configuração do Airflow
```bash
airflow config get-value core dags_folder
airflow config get-value core dagbag_import_timeout
```

### Aumentar timeout de importação das DAGs
```bash
airflow config set core dagbag_import_timeout 120
```

### Verificar encoding de arquivo
```bash
python3 -c "
with open('/root/arquivo.py', 'rb') as f:
    data = f.read()
print('Tamanho:', len(data))
print('Bytes 95-110:', data[95:110])
"
```

### Converter encoding de arquivo (latin-1 para UTF-8)
```bash
python3 << 'EOF'
content = open('/root/arquivo.py', 'rb').read()
for enc in ['utf-8', 'latin-1', 'cp1252']:
    try:
        decoded = content.decode(enc)
        with open('/root/arquivo.py', 'w', encoding='utf-8') as f:
            f.write(decoded)
        print(f'Convertido de {enc} para UTF-8')
        break
    except Exception as e:
        print(f'{enc} falhou: {e}')
EOF
```

---

## 8️⃣ EXTRAÇÃO DO RELATÓRIO HTML

```bash
# PowerShell do Windows — copiar relatório do container para o Windows
docker cp aula_airflow:/root/relatorio_trabalho_final_marcinha.html C:\Users\marci\Downloads\relatorio_trabalho_final_marcinha.html

# Relatório da Aula 4
docker cp aula_airflow:/root/relatorio_licitacoes.html C:\Users\marci\Downloads\relatorio_licitacoes.html
```

---

## 9️⃣ CONSULTAS SQL DE VALIDAÇÃO

Cole no **DBeaver** conectado ao PostgreSQL (`172.17.0.3:5432`):

```sql
-- Total extraído vs classificado
SELECT
    (SELECT COUNT(*) FROM contratos_api)            AS total_extraidos,
    (SELECT COUNT(*) FROM contratos_classificados)  AS total_classificados,
    (SELECT COUNT(*) FROM contratos_classificados
     WHERE objeto_vago = true)                      AS total_vagos;

-- Distribuição por categoria
SELECT
    categoria,
    COUNT(*) AS qtd,
    ROUND(COUNT(*) * 100.0 / SUM(COUNT(*)) OVER(), 1) AS pct
FROM contratos_classificados
GROUP BY categoria
ORDER BY qtd DESC;

-- Top 10 contratos por valor com classificação
SELECT
    numero_contrato,
    LEFT(objeto, 80) AS objeto,
    categoria,
    confianca,
    TO_CHAR(valor_global, 'FM999G999G999D00') AS valor
FROM contratos_classificados
ORDER BY valor_global DESC
LIMIT 10;

-- Tokens consumidos por modelo
SELECT
    modelo_usado,
    COUNT(*) AS contratos,
    SUM(tokens_usados) AS total_tokens
FROM contratos_classificados
GROUP BY modelo_usado;
```

---

## 🗺️ Fluxo Completo Resumido

```
Windows PowerShell                    Container aula_airflow
─────────────────                     ──────────────────────
docker exec → bash            →       source venv/activate
                                      pip install groq psycopg...
docker cp arquivo.py → /dags/ →       ls /root/airflow/dags/
                                      airflow variables set GROQ_API_KEY ...
docker exec → airflow standalone →    [Airflow rodando na porta 8080]
                                      
Airflow UI (localhost:8080)
──────────────────────────
Trigger DAG → 5 tasks executam → HTML gerado

Windows PowerShell
──────────────────
docker cp /root/relatorio.html → Downloads/
Abrir no navegador ✅
```

---

## ⚠️ Principais Erros Encontrados e Soluções

| Erro | Causa | Solução |
|------|-------|---------|
| `docker: command not found` | Rodar `docker cp` dentro do container | Usar PowerShell do Windows |
| `source: not found` | Terminal Exec usa `sh` em vez de `bash` | Usar `Open in external terminal` ou PowerShell |
| `UnicodeDecodeError 0xe7` | psycopg2 incompatível com Python 3.14 | Migrar para `psycopg[binary]` |
| `Connection refused port 5432` | Container usando `localhost` em vez do IP | Usar IP `172.17.0.3` do container |
| `FATAL: autenticação falhou` | `pg_hba.conf` não aceitava conexões externas | Adicionar regra `host all all 0.0.0.0/0 md5` |
| `DagBag import timeout 30s` | Bibliotecas ML pesadas no import | Aumentar `dagbag_import_timeout` para 120 |
| `Todos classificados como Outros` | `GROQ_MODEL` recebeu a API key por engano | Corrigir para `llama-3.3-70b-versatile` |
| `airflow: not found` no Exec | Terminal Exec não tem venv ativo | Usar `docker exec` com `source venv/activate` |

---

*Registro de comandos — Arquitetura e Automação de Pipeline de Dados · UNIFOR 2026*
