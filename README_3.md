# 📚 Web Crawling, Scraping e Coleta Automatizada de Dados

> **Pós-Graduação em Data Science – UNIFOR (Universidade de Fortaleza)**  
> Repositório de atividades práticas desenvolvidas ao longo da disciplina.

---

## 🗂️ Índice

- [Sobre a Disciplina](#sobre-a-disciplina)
- [Módulo 1 – Fundamentos de Web Scraping e Ferramentas Essenciais](#módulo-1--fundamentos-de-web-scraping-e-ferramentas-essenciais)
- [Módulo 2 – Scrapy, CSS, XPath e Técnicas Avançadas](#módulo-2--scrapy-css-xpath-e-técnicas-avançadas)
- [Módulo 3 – Scrapy: Desenvolvimento de Spider](#módulo-3--scrapy-desenvolvimento-de-spider)
- [Atividade Final – Pesquisa de Preços com Web Scraping](#atividade-final--pesquisa-de-preços-com-web-scraping)
- [Ferramentas e Tecnologias](#ferramentas-e-tecnologias)
- [Aprendizados e Boas Práticas](#aprendizados-e-boas-práticas)

---

## Sobre a Disciplina

A disciplina aborda técnicas modernas de **Web Crawling**, **Web Scraping** e **Coleta Automatizada de Dados**, com aplicações práticas em projetos reais. As atividades foram desenvolvidas individualmente e em grupo, com entregas ao longo dos módulos e uma atividade final integradora.

**Stack principal utilizada:**
- Python 3.13.1 · Jupyter Notebook · Windows (PowerShell)
- Scrapy · SeleniumBase · BeautifulSoup (bs4) · Requests
- Pandas · Matplotlib · smtplib (Gmail SMTP)

---

## Módulo 1 – Fundamentos de Web Scraping e Ferramentas Essenciais

### 📖 Conteúdo Teórico

#### Web Scraping vs. Web Crawling

| Conceito | Definição | Objetivo |
|---|---|---|
| **Web Scraping** | Coleta de dados específicos de páginas web, sem interação direta com uma API | Extração de dados estruturados |
| **Web Crawling** | Navegação automatizada pela web seguindo links de página em página | Descoberta e indexação de páginas |

#### Como Funciona uma Requisição Web

1. **Requisição Inicial** – o computador envia pacotes de dados com o endereço IP do servidor e a requisição HTTP
2. **Recebimento pelo Servidor** – o servidor direciona o pacote para a aplicação correta
3. **Processamento** – a aplicação lê a requisição (ex: `GET index.html`)
4. **Resposta** – o servidor localiza o arquivo e o envia de volta

**Métodos HTTP principais:**
- `GET` – busca informações (pesquisa no Google, abrir uma página)
- `POST` – envia informações sensíveis ou grandes (login, formulários)

> O navegador é apenas uma ferramenta que constrói e interpreta esses pacotes de informação.

---

#### HTML – Linguagem de Marcação

HTML (HyperText Markup Language) é a linguagem padrão para estruturar páginas web. Define conteúdo e hierarquia dos elementos renderizados pelo navegador. **Não é uma linguagem de programação**, mas de marcação.

**Estrutura base de um documento HTML5:**

```html
<!DOCTYPE html>  <!-- Informa o padrão HTML5 ao navegador -->
<html>           <!-- Elemento raiz -->
  <head>         <!-- Metadados (título, charset, links, scripts) -->
  </head>
  <body>         <!-- Conteúdo visível da página -->
  </body>
</html>
```

**Tags Estruturais (Semânticas):**

| Tag | Uso |
|---|---|
| `<header>` | Cabeçalho da página ou seção (logo, menu, título) |
| `<nav>` | Agrupa links de navegação |
| `<main>` | Conteúdo principal do documento |
| `<section>` | Seção temática dentro do conteúdo |
| `<article>` | Conteúdo independente (post, notícia) |
| `<aside>` | Conteúdo lateral, complementar (barra lateral) |
| `<footer>` | Rodapé da página (contato, direitos autorais) |
| `<div>` | Bloco genérico para agrupamento sem função semântica |

**Tags de Texto e Títulos:**

| Tag | Uso |
|---|---|
| `<h1>` ... `<h6>` | Cabeçalhos hierárquicos (`<h1>` é o mais importante) |
| `<p>` | Parágrafo de texto |
| `<br>` | Quebra de linha |
| `<span>` | Container inline para destacar partes do texto |
| `<strong>` | Texto em negrito com ênfase semântica |
| `<em>` | Texto em itálico, enfatizado |
| `<blockquote>` | Citação em bloco |
| `<code>` | Trecho de código |

**Tags de Links e Mídias** (importantes para scraping, pois extraímos URLs):

```html
<a href="https://www.unifor.br">UNIFOR</a>
<img src="logo.png" alt="Logo da Unifor">
<video src="aula.mp4" controls></video>
<audio src="som.mp3" controls></audio>
```

**Tags de Listas** (frequentes em menus e listagens de produtos):

```html
<ul><li>Item não ordenado</li></ul>
<ol><li>Passo 1</li></ol>
```

**Tags de Tabelas** (fontes ricas de dados estruturados):

```html
<table>
  <tr><th>Nome</th><th>Preço</th></tr>
  <tr><td>Produto A</td><td>R$ 10,00</td></tr>
</table>
```

**Tags de Formulários** (úteis em automação com Selenium para login e busca):

```html
<form action="/enviar">
  <input type="text" name="usuario">
  <label for="usuario">Usuário</label>
  <select><option>A</option></select>
  <textarea>Mensagem</textarea>
  <button type="submit">Enviar</button>
</form>
```

---

#### HTML – Atributos

Atributos são informações adicionais dentro das tags, definindo propriedades ou comportamentos:

```html
<tag atributo="valor">conteúdo</tag>
```

**Atributos Globais (essenciais para scraping):**

| Atributo | Descrição | Exemplo |
|---|---|---|
| `id` | Identificador único na página | `<p id="introducao">` |
| `class` | Classes CSS (pode se repetir) | `<div class="conteudo destaque">` |
| `style` | Estilos CSS inline | `<p style="color:blue;">` |
| `title` | Texto ao passar o mouse | `<abbr title="HyperText Markup Language">HTML</abbr>` |
| `hidden` | Oculta o elemento | `<div hidden>` |
| `lang` | Idioma do conteúdo | `<html lang="pt-BR">` |
| `data-*` | Atributos personalizados para dados | `<div data-preco="29.90">` |

---

#### HTML – DOM (Document Object Model)

O DOM é a forma como o navegador (ou o BeautifulSoup) "enxerga" o HTML — uma **estrutura hierárquica de nós**. Cada tag é um nó da árvore, permitindo percorrer e selecionar elementos.

```
html
├── head
│   └── title
└── body
    ├── h1
    └── p
```

---

#### BeautifulSoup

A biblioteca BeautifulSoup é uma das mais usadas em Python para web scraping. Permite navegar, buscar e modificar a estrutura de documentos HTML ou XML de forma simples.

**Fluxo básico:**
1. Faz-se uma requisição HTTP
2. Passa-se o HTML obtido para o BeautifulSoup
3. Utiliza-se um **Parser** (interpretador) que transforma o HTML em uma árvore estruturada

```python
import requests
from bs4 import BeautifulSoup

response = requests.get("https://exemplo.com")
soup = BeautifulSoup(response.text, "html.parser")

# Extraindo dados
titulo = soup.find("h1").text
links = [a["href"] for a in soup.find_all("a", href=True)]
```

---

### 🛠️ Atividade Prática – Módulo 1

- **Contribuição individual:** extração do conteúdo principal de artigos (scraping do corpo do texto)
- **Biblioteca utilizada:** BeautifulSoup + Requests
- **Abordagem:** requisição HTTP estática com parsing HTML

---

## Módulo 2 – Scrapy, CSS, XPath e Técnicas Avançadas

### 📖 Conteúdo Teórico

#### Introdução ao Scrapy

O Scrapy é um **framework completo para raspagem e coleta automatizada de dados da web**. Ele fornece pronto:

- Controle de requisições HTTP
- Parsing de HTML
- Controle de filas de páginas
- Exportação dos dados (CSV, JSON, XML, banco de dados)
- Mecanismos de respeito a regras dos sites (robots.txt, delays)

> O Scrapy é baseado em **Spiders (aranhas)** — scripts que definem onde começar (URL inicial), como encontrar os dados e como seguir links para outras páginas.

**Vantagens:**
- Muito rápido e eficiente para grandes volumes
- Permite seguir links automaticamente
- Exportação nativa (CSV, JSON, XML)
- Integração fácil com APIs REST e bancos de dados
- Requisições assíncronas (paralelismo embutido)

**Limitação principal:**
- ⚠️ Não executa JavaScript (para isso, usar Selenium ou Playwright)

---

#### Componentes do Scrapy

| Componente | Função |
|---|---|
| **Spider** | Cérebro do scraping: define `start_urls`, como extrair dados (`parse()`) e como seguir links (`response.follow()`) |
| **Request** | Pedido de página (tem URL, callback e metadados) |
| **Response** | Resultado do Request com o HTML e métodos de seleção (`response.css()` / `response.xpath()`) |
| **Item Pipeline** | Processa e salva os dados extraídos (limpar texto, remover duplicatas, salvar em CSV/JSON/BD) |
| **Middlewares** | Camadas intermediárias para User-Agent personalizado, proxies, headers e bloqueios |

#### Fluxo de Funcionamento

```
Engine → Scheduler → Downloader → Engine → Spider → Engine → Item Pipeline
                  ↑___________________________|
                         (novas requisições)
```

1. **Engine** inicia e envia a primeira requisição ao Scheduler
2. **Scheduler** guarda requisições em fila
3. **Downloader** baixa a página e retorna a Response
4. **Spider** analisa o HTML, extrai dados e gera novas requisições
5. **Item Pipeline** processa e salva os dados

---

#### Instalação e Criação de Projeto

```bash
pip install scrapy

# Criar novo projeto
scrapy startproject nome_projeto

# Criar spider
scrapy genspider nome_spider dominio.com

# Executar spider
scrapy crawl nome_spider

# Exportar dados
scrapy crawl books -o books.csv    # CSV
scrapy crawl books -o books.json   # JSON
scrapy crawl books -o books.xml    # XML
```

---

#### Arquivos do Projeto Scrapy

**`spider.py`** – onde o scraper é construído; o método `parse` usa `yield` para retornar dados:

```python
import scrapy

class MeuSpider(scrapy.Spider):
    name = "meu_spider"
    start_urls = ["https://exemplo.com"]

    def parse(self, response):
        for item in response.css("div.produto"):
            yield {
                "nome": item.css("h3::text").get(),
                "preco": item.css(".preco::text").get(),
            }
        # Paginação
        proxima = response.css("li.next a::attr(href)").get()
        if proxima:
            yield response.follow(proxima, self.parse)
```

**`items.py`** – armazena os dados temporariamente de forma estruturada:

```python
import scrapy

class ProdutoItem(scrapy.Item):
    nome = scrapy.Field()
    preco = scrapy.Field()
    url = scrapy.Field()
```

**`pipelines.py`** – tratamento dos dados (ativar no `settings.py` descomentando `ITEM_PIPELINES`):

```python
class MeuPipeline:
    def process_item(self, item, spider):
        # limpeza, validação, salvamento
        return item
```

> O número de prioridade no pipeline (ex: `300`) define a ordem de execução — menor número = executa primeiro.

---

#### CSS vs XPath – Seletores

| Seleção | CSS | XPath |
|---|---|---|
| Por Tag | `div` | `//div` |
| Por Classe | `.item` | `//*[@class="item"]` |
| Por ID | `#header` | `//*[@id="header"]` |
| Por Atributo | `a[href="exemplo.com"]` | `//*[@href="exemplo.com"]` |
| Contém | `a[href*="login"]` | `//*[contains(@class, "item")]` |
| Começa com | `a[href^="/produto"]` | `//a[starts-with(@href, "/produto")]` |
| Termina com | `img[src$=".jpg"]` | `//a[ends-with(@href, "/produto")]` |
| Descendentes | `div a` | `//div//a` |
| Filho direto | `ul > li` | `/div/a` |
| Primeiro elemento | `li:first-child` | `(//tr)[1]` |
| N-ésimo elemento | `tr:nth-child(2)` | `(//tr)[2]` |

**Capacidades exclusivas do XPath:**

```xpath
# 1. Subir no DOM (pegar o elemento pai)
//span[@class="preco"]/..

# 2. Selecionar elemento pelo texto
//p[contains(text(), "Promoção")]

# 3. Condições numéricas
//li[@data-qtd > 6]

# 4. Following-sibling com condições complexas
//h2[contains(text(),"Notebook")]/following-sibling::p[1]

# 5. N-ésimo elemento com filtros avançados
//div[@class="item"][3]

# 6. Condições lógicas (AND/OR)
//a[@class="btn" and @data-role="primary"]
//a[@data-role="primary" or @data-role="secondary"]

# 7. Posição relativa
//h3[text()="TV"]/following::span[@class="new-price"][1]

# 8. Elementos sem determinado atributo
//img[not(@alt)]

# 9. Elementos baseados no texto do filho
//div[span[contains(text(),"OK")]]
```

**Combinando CSS e XPath no Scrapy:**

```python
response.css("//li.next a").xpath("@href").get()
```

---

#### Testando Seletores no Scrapy Shell

```bash
scrapy shell "https://exemplo.com"

# Testar CSS
response.css("h3 a::attr(title)").getall()[:5]

# Testar XPath
response.xpath('//li[@class="next"]/a/@href').get()
```

---

#### Passando por Restrições

**User-Agent** – string enviada ao servidor identificando o cliente:

```python
# settings.py
USER_AGENT = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36"
```

Motivos para customizar:
1. Evitar bloqueios (sites identificam bots pelo User-Agent)
2. Simular um navegador real
3. Imitar dispositivos (Mobile, Desktop, Tablet)

**Proxy** – intermediário que esconde seu IP:

```python
# settings.py
DOWNLOADER_MIDDLEWARES = {
    "scrapy_proxy_pool.middlewares.ProxyPoolMiddleware": 610,
}
```

Usos: evitar bloqueios por IP, aumentar anonimato, acessar conteúdo restrito por região.

Referência de User-Agents: https://explore.whatismybrowser.com/useragents/explore/software_name/googlebot/  
Pool de proxies: https://github.com/rejoiceinhope/scrapy-proxy-pool

---

### 🛠️ Atividade Prática – Módulo 2

- **Contribuição individual:** extração e parsing de URLs usando **expressões regulares (regex)**
- **Técnica aplicada:** identificação de padrões de data e categoria nas URLs do portal G1
- **Observação técnica:** o G1 depende fortemente de JavaScript para renderização, tornando a extração baseada em URL+regex a abordagem mais prática com BeautifulSoup

```python
import re

# Exemplo: extraindo data e categoria de URLs do G1
url = "https://g1.globo.com/tecnologia/noticia/2024/03/15/artigo.ghtml"
padrao = r"g1\.globo\.com/([^/]+)/noticia/(\d{4}/\d{2}/\d{2})/"
match = re.search(padrao, url)
if match:
    categoria = match.group(1)   # "tecnologia"
    data = match.group(2)        # "2024/03/15"
```

---

## Módulo 3 – Scrapy: Desenvolvimento de Spider

### 🛠️ Atividade Prática – Módulo 3

- **Contribuição individual:** desenvolvimento completo de um **Scrapy Spider**
- **Projeto:** spider para coleta de dados do site O Boticário
- **Estrutura desenvolvida:** spider com paginação, pipeline de limpeza e exportação em CSV

```
boticario_spider/
├── boticario_spider/
│   ├── spiders/
│   │   └── boticario.py      # Spider principal
│   ├── items.py               # Definição dos campos
│   ├── pipelines.py           # Tratamento dos dados
│   └── settings.py            # Configurações (User-Agent, pipelines)
├── scrapy.cfg
└── produtos.csv               # Output gerado
```

**Campos coletados:** nome do produto, preço, categoria, URL da página

---

## Atividade Final – Pesquisa de Preços com Web Scraping

### 🎯 Objetivo

Simular uma pesquisa de preços completa para a **compra de 25 materiais de reforma doméstica**, utilizando web scraping automatizado para comparar preços em marketplaces e gerar análise em PDF.

### 📋 Tarefas Realizadas

| # | Tarefa | Status |
|---|---|---|
| 1 | Definição e recebimento da lista com 25 itens | ✅ |
| 2 | Pesquisa automatizada dos itens nos marketplaces | ✅ |
| 3 | Coleta de nome, preço, quantidade, URL e página de cada produto | ✅ |
| 4 | Estruturação dos dados em Pandas DataFrame | ✅ |
| 5 | Análise descritiva (preço médio, menor/maior preço, variação %, economia total) | ✅ |
| 6 | Geração de PDF com gráficos, tabelas e conclusões | ✅ |
| 7 | Envio automático do PDF por e-mail via Gmail SMTP | ✅ |
| 8 | Geração de logs com data/hora, itens pesquisados, erros e status | ✅ |
| 9 | Gravação de vídeo da automação em execução | 🎥 |
| 10 | Empacotamento em arquivo ZIP para entrega | 📦 |

### 🛠️ Tecnologias Aplicadas

**Automação do navegador:**
```python
from seleniumbase import Driver

driver = Driver(uc=True)  # Necessário para Python 3.13.1
                          # (incompatibilidade com undetected-chromedriver)
```

**Coleta e estruturação:**
```python
import pandas as pd

df = pd.DataFrame(dados, columns=["item", "site", "nome", "preco", "quantidade", "link"])
```

**Análise descritiva:**
```python
# Menor preço por item entre os marketplaces
menor_preco = df.groupby("item")["preco"].min()

# Site mais vantajoso para toda a compra
site_economico = df.loc[df.groupby("item")["preco"].idxmin()]["site"].value_counts().idxmax()

# Variação percentual entre menor e maior preço
df["variacao_%"] = ((df["preco_max"] - df["preco_min"]) / df["preco_min"]) * 100

# Economia total escolhendo sempre o menor preço
economia_total = df["preco_max"].sum() - df["preco_min"].sum()
```

**Envio de e-mail:**
```python
import smtplib
from email.mime.multipart import MIMEMultipart
from email.mime.base import MIMEBase
from email import encoders

# Autenticação via Google App Password
# (senha comum do Gmail não funciona para SMTP programático)
# Gerar em: https://myaccount.google.com/apppasswords

with smtplib.SMTP_SSL("smtp.gmail.com", 465) as server:
    server.login(remetente, app_password)
    server.sendmail(remetente, destinatario, msg.as_string())
```

**Sistema de logs:**
```python
import logging
from datetime import datetime

logging.basicConfig(
    filename=f"scraping_{datetime.now().strftime('%Y%m%d_%H%M%S')}.log",
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s"
)
```

### 📦 Estrutura de Entrega

```
Atividade_Final_WebScraping.zip
├── Atividade_Final.ipynb         # Notebook principal com toda a automação
├── relatorio_reforma.pdf         # PDF com análise, gráficos e conclusões
├── dados_coletados.csv           # Dados brutos estruturados
├── scraping_YYYYMMDD_HHMMSS.log  # Log da execução
└── execucao_automacao.mp4        # Vídeo da automação em funcionamento
```

---

## Ferramentas e Tecnologias

| Categoria | Ferramenta | Uso |
|---|---|---|
| **Linguagem** | Python 3.13.1 | Toda a codificação |
| **Ambiente** | Jupyter Notebook | Desenvolvimento interativo |
| **SO** | Windows (PowerShell) | Execução local |
| **Scraping estático** | Requests + BeautifulSoup | Módulo 1 e 2 |
| **Scraping dinâmico** | SeleniumBase (`uc=True`) | Atividade Final (Amazon) |
| **Framework de scraping** | Scrapy | Módulos 2 e 3 |
| **Dados** | Pandas | Estruturação e análise |
| **Visualização** | Matplotlib | Gráficos do relatório |
| **PDF** | Matplotlib PdfPages / ReportLab | Geração do relatório |
| **E-mail** | smtplib + Gmail SMTP SSL | Envio automatizado |
| **Versionamento** | GitHub | Portfólio de entregas |

---

## Aprendizados e Boas Práticas

### ⚠️ Compatibilidade Python

> `undetected-chromedriver` **não é compatível** com Python 3.13.1.  
> **Solução:** usar `SeleniumBase` com `Driver(uc=True)`.

### 🔐 Autenticação Gmail SMTP

> Senhas comuns do Gmail **não funcionam** para envio programático.  
> **Solução:** gerar um **Google App Password** em [myaccount.google.com/apppasswords](https://myaccount.google.com/apppasswords).

### 📁 Disciplina com Diretórios (Scrapy)

> Executar comandos Scrapy do diretório errado causa falhas silenciosas.  
> **Sempre verificar** se o diretório de trabalho é a raiz do projeto (`scrapy.cfg` deve estar visível).

### 📄 Caminhos Absolutos em CSV

> Quando projetos Scrapy são criados diretamente na Área de Trabalho (e não dentro de uma pasta nomeada), os caminhos absolutos nos notebooks devem refletir a estrutura real de pastas.

### 🌐 Sites com JavaScript (G1)

> Sites que dependem de JavaScript para renderização (como o G1) não são bem acessíveis com BeautifulSoup puro.  
> **Abordagem adotada:** extração via regex aplicada diretamente nas URLs para campos como data e categoria.

### 📂 Arquivos com Extensão Enganosa

> Materiais do curso estavam em formato `.pptx` renomeados para `.pdf`.  
> **Solução:** verificar o tipo real com `zipfile` e processar com `python-pptx`.

---

*Disciplina: Web Crawling, Scraping e Coleta Automatizada de Dados | UNIFOR Pós-Graduação em Data Science*
