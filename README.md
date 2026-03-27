# Desafio Nava — Pipelines de Dados com Dagster

Pipeline de engenharia de dados utilizando [Dagster](https://dagster.io/) como orquestrador, [Apache Spark](https://spark.apache.org/) + [Delta Lake](https://delta.io/) como camada de armazenamento e processamento, com arquitetura **Medallion (Bronze → Silver → Gold)**.

A fonte de dados é o arquivo PDA (Plano de Dados Abertos) da ANS — beneficiários de planos de saúde.

---

## Sumário

- [Arquitetura](#arquitetura)
- [Estrutura do Projeto](#estrutura-do-projeto)
- [Pré-requisitos](#pré-requisitos)
- [Instalação](#instalação)
- [Executando o Dagster](#executando-o-dagster)
- [Grafo de Assets](#grafo-de-assets)
- [Camadas de Dados](#camadas-de-dados)
- [Consultas Analíticas](#consultas-analíticas)

---

## Arquitetura

```
Fonte (CSV ANS/PDA)
        │
        ▼
┌───────────────┐
│    Bronze     │  Ingestão bruta — todos os campos STRING
│ raw_pda_bene  │  Particionado: SG_UF + ID_CMPT_MOVEL
└──────┬────────┘
       │
       ▼
┌───────────────┐
│    Silver     │  Limpeza, cast de tipos, trim de strings
│ stg_pda_bene  │  Particionado: SG_UF + ID_CMPT_MOVEL
└──────┬────────┘
       │
       ├──────────────────────────────────┐
       │                                  │
       ▼                                  ▼
┌─────────────────────────────────────────────────────────┐
│                        Gold                             │
│  dim_operadora   dim_municipio   dim_faixa_etaria        │
│                  fato_beneficiario                       │
└─────────────────────────────────────────────────────────┘
```

---

## Estrutura do Projeto

```
desafio_nava/
├── desafio_nava/                  # Pacote principal Dagster
│   ├── assets/
│   │   ├── bronze/
│   │   │   └── ing_pda_beneficiario.py   # Ingestão bruta dos CSVs
│   │   ├── silver/
│   │   │   └── stg_pda_beneficiario.py   # Limpeza e padronização
│   │   └── gold/
│   │       ├── dim_operadora.py          # Dimensão Operadora (SCD Tipo 1)
│   │       ├── dim_municipio.py          # Dimensão Município (SCD Tipo 1)
│   │       ├── dim_faixa_etaria.py       # Dimensão Faixa Etária (SCD Tipo 1)
│   │       └── fato_beneficiario.py      # Tabela Fato — métricas de beneficiários
│   ├── config/
│   │   ├── paths.py                      # Caminhos das camadas Delta
│   │   └── partitions.py
│   ├── resources/
│   │   └── spark_resource.py             # Recurso Dagster — SparkSession
│   ├── utils/
│   │   └── spark_config.py               # Inicialização do Spark + Delta Lake
│   └── definitions.py                    # Registro de assets e resources
├── data/
│   ├── raw/                              # CSVs fonte (ANS/PDA)
│   ├── bronze/                           # Delta Tables Bronze
│   ├── silver/                           # Delta Tables Silver
│   └── gold/                             # Delta Tables Gold
├── delta_lake/                           # Metastore Hive local + Spark Warehouse
├── notebook/
│   └── create-delta-tables.ipynb         # Criação manual das tabelas Delta
├── sql/
│   ├── duckdb_create_schema.sql          # Views DuckDB sobre Delta Tables
│   └── analises_beneficiarios.sql        # Consultas analíticas
├── pyproject.toml
└── README.md
```

---

## Pré-requisitos

| Ferramenta | Versão mínima | Função |
|---|---|---|
| Python | 3.11 | Runtime |
| pyenv | qualquer | Gerenciamento de versão Python |
| Poetry | 1.8+ | Gerenciamento de dependências |
| Java (JDK) | 11 ou 17 | Requisito do Apache Spark |
| Hadoop (winutils) | 3.x | Requisito do Spark no Windows |
| Apache Spark | 3.5.0 | Processamento distribuído |
| Delta Lake | 3.1.0 | Armazenamento ACID sobre Parquet |
| Dagster | 1.12+ | Orquestração de pipelines |

---

## Instalação

### 1. Instalar o pyenv (Windows)

```powershell
# Via pyenv-win
Invoke-WebRequest -UseBasicParsing -Uri "https://raw.githubusercontent.com/pyenv-win/pyenv-win/master/pyenv-win/install-pyenv-win.ps1" -OutFile "./install-pyenv-win.ps1"; &"./install-pyenv-win.ps1"

# Instalar e definir a versão do Python
pyenv install 3.11.9
pyenv local 3.11.9
```

### 2. Instalar o Poetry

```powershell
(Invoke-WebRequest -Uri https://install.python-poetry.org -UseBasicParsing).Content | python -

# Verificar instalação
poetry --version
```

### 3. Instalar Java (JDK 17)

O Spark requer Java 11 ou 17. Recomenda-se o [Eclipse Temurin](https://adoptium.net/).

```powershell
# Verificar se já está instalado
java -version

# Após instalar, configure a variável de ambiente
$env:JAVA_HOME = "C:\Program Files\Eclipse Adoptium\jdk-17..."
```

### 4. Configurar Hadoop no Windows (winutils)

O Spark no Windows requer os binários do Hadoop (`winutils.exe`).

```powershell
# Baixe winutils para Hadoop 3.x em:
# https://github.com/cdarlint/winutils

# Extraia em D:\hadoop (ou outro diretório de sua preferência)
# A variável HADOOP_HOME é configurada automaticamente pelo spark_config.py:
#   os.environ['HADOOP_HOME'] = r'D:\hadoop'
# Ajuste o caminho em desafio_nava/utils/spark_config.py se necessário.
```

### 5. Instalar dependências do projeto

```powershell
# Clone o repositório
git clone <url-repositorio>
cd desafio_nava

# Instalar dependências via Poetry
poetry install

# Ativar o ambiente virtual
poetry shell
# ou: .venv\Scripts\activate
```

O `pyproject.toml` declara automaticamente:
- `pyspark = 3.5.0`
- `delta-spark = 3.1.0`
- `dagster ^1.12`
- `dagster-webserver ^1.12`

### 6. Criar as tabelas Delta

Execute o notebook `notebook/create-delta-tables.ipynb` para criar os schemas e tabelas Delta nas camadas Bronze, Silver e Gold antes de executar o pipeline.

---

## Executando o Dagster

```powershell
# Na raiz do projeto, com o ambiente ativado
dagster dev
```

Acesse **http://localhost:3000** no navegador.

A configuração `[tool.dagster]` em `pyproject.toml` aponta automaticamente para `desafio_nava.definitions`, sem necessidade de variáveis de ambiente adicionais.

---

## Grafo de Assets

```
                    ┌─────────────────────────┐
                    │  raw_pda_beneficiario   │  (bronze)
                    │  Ingestão bruta CSV     │
                    └────────────┬────────────┘
                                 │
                                 ▼
                    ┌─────────────────────────┐
                    │  stg_pda_beneficiario   │  (silver)
                    │  Limpeza + cast tipos   │
                    └──┬──────────┬───────────┘
                       │          │
          ┌────────────┼──────────┼────────────┐
          ▼            ▼          ▼            ▼
  ┌──────────────┐ ┌──────────┐ ┌───────────────────┐
  │ dim_operadora│ │dim_muni  │ │  dim_faixa_etaria  │
  │  (gold)      │ │cipio     │ │      (gold)        │
  │  SCD Tipo 1  │ │(gold)    │ │    SCD Tipo 1      │
  └──────┬───────┘ └────┬─────┘ └────────┬──────────┘
         │              │                │
         └──────────────┴────────────────┘
                         │
                         ▼
            ┌────────────────────────┐
            │   fato_beneficiario    │  (gold)
            │  Idempotente por       │
            │  SG_UF + ID_CMPT_MOVEL │
            └────────────────────────┘
```

### Estratégia de idempotência

| Tabela | Estratégia | Chave |
|---|---|---|
| `bronze.raw_pda_beneficiario` | `overwrite` dinâmico por partição | `SG_UF + ID_CMPT_MOVEL` |
| `silver.stg_pda_beneficiario` | `overwrite` dinâmico por partição | `SG_UF + ID_CMPT_MOVEL` |
| `gold.dim_operadora` | MERGE SCD Tipo 1 | `CD_OPERADORA` |
| `gold.dim_municipio` | MERGE SCD Tipo 1 | `CD_MUNICIPIO` |
| `gold.dim_faixa_etaria` | MERGE SCD Tipo 1 | `DE_FAIXA_ETARIA` |
| `gold.fato_beneficiario` | `overwrite` dinâmico por partição | `SG_UF + ID_CMPT_MOVEL` |

---

## Camadas de Dados

### Bronze — `raw_pda_beneficiario`

- Todos os campos armazenados como `STRING` para preservar dados originais
- Particionada por `SG_UF` e `ID_CMPT_MOVEL`
- Adiciona coluna `CRIADO_EM` (timestamp de ingestão)

### Silver — `stg_pda_beneficiario`

- Cast de colunas numéricas para `INT` e `DT_CARGA` para `DATE`
- Trim em todos os campos string
- Mesma partição da Bronze

### Gold — Dimensões (SCD Tipo 1)

| Tabela | Grain | SK |
|---|---|---|
| `dim_operadora` | `CD_OPERADORA` | `xxhash64(CD_OPERADORA)` |
| `dim_municipio` | `CD_MUNICIPIO` | `xxhash64(CD_MUNICIPIO)` |
| `dim_faixa_etaria` | `DE_FAIXA_ETARIA` | `xxhash64(DE_FAIXA_ETARIA)` |

### Gold — Fato

- `fato_beneficiario`: métricas de beneficiários ativos, aderidos e cancelados por operadora, município e faixa etária
- FKs com fallback para `-1` quando não há correspondência na dimensão

---

## Consultas Analíticas

As consultas em `sql/analises_beneficiarios.sql` utilizam **DuckDB** com as views definidas em `sql/duckdb_create_schema.sql`, que mapeiam as Delta Tables via `delta_scan()`.

```sql
-- Carregar schemas e views
LOAD delta;
-- (executar duckdb_create_schema.sql)

-- Top 5 operadoras com mais beneficiários ativos
SELECT NM_RAZAO_SOCIAL, SUM(QT_BENEFICIARIO_ATIVO)
FROM gold.fato_beneficiario
GROUP BY NM_RAZAO_SOCIAL
ORDER BY 2 DESC LIMIT 5;
```

Consultas disponíveis:
- **a)** Top 5 operadoras por beneficiários ativos
- **b)** Faixa etária com maior número de beneficiários
- **c)** Ranking de municípios por quantidade de beneficiários
