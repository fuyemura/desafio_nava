# Desafio Nava — Pipelines de Dados com Dagster

Projeto profissional de engenharia de dados utilizando [Dagster](https://dagster.io/) como orquestrador de pipelines, com arquitetura Medallion (Bronze → Silver → Gold).

## Estrutura do Projeto

```
desafio_nava/
├── desafio_nava/           # Pacote principal Dagster
│   ├── assets/
│   │   ├── bronze/         # Camada de ingestão bruta
│   │   ├── silver/         # Camada de limpeza e transformação
│   │   └── gold/           # Camada analítica / negócio
│   ├── jobs/               # Definições de jobs
│   ├── schedules/          # Agendamentos
│   ├── sensors/            # Sensores reativos
│   ├── resources/          # Conexões e clientes externos
│   ├── io_managers/        # Gerenciadores de I/O
│   └── partitions/         # Definições de partições
├── tests/                  # Testes automatizados
├── notebooks/              # Exploração e análise
├── scripts/                # Scripts utilitários
├── docker/                 # Configurações Docker
├── dagster_home/           # Configuração da instância Dagster
└── .github/workflows/      # CI/CD
```

## Pré-requisitos

- Python 3.10+
- pip or uv

## Instalação

```bash
# Criando e ativando o ambiente virtual
python -m venv .venv
.venv\Scripts\activate        # Windows
# source .venv/bin/activate   # Linux/macOS

# Instalando as dependências
pip install -e ".[dev]"
```

## Executando o Dagster

```bash
# Variáveis de ambiente
cp .env.example .env

# Subindo a UI do Dagster (Dagit)
dagster dev
```

Acesse http://localhost:3000 no navegador.

## Executando com Docker Compose

```bash
docker compose -f docker/docker-compose.yml up --build
```

## Testes

```bash
pytest tests/ -v
```

## Variáveis de Ambiente

Veja `.env.example` para todas as variáveis necessárias.

## Arquitetura Medallion

| Camada | Descrição |
|--------|-----------|
| **Bronze** | Dados brutos ingeridos das fontes originais sem transformação |
| **Silver** | Dados limpos, validados e com schema padronizado |
| **Gold** | Dados agregados e modelados para consumo analítico |
