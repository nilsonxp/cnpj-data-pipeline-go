# CNPJ Data Pipeline (v2)

[![Release](https://img.shields.io/github/v/release/caiopizzol/cnpj-data-pipeline)](https://github.com/caiopizzol/cnpj-data-pipeline/releases)
[![Python](https://img.shields.io/badge/python-3.11+-blue)](https://www.python.org)
[![Ruff](https://img.shields.io/endpoint?url=https://raw.githubusercontent.com/astral-sh/ruff/main/assets/badge/v2.json)](https://github.com/astral-sh/ruff)
[![codecov](https://codecov.io/gh/caiopizzol/cnpj-data-pipeline/graph/badge.svg)](https://codecov.io/gh/caiopizzol/cnpj-data-pipeline)

Baixa e processa dados de empresas brasileiras da Receita Federal para PostgreSQL.

> [!IMPORTANT]
> **Novo em v1.3.2** — _A Receita Federal migrou os arquivos CNPJ para um novo repositório Nextcloud. Esta versão já suporta a nova URL e realiza downloads via WebDAV automaticamente. Nenhuma configuração adicional necessária._

## Requisitos

### macOS

- [uv](https://docs.astral.sh/uv/) — `brew install uv` (ou `curl -LsSf https://astral.sh/uv/install.sh | sh`)
- [just](https://github.com/casey/just) — `brew install just`
- Docker Desktop (inclui Docker Compose), para `just up` com PostgreSQL local

### Linux

- [uv](https://docs.astral.sh/uv/) — `curl -LsSf https://astral.sh/uv/install.sh | sh`
- [just](https://github.com/casey/just) — `sudo apt install -y just` (Debian/Ubuntu)
- Docker Engine e **Docker Compose**, para `just up` com PostgreSQL local

## Início Rápido

```bash
cp .env.example .env
just up      # Iniciar PostgreSQL
just run     # Executar pipeline
```

## Comandos

```bash
just install # Instalar dependências
just up      # Iniciar PostgreSQL
just down    # Parar PostgreSQL
just db      # Entrar no banco (psql)
just run     # Executar pipeline
just reset   # Limpar e reiniciar banco
just lint    # Verificar código
just format  # Formatar código
just test    # Rodar testes
just check   # Rodar todos (lint, format, test)
```

## Uso

```bash
just run                          # Processar mês mais recente
just run --list                   # Listar meses disponíveis
just run --month 2024-11          # Processar mês específico
just run --month 2024-11 --force  # Forçar reprocessamento
```

## Configuração

Copie `.env.example` para `.env` e ajuste. Você pode usar `DATABASE_URL` **ou** `DB_HOST`, `DB_PORT`, `DB_NAME`, `DB_USER`, `DB_PASSWORD` e `DB_SCHEMA` (obrigatório para o schema das tabelas).

Exemplo mínimo com URL única (PostgreSQL local via Docker na porta 5435):

```bash
DATABASE_URL=postgres://postgres:postgres@localhost:5435/cnpj
DB_SCHEMA=cnpj
BATCH_SIZE=500000
TEMP_DIR=./temp
DOWNLOAD_WORKERS=4
RETRY_ATTEMPTS=3
RETRY_DELAY=5
CONNECT_TIMEOUT=30
READ_TIMEOUT=300
KEEP_DOWNLOADED_FILES=false
```

## Schema

> Documentação completa: [docs/data-schema.md](docs/data-schema.md)

Cada execução de carga cria (ou retoma) um registro em **`cnpj.cargas`** (`directory` = pasta `YYYY-MM`). As tabelas de domínio guardam **`carga_id`** apontando para esse lote; **`cnpj.processed_files`** liga cada ZIP processado à mesma carga.

```
EMPRESAS (1) ─── (N) ESTABELECIMENTOS
         ├─── (N) SOCIOS
         └─── (1) DADOS_SIMPLES
```

## Fonte de Dados

- **URL**: https://arquivos.receitafederal.gov.br/index.php/s/YggdBLfdninEJX9
- **Atualização**: Mensal
