"""Configuration for CNPJ data pipeline."""

import os
from dataclasses import dataclass
from urllib.parse import quote_plus

from dotenv import load_dotenv

load_dotenv()


def _resolve_database_url() -> str:
    """
    DATABASE_URL tem precedência. Senão monta a URL só com DB_* do ambiente (sem host/banco fixos no código).
    DB_HOST e DB_NAME são obrigatórios nesse modo; DB_PORT omite → 5432 (padrão Postgres); DB_USER omite → postgres.
    """
    explicit = os.getenv("DATABASE_URL", "").strip()
    if explicit:
        return explicit

    host = os.getenv("DB_HOST", "").strip()
    name = os.getenv("DB_NAME", "").strip()
    if not host or not name:
        return ""

    port = os.getenv("DB_PORT", "").strip() or "5432"
    user = os.getenv("DB_USER", "").strip() or "postgres"
    password = os.getenv("DB_PASSWORD", "")

    user_q = quote_plus(user)
    if password:
        auth = f"{user_q}:{quote_plus(password)}"
    else:
        auth = user_q

    return f"postgres://{auth}@{host}:{port}/{name}"


@dataclass
class Config:
    """Pipeline configuration with sensible defaults."""

    database_url: str
    db_schema: str
    batch_size: int = 500000
    temp_dir: str = "./temp"
    download_workers: int = 4
    retry_attempts: int = 3
    retry_delay: int = 5
    connect_timeout: int = 30
    read_timeout: int = 300
    keep_files: bool = False
    base_url: str = "https://arquivos.receitafederal.gov.br/public.php/webdav"
    share_token: str = "YggdBLfdninEJX9"

    @classmethod
    def from_env(cls) -> "Config":
        """Create config from environment variables."""
        return cls(
            database_url=_resolve_database_url(),
            db_schema=os.getenv("DB_SCHEMA", "").strip(),
            batch_size=int(os.getenv("BATCH_SIZE", "500000")),
            temp_dir=os.getenv("TEMP_DIR", "./temp"),
            download_workers=int(os.getenv("DOWNLOAD_WORKERS", "4")),
            retry_attempts=int(os.getenv("RETRY_ATTEMPTS", "3")),
            retry_delay=int(os.getenv("RETRY_DELAY", "5")),
            connect_timeout=int(os.getenv("CONNECT_TIMEOUT", "30")),
            read_timeout=int(os.getenv("READ_TIMEOUT", "300")),
            keep_files=os.getenv("KEEP_DOWNLOADED_FILES", "false").lower() == "true",
        )


config = Config.from_env()
