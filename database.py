"""PostgreSQL database operations with Polars for fast bulk loading."""

import io
import logging
import time
from typing import List, Set
from urllib.parse import urlparse

import polars as pl
import psycopg2
from psycopg2 import sql

logger = logging.getLogger(__name__)

TABLES_WITH_CARGA_ID = frozenset(
    {
        "cnaes",
        "motivos",
        "municipios",
        "naturezas_juridicas",
        "paises",
        "qualificacoes_socios",
        "empresas",
        "estabelecimentos",
        "socios",
        "dados_simples",
    }
)


class Database:
    """PostgreSQL database handler with temp table upsert."""

    def __init__(self, database_url: str, schema: str):
        self.database_url = database_url
        self.schema = schema
        self._pk_cache: dict = {}
        self.conn = None

    def _parse_url(self) -> dict:
        """Parse DATABASE_URL into connection parameters."""
        parsed = urlparse(self.database_url)
        return {
            "host": parsed.hostname,
            "port": parsed.port or 5432,
            "database": parsed.path[1:],
            "user": parsed.username,
            "password": parsed.password,
        }

    def connect(self):
        """Establish database connection with retry."""
        if self.conn is not None:
            return

        params = self._parse_url()
        for attempt in range(4):
            try:
                self.conn = psycopg2.connect(**params)
                self.conn.autocommit = False
                with self.conn.cursor() as cur:
                    cur.execute(
                        sql.SQL("SET search_path TO {}, public").format(sql.Identifier(self.schema))
                    )
                return
            except psycopg2.OperationalError:
                if attempt == 3:
                    raise
                time.sleep(2**attempt)

    def disconnect(self):
        """Close database connection."""
        if self.conn:
            self.conn.close()
            self.conn = None

    def abandon_open_cargas(self, directory: str):
        """Encerra cargas abertas do diretório (ex.: antes de force)."""
        self.connect()
        with self.conn.cursor() as cur:
            cur.execute(
                """
                UPDATE cargas SET concluida_em = NOW()
                WHERE directory = %s AND concluida_em IS NULL
                """,
                (directory,),
            )
        self.conn.commit()

    def ensure_carga(self, directory: str) -> int:
        """Reutiliza carga aberta (retomada) ou cria nova para o diretório."""
        self.connect()
        with self.conn.cursor() as cur:
            cur.execute(
                """
                SELECT id FROM cargas
                WHERE directory = %s AND concluida_em IS NULL
                ORDER BY id DESC
                LIMIT 1
                """,
                (directory,),
            )
            row = cur.fetchone()
            if row:
                cid = row[0]
                self.conn.commit()
                return cid
            cur.execute(
                "INSERT INTO cargas (directory) VALUES (%s) RETURNING id",
                (directory,),
            )
            cid = cur.fetchone()[0]
        self.conn.commit()
        return cid

    def finalize_carga(self, carga_id: int):
        """Marca carga como concluída quando todos os arquivos do mês foram processados."""
        self.connect()
        with self.conn.cursor() as cur:
            cur.execute(
                """
                UPDATE cargas SET concluida_em = NOW()
                WHERE id = %s AND concluida_em IS NULL
                """,
                (carga_id,),
            )
        self.conn.commit()

    def get_processed_files(self, directory: str) -> Set[str]:
        """Get all processed filenames for a directory."""
        self.connect()
        try:
            with self.conn.cursor() as cur:
                cur.execute(
                    "SELECT filename FROM processed_files WHERE directory = %s",
                    (directory,),
                )
                return {row[0] for row in cur.fetchall()}
        except Exception as e:
            self.conn.rollback()
            logger.error(f"Error reading processed_files for {directory}: {e}")
            return set()

    def mark_processed(self, directory: str, filename: str, carga_id: int):
        """Mark a file as processed."""
        self.connect()
        with self.conn.cursor() as cur:
            cur.execute(
                """
                INSERT INTO processed_files (carga_id, directory, filename)
                VALUES (%s, %s, %s)
                ON CONFLICT (carga_id, filename) DO NOTHING
                """,
                (carga_id, directory, filename),
            )
            self.conn.commit()

    def clear_processed_files(self, directory: str):
        """Clear all processed file records for a directory (for force re-processing)."""
        self.connect()
        with self.conn.cursor() as cur:
            cur.execute(
                "DELETE FROM processed_files WHERE directory = %s",
                (directory,),
            )
            self.conn.commit()

    def bulk_upsert(self, df: pl.DataFrame, table_name: str, columns: List[str], carga_id: int):
        """Bulk upsert using temp table + COPY."""
        if df.is_empty():
            return

        if table_name in TABLES_WITH_CARGA_ID:
            df = df.with_columns(pl.lit(carga_id).alias("carga_id"))
            columns = list(columns) + ["carga_id"]

        self.connect()
        temp_table = f"temp_{table_name}_{id(df)}"

        try:
            with self.conn.cursor() as cur:
                # 1. Create temp table
                cur.execute(
                    f"CREATE TEMP TABLE {temp_table} "
                    f"(LIKE {table_name} INCLUDING DEFAULTS INCLUDING STORAGE) ON COMMIT DROP"
                )

                # 2. COPY to temp
                self._copy_to_temp(cur, df, temp_table, columns)

                # 3. Upsert from temp to main
                primary_keys = self._get_primary_keys(cur, table_name)
                self._upsert_from_temp(cur, temp_table, table_name, columns, primary_keys)

                self.conn.commit()

        except Exception as e:
            self.conn.rollback()
            logger.error(f"Error: {table_name}: {e}")
            raise

    def _copy_to_temp(self, cur, df: pl.DataFrame, temp_table: str, columns: List[str]):
        """COPY DataFrame to temp table using Polars CSV."""
        columns_str = ", ".join([f'"{col}"' for col in columns])
        csv_bytes = df.write_csv(include_header=False).encode("utf-8", errors="replace")
        csv_bytes = csv_bytes.replace(b"\x00", b"")

        cur.copy_expert(
            f"COPY {temp_table} ({columns_str}) FROM STDIN WITH CSV ENCODING 'UTF8'",
            io.BytesIO(csv_bytes),
        )

    def _get_primary_keys(self, cur, table_name: str) -> List[str]:
        """Get primary key columns for a table with caching."""
        cache_key = f"{self.schema}.{table_name}"
        if cache_key in self._pk_cache:
            return self._pk_cache[cache_key]

        cur.execute(
            """
            SELECT a.attname
            FROM pg_index i
            JOIN pg_attribute a ON a.attrelid = i.indrelid AND a.attnum = ANY(i.indkey)
            WHERE i.indrelid = %s::regclass AND i.indisprimary
            ORDER BY array_position(i.indkey, a.attnum)
            """,
            (table_name,),
        )

        primary_keys = [row[0] for row in cur.fetchall()]
        self._pk_cache[cache_key] = primary_keys
        return primary_keys

    def _upsert_from_temp(self, cur, temp_table: str, target_table: str, columns: List[str], primary_keys: List[str]):
        """Upsert from temp to target table."""
        columns_str = ", ".join([f'"{col}"' for col in columns])
        pk_str = ", ".join([f'"{pk}"' for pk in primary_keys])

        update_cols = [c for c in columns if c not in primary_keys]
        update_clause = ", ".join([f'"{c}" = EXCLUDED."{c}"' for c in update_cols])
        if update_clause:
            update_clause += ", data_atualizacao = CURRENT_TIMESTAMP"

        upsert_sql = f"""
            INSERT INTO {target_table} ({columns_str})
            SELECT DISTINCT ON ({pk_str}) {columns_str} FROM {temp_table} ORDER BY {pk_str}
            ON CONFLICT ({pk_str}) {"DO UPDATE SET " + update_clause if update_clause else "DO NOTHING"}
        """
        cur.execute(upsert_sql)
