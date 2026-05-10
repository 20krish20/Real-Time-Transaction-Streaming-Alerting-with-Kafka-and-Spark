"""
Snowflake Setup Script

Executes DDL and RBAC SQL files against Snowflake in the correct order:
  1. ddl.sql    — database, schema, warehouse, tables, views, resource monitor
  2. rbac.sql   — roles, grants, column masking policies

Usage:
    # Set credentials via env vars or .env file
    SNOWFLAKE__ACCOUNT=myaccount \\
    SNOWFLAKE__USER=svc_setup \\
    SNOWFLAKE__PASSWORD=secret \\
    python scripts/setup_snowflake.py

    # Dry-run: print statements without executing
    python scripts/setup_snowflake.py --dry-run

Options:
    --dry-run     Print SQL statements without executing
    --ddl-only    Execute only DDL (skip RBAC — useful for dev environments)
    --rbac-only   Execute only RBAC (useful when tables already exist)
"""

from __future__ import annotations

import argparse
import re
import sys
from pathlib import Path

from pipeline.config import settings

SNOWFLAKE_DIR = Path(__file__).parent.parent / "snowflake"


def _split_statements(sql: str) -> list[str]:
    """
    Split a SQL file into individual statements on semicolons,
    stripping comments and empty lines.
    """
    # Remove single-line comments
    sql = re.sub(r"--[^\n]*", "", sql)
    # Remove block comments
    sql = re.sub(r"/\*.*?\*/", "", sql, flags=re.DOTALL)
    # Split on semicolons
    stmts = [s.strip() for s in sql.split(";")]
    return [s for s in stmts if s and len(s) > 5]


def _get_connection():
    try:
        import snowflake.connector
    except ImportError:
        print("ERROR: snowflake-connector-python not installed.")
        print("       Run: pip install snowflake-connector-python")
        sys.exit(1)

    cfg = settings.snowflake
    conn_kwargs = {
        "account": cfg.account,
        "user": cfg.user,
        "warehouse": cfg.warehouse,
        "role": "SYSADMIN",   # DDL requires SYSADMIN; RBAC requires SECURITYADMIN
    }

    if cfg.password:
        conn_kwargs["password"] = cfg.password
    elif cfg.private_key_path:
        from cryptography.hazmat.primitives.serialization import (
            load_pem_private_key, Encoding, PrivateFormat, NoEncryption
        )
        from cryptography.hazmat.backends import default_backend

        with open(cfg.private_key_path, "rb") as f:
            pk = load_pem_private_key(
                f.read(),
                password=cfg.private_key_passphrase.encode() if cfg.private_key_passphrase else None,
                backend=default_backend(),
            )
        conn_kwargs["private_key"] = pk.private_bytes(
            Encoding.DER, PrivateFormat.PKCS8, NoEncryption()
        )
    else:
        print("ERROR: No authentication method configured (SNOWFLAKE__PASSWORD or SNOWFLAKE__PRIVATE_KEY_PATH)")
        sys.exit(1)

    return snowflake.connector.connect(**conn_kwargs)


def _execute_file(conn, sql_path: Path, dry_run: bool) -> None:
    print(f"\n{'[DRY-RUN] ' if dry_run else ''}Executing: {sql_path.name}")
    sql = sql_path.read_text()
    statements = _split_statements(sql)
    print(f"  Found {len(statements)} statements")

    if dry_run:
        for i, stmt in enumerate(statements, 1):
            preview = stmt[:80].replace("\n", " ")
            print(f"  [{i:02d}] {preview}...")
        return

    cursor = conn.cursor()
    succeeded = 0
    for i, stmt in enumerate(statements, 1):
        try:
            cursor.execute(stmt)
            succeeded += 1
            preview = stmt[:60].replace("\n", " ")
            print(f"  [{i:02d}] OK  {preview}...")
        except Exception as exc:
            err = str(exc)
            # Treat "already exists" as success (idempotent)
            if any(k in err.lower() for k in ("already exists", "duplicate")):
                print(f"  [{i:02d}] SKIP (already exists)")
                succeeded += 1
            else:
                print(f"  [{i:02d}] ERROR: {err}")
                print(f"       Statement: {stmt[:200]}")

    cursor.close()
    print(f"  Completed: {succeeded}/{len(statements)} statements succeeded")


def main() -> None:
    parser = argparse.ArgumentParser(description="Set up Snowflake for the reconciliation platform")
    parser.add_argument("--dry-run", action="store_true", help="Print SQL without executing")
    parser.add_argument("--ddl-only", action="store_true", help="Execute only DDL")
    parser.add_argument("--rbac-only", action="store_true", help="Execute only RBAC")
    args = parser.parse_args()

    print(f"Snowflake Setup — account: {settings.snowflake.account}")
    print(f"Mode: {'DRY-RUN' if args.dry_run else 'LIVE'}")

    conn = None if args.dry_run else _get_connection()

    files_to_run = []
    if not args.rbac_only:
        files_to_run.append(SNOWFLAKE_DIR / "ddl.sql")
    if not args.ddl_only:
        files_to_run.append(SNOWFLAKE_DIR / "rbac.sql")

    try:
        for sql_file in files_to_run:
            if not sql_file.exists():
                print(f"ERROR: SQL file not found: {sql_file}")
                sys.exit(1)
            _execute_file(conn, sql_file, args.dry_run)
    finally:
        if conn:
            conn.close()

    print("\nSnowflake setup complete.")


if __name__ == "__main__":
    main()
