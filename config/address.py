#!/usr/bin/env python3
"""
config/address.py — export the Addresses table to per-network text files.

Reads every row from the `Addresses` table in Supabase and writes one file
per supported network into `<repo_root>/address/`:

    address/eth.txt
    address/bsc.txt
    address/sol.txt
    address/ton.txt
    address/robinhood.txt

Each file contains one line per exchange that has a non-empty address for
that network, formatted as:

    Bybit = 0x3f634...
    OKX   = 0x3f634...

Usage:
    python config/address.py
"""

import logging
import os

HERE   = os.path.dirname(os.path.abspath(__file__))
PARENT = os.path.dirname(HERE)

OUTPUT_DIR = os.path.join(PARENT, "address")

NETWORKS = ["ETH", "BSC", "SOL", "TON", "ROBINHOOD", "APTOS", "PLASMA"]

logging.basicConfig(
    level=logging.INFO,
    format="%(message)s",
    handlers=[logging.StreamHandler()],
)
log = logging.getLogger()
logging.getLogger("httpx").setLevel(logging.WARNING)
logging.getLogger("httpcore").setLevel(logging.WARNING)


def _load_db_config() -> dict:
    candidates = [
        os.path.join(HERE, "db.txt"),
        os.path.join(PARENT, "db.txt"),
    ]
    for path in candidates:
        if os.path.exists(path):
            config = {}
            with open(path, "r") as f:
                for line in f:
                    line = line.strip()
                    if not line or line.startswith("#"):
                        continue
                    if "=" in line:
                        k, _, v = line.partition("=")
                        config[k.strip()] = v.strip().strip('"')
            return config
    raise FileNotFoundError(f"db.txt not found in any of: {candidates}")


def _get_supabase():
    from supabase import create_client
    cfg = _load_db_config()
    return create_client(cfg["SUPABASE_URL"], cfg["SUPABASE_KEY"])


def fetch_addresses() -> list:
    sb = _get_supabase()
    rows = sb.table("Addresses").select("*").execute()
    return rows.data or []


def _clean(value):
    if value is None:
        return None
    if isinstance(value, str):
        value = value.strip()
        if value == "":
            return None
    return str(value)


def write_files(rows: list) -> dict:
    os.makedirs(OUTPUT_DIR, exist_ok=True)

    counts = {}

    for net in NETWORKS:
        filename = f"{net.lower()}.py"
        path     = os.path.join(OUTPUT_DIR, filename)

        entries = []
        for row in rows:
            exchange = _clean(row.get("exchange"))
            if not exchange:
                continue
            value = _clean(row.get(net))
            if not value:
                continue
            entries.append((exchange, value))

        # Alphabetical by exchange name, case-insensitive — matches the
        # "Bybit = ..., OKX = ..." example order and stays deterministic.
        entries.sort(key=lambda e: e[0].lower())

        # Pad the exchange name so "Bybit" and "OKX" line up at the "=".
        width = max((len(e[0]) for e in entries), default=0)

        lines = [f"{ex.ljust(width)} = {val}" for ex, val in entries]

        with open(path, "w") as f:
            if lines:
                f.write("\n".join(lines) + "\n")

        counts[net] = len(lines)
        log.info(f"  {filename:<14} {len(lines)} address(es)  ->  {path}")

    return counts


def main():
    log.info("config/address.py — exporting Addresses table to text files")
    log.info(f"Output directory: {OUTPUT_DIR}")
    log.info("")

    try:
        rows = fetch_addresses()
    except Exception as e:
        log.error(f"Failed to read Addresses table: {str(e)[:300]}")
        return 1

    if not rows:
        log.warning("Addresses table is empty — nothing to export")
        return 0

    log.info(f"Loaded {len(rows)} row(s) from Addresses")
    log.info("")

    counts = write_files(rows)

    log.info("")
    log.info("=" * 50)
    log.info("SUMMARY")
    log.info("=" * 50)
    total = 0
    for net in NETWORKS:
        n = counts.get(net, 0)
        total += n
        log.info(f"  {net:<10} {n} address(es)")
    log.info(f"  {'TOTAL':<10} {total} address(es) written")

    return 0


if __name__ == "__main__":
    raise SystemExit(main())