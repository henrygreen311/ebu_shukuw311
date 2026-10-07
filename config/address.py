#!/usr/bin/env python3
"""
config/address.py — fetch per-network deposit addresses via ccxt and
upsert them into the Supabase `Addresses` table.

Only ETH, BSC, SOL, TON, ROBINHOOD are supported.  The Addresses table
must match NETWORK_CANDIDATES exactly:

    create table public."Addresses" (
        exchange            text primary key,
        evm                 boolean not null default false,
        universal           boolean not null default false,
        whitelisted_network text[]  not null default '{}'::text[],
        "ETH"               text,
        "BSC"               text,
        "SOL"               text,
        "TON"               text,
        "ROBINHOOD"         text
    );

If the table has RLS enabled, either disable it or add a permissive policy:

    alter table public."Addresses" disable row level security;

Usage:
    python config/address.py                    # fetch every exchange
    python config/address.py --only Bybit       # only Bybit (repeatable)
    python config/address.py --dry-run          # preview, don't write
    python config/address.py --coin USDT        # default, change if needed
"""

import argparse
import json
import logging
import os
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from urllib.parse import urlparse

import ccxt
import requests

HERE = os.path.dirname(os.path.abspath(__file__))
PARENT = os.path.dirname(HERE)

logging.basicConfig(
    level=logging.INFO,
    format="%(message)s",
    handlers=[logging.StreamHandler()],
)
log = logging.getLogger()
logging.getLogger("httpx").setLevel(logging.WARNING)
logging.getLogger("httpcore").setLevel(logging.WARNING)

PROXY_BASE_TRADE = "https://arb-bot.infinityfree.io/trade_proxy.php"

PROXY_EXCHANGES = {"Bybit", "Bitget", "MEXC", "BingX", "OKX", "KuCoin", "LBank", "Gate"}
PROXY_EXCHANGE_IDS = {name.lower() for name in PROXY_EXCHANGES}

_PROXY_HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 "
        "(KHTML, like Gecko) Chrome/149.0.0.0 Safari/537.36"
    ),
    "Accept-Language": "en-US,en;q=0.9",
    "Referer": "https://arb-bot.infinityfree.io/",
}

_proxy_session = requests.Session()

TIMEOUT_MS = 15_000
PROXY_TIMEOUT_MS = 25_000
RETRY_ATTEMPTS = 2
RETRY_DELAY = 1
WORKERS = 4

EXCHANGE_BUILDERS = {
    "Bybit":  "bybit",
    "Bitget": "bitget",
    "MEXC":   "mexc",
    "BingX":  "bingx",
    "KuCoin": "kucoin",
    "OKX":    "okx",
    "LBank":  "lbank",
    "Gate":   "gate",
}

NETWORK_CANDIDATES = {
    "ETH":       ["ERC20", "ETH", "ETHEREUM"],
    "BSC":       ["BEP20", "BSC", "BNB", "BNBSMARTCHAIN"],
    "SOL":       ["SOL", "SOLANA"],
    "TON":       ["TON"],
    "APTOS":     ["APTOS", "APT"],
    "PLASMA":    ["PLASMA"],
    "ROBINHOOD": ["ROBINHOOD", "ROBINHOODCHAIN"],
}

EVM_COLUMNS = ["ETH", "BSC", "PLASMA"]
ADDRESS_COLUMNS = list(NETWORK_CANDIDATES.keys())


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


_SUPABASE_CLIENT = None


def _get_supabase():
    global _SUPABASE_CLIENT
    if _SUPABASE_CLIENT is None:
        from supabase import create_client
        cfg = _load_db_config()
        _SUPABASE_CLIENT = create_client(cfg["SUPABASE_URL"], cfg["SUPABASE_KEY"])
    return _SUPABASE_CLIENT


def _clean_secret(value):
    if value is None:
        return None
    if isinstance(value, str) and value.strip().upper() in ("", "NULL"):
        return None
    return value


def load_api_credentials() -> dict:
    try:
        sb = _get_supabase()
        rows = sb.table("api_keys").select("*").execute()
    except Exception as e:
        log.warning(f"  WARNING  Supabase credentials: {str(e)[:150]}")
        return {}
    creds = {}
    for row in rows.data or []:
        name = (row.get("exchange_name") or "").strip().lower()
        if not name:
            continue
        creds[name] = {
            "apiKey":   _clean_secret(row.get("api_key")),
            "secret":   _clean_secret(row.get("api_secret")),
            "password": _clean_secret(row.get("passphrase")),
            "cookie":   _clean_secret(row.get("cookie")),
        }
    return creds


CREDENTIALS = load_api_credentials()


def _route_through_proxy(ex):
    exchange_key = ex.id
    proxy_cookie = (CREDENTIALS.get(exchange_key) or {}).get("cookie")
    if not proxy_cookie:
        log.warning(
            f"  WARNING  no proxy cookie for '{exchange_key}' (api_keys.cookie) — "
            f"InfinityFree's bot-check may block requests"
        )
    proxy_cookies = {"__test": proxy_cookie} if proxy_cookie else {}

    def proxied_fetch(url, method="GET", headers=None, body=None):
        parsed = urlparse(url)
        request_headers = dict(headers or {})
        request_headers.update(_PROXY_HEADERS)
        request_headers["X-Proxy-Target-Host"] = parsed.netloc
        request_headers["X-Proxy-Exchange"] = exchange_key

        path = parsed.path or "/"
        new_url = f"{PROXY_BASE_TRADE}/{exchange_key}{path}"
        if parsed.query:
            new_url += f"?{parsed.query}"

        def do_request():
            resp = _proxy_session.request(
                method,
                new_url,
                headers=request_headers,
                cookies=proxy_cookies,
                data=body if method != "GET" else None,
                timeout=25,
            )
            if resp.status_code >= 400:
                raise Exception(
                    f"{exchange_key} {method} {new_url} -> "
                    f"{resp.status_code}: {resp.text[:600]}"
                )
            try:
                return json.loads(resp.text)
            except ValueError:
                return resp.text

        try:
            return do_request()
        except Exception:
            return do_request()

    ex.fetch = proxied_fetch
    return ex


def build_exchange(name: str):
    if name not in EXCHANGE_BUILDERS:
        raise ValueError(f"Unsupported exchange: {name}")

    ccxt_id = EXCHANGE_BUILDERS[name]
    proxied = name in PROXY_EXCHANGES
    creds = CREDENTIALS.get(ccxt_id, {})

    cfg = {
        "enableRateLimit": True,
        "timeout": PROXY_TIMEOUT_MS if proxied else TIMEOUT_MS,
        "options": {"adjustForTimeDifference": True},
    }
    if creds.get("apiKey"):
        cfg["apiKey"] = creds["apiKey"]
    if creds.get("secret"):
        cfg["secret"] = creds["secret"]
    if creds.get("password"):
        cfg["password"] = creds["password"]

    ex_class = getattr(ccxt, ccxt_id, None)
    if ex_class is None:
        raise ValueError(f"ccxt has no class for {ccxt_id}")
    ex = ex_class(cfg)

    if ccxt_id == "bybit":
        ex.has["fetchCurrencies"] = False

    if proxied:
        ex = _route_through_proxy(ex)
        if ccxt_id == "kucoin":
            try:
                ex.set_markets(ex.fetch_markets())
            except Exception as e:
                log.warning(f"  WARNING  KuCoin set_markets failed: {str(e)[:200]}")

    if not proxied:
        try:
            ex.load_markets()
        except Exception as e:
            log.warning(f"  WARNING  {name}: load_markets failed: {str(e)[:200]}")

    return ex


def _try_with_retries(fn, label):
    last_err = None
    for attempt in range(RETRY_ATTEMPTS + 1):
        try:
            return fn()
        except Exception as e:
            last_err = e
            if attempt < RETRY_ATTEMPTS:
                time.sleep(RETRY_DELAY)
    raise last_err


def fetch_deposit_addresses(exchange_name, coin="USDT"):
    ex = build_exchange(exchange_name)
    results = {}
    errors = {}

    available_networks = set()
    try:
        currencies = _try_with_retries(
            lambda: ex.fetch_currencies(), f"{exchange_name} fetch_currencies"
        ) or {}
        usdt = currencies.get(coin) or {}
        available_networks = set((usdt.get("networks") or {}).keys())
        if available_networks:
            log.info(
                f"[{exchange_name}] {coin} networks reported by exchange: "
                f"{sorted(available_networks)}"
            )
        else:
            log.info(
                f"[{exchange_name}] {coin} has no networks listed in fetch_currencies — "
                f"probing candidates"
            )
    except Exception as e:
        log.warning(f"[{exchange_name}] fetch_currencies failed: {str(e)[:600]}")

    for col, candidates in NETWORK_CANDIDATES.items():
        tried = []
        for net_code in candidates:
            if available_networks and net_code not in available_networks:
                continue
            tried.append(net_code)
            try:
                r = ex.fetch_deposit_address(coin, params={"network": net_code})
            except Exception as e:
                errors[col] = f"{net_code}: {str(e)[:400]}"
                continue
            if r and r.get("address"):
                results[col] = {
                    "address": r["address"],
                    "tag": r.get("tag"),
                    "net_used": net_code,
                }
                break

    return results, errors


def _auto_evm_flags(addresses: dict) -> dict:
    flags = {}
    eth = addresses.get("ETH", {}).get("address")
    if eth and eth.startswith("0x"):
        flags["evm"] = True

    evm_addrs = set()
    evm_cols_present = 0
    for col in EVM_COLUMNS:
        val = addresses.get(col, {}).get("address")
        if val and val.startswith("0x"):
            evm_addrs.add(val.lower())
            evm_cols_present += 1
    if flags.get("evm") and evm_cols_present >= 1 and len(evm_addrs) == 1:
        flags["universal"] = True
    return flags


def upsert_addresses(exchange_name, addresses, dry_run=False):
    if not addresses:
        log.info(f"[{exchange_name}] no addresses fetched — nothing to write")
        return False

    sb = _get_supabase()
    try:
        existing = (
            sb.table("Addresses").select("*").eq("exchange", exchange_name).execute()
        )
        current = existing.data[0] if existing.data else {}
    except Exception as e:
        log.warning(
            f"[{exchange_name}] could not read existing Addresses row: {str(e)[:200]}"
        )
        current = {}

    row = {"exchange": exchange_name}
    for col in ADDRESS_COLUMNS:
        row[col] = None
    row["evm"] = bool(current.get("evm", False))
    row["universal"] = bool(current.get("universal", False))
    row["whitelisted_network"] = current.get("whitelisted_network") or []

    for col, info in addresses.items():
        if col not in ADDRESS_COLUMNS:
            continue
        value = info["address"]
        if info.get("tag"):
            value = f"{value}|{info['tag']}"
        row[col] = value

    auto_flags = _auto_evm_flags(addresses)
    for k, v in auto_flags.items():
        row[k] = v

    filled = [c for c in ADDRESS_COLUMNS if row.get(c)]
    if dry_run:
        log.info(
            f"[{exchange_name}] DRY-RUN — would write {len(filled)} addresses: "
            f"{', '.join(filled) if filled else '(none)'}"
        )
        for c in filled:
            log.info(f"    {c:<10} {row[c]}")
        log.info(
            f"[{exchange_name}] DRY-RUN — auto flags: evm={row.get('evm')} "
            f"universal={row.get('universal')}"
        )
        return True

    try:
        sb.table("Addresses").upsert(row, on_conflict="exchange").execute()
        log.info(
            f"[{exchange_name}] ✅ saved {len(filled)} addresses: "
            f"{', '.join(filled) if filled else '(none)'}"
        )
        if row.get("evm"):
            log.info(f"[{exchange_name}] ℹ️  evm=True (auto-detected)")
        if row.get("universal"):
            log.info(
                f"[{exchange_name}] ℹ️  universal=True (EVM addresses identical)"
            )
        else:
            log.info(
                f"[{exchange_name}] ℹ️  universal not set — configure manually if "
                f"{exchange_name} treats one EVM whitelist entry as covering all chains"
            )
        return True
    except Exception as e:
        log.warning(f"[{exchange_name}] Addresses upsert failed: {str(e)[:300]}")
        return False


def process_exchange(exchange_name, coin, dry_run):
    try:
        addresses, errors = fetch_deposit_addresses(exchange_name, coin=coin)
    except Exception as e:
        log.warning(
            f"[{exchange_name}] failed entirely: {type(e).__name__}: {str(e)[:600]}"
        )
        return exchange_name, 0, 0

    if not addresses:
        log.info(f"[{exchange_name}] no deposit addresses returned")
        for col, err in errors.items():
            log.info(f"    {col:<10} {err}")
        log.info(
            f"[{exchange_name}] ⚠️  all fetch attempts failed — check that the API key "
            f"has 'read' permission and that InfinityFree's server IP is whitelisted "
            f"on {exchange_name} (requests leave from the proxy, not your machine)"
        )
        return exchange_name, 0, 0

    ok = upsert_addresses(exchange_name, addresses, dry_run=dry_run)

    missing = [c for c in ADDRESS_COLUMNS if c not in addresses]
    if missing:
        log.info(
            f"[{exchange_name}] missing networks: {', '.join(missing)} "
            f"(exchange may not support them)"
        )

    fetched = len(addresses)
    stored = fetched if (ok and not dry_run) else 0
    return exchange_name, fetched, stored


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--only",
        action="append",
        help="Only fetch this exchange (repeatable). Default: all configured.",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Print what would be written without touching the DB.",
    )
    parser.add_argument(
        "--coin",
        default="USDT",
        help="Deposit coin to fetch addresses for (default: USDT).",
    )
    args = parser.parse_args()

    targets = args.only or list(EXCHANGE_BUILDERS.keys())
    for t in targets:
        if t not in EXCHANGE_BUILDERS:
            log.error(f"Unknown exchange: {t}")
            return 1

    log.info(f"config/address.py — fetching {args.coin} deposit addresses")
    log.info(f"Exchanges : {', '.join(targets)}")
    log.info(f"Networks  : {', '.join(ADDRESS_COLUMNS)}")
    log.info(f"Proxy     : {PROXY_BASE_TRADE}")
    log.info(f"Mode      : {'DRY-RUN' if args.dry_run else 'WRITE TO SUPABASE'}")
    log.info("")

    fetched_summary = {}
    stored_summary = {}

    with ThreadPoolExecutor(max_workers=min(WORKERS, len(targets))) as pool:
        futures = {
            pool.submit(process_exchange, name, args.coin, args.dry_run): name
            for name in targets
        }
        for future in as_completed(futures):
            name = futures[future]
            try:
                _, fetched, stored = future.result()
            except Exception as e:
                log.error(f"[{name}] unhandled error: {e}")
                fetched = stored = 0
            fetched_summary[name] = fetched
            stored_summary[name] = stored

    log.info("")
    log.info("=" * 60)
    log.info("SUMMARY")
    log.info("=" * 60)
    log.info(f"  {'exchange':<10} {'fetched':>8} {'stored':>8}")
    log.info(f"  {'-'*10} {'-'*8} {'-'*8}")
    for name in targets:
        log.info(
            f"  {name:<10} {fetched_summary.get(name, 0):>8} "
            f"{stored_summary.get(name, 0):>8}"
        )

    if args.dry_run:
        log.info("")
        log.info("  (dry-run — no rows were written)")

    return 0


if __name__ == "__main__":
    raise SystemExit(main())