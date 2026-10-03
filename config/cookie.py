"""
Opens https://arb-bot.infinityfree.io/?i=1 with Playwright, waits for
InfinityFree's bot-check to pass, extracts the resulting "__test" cookie,
and stores its value in Supabase (api_keys.cookie) across every exchange
row — same table/column tracker.py reads from. The cookie itself is a
per-domain bot-check pass (not per-exchange), so one fetch is stored under
every row; only Bybit and KuCoin currently route through the proxy and
actually use it, but the rest are kept in sync in case they're added later.

Usage:
    pip install playwright
    playwright install chromium
    python fetch_cookie.py
"""

import os
import sys
from playwright.sync_api import sync_playwright

URL = "https://arb-bot.infinityfree.io/?i=1"
USER_AGENT = (
    "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 "
    "(KHTML, like Gecko) Chrome/149.0.0.0 Safari/537.36"
)
COOKIE_NAME = "__test"
EXCHANGE_NAMES = [
    "binance", "bybit", "bitget", "mexc", "bingx",
    "kucoin", "coinex", "okx", "weex", "bitmart", "lbank"
]  # matches tracker.py's lowercase exchange_name keys


# ---- Supabase helpers, mirrored from tracker.py -----------------------

def _load_db_config() -> dict:
    here = os.path.dirname(os.path.abspath(__file__))
    candidates = [
        os.path.join(here, "db.txt"),
        os.path.join(os.path.dirname(here), "db.txt"),
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
    config = _load_db_config()
    return create_client(config["SUPABASE_URL"], config["SUPABASE_KEY"])


def store_cookie(exchange_name: str, cookie_value: str):
    sb = _get_supabase()

    # api_key has a NOT NULL constraint, so we can't blind-insert a fresh
    # row here — match the existing row case-insensitively and update it.
    existing = sb.table("api_keys").select("id, exchange_name").execute()
    match = next(
        (
            row for row in (existing.data or [])
            if (row.get("exchange_name") or "").strip().lower() == exchange_name.lower()
        ),
        None,
    )

    if not match:
        raise RuntimeError(
            f"No row found in api_keys where exchange_name matches "
            f"'{exchange_name}' (case-insensitive). Create the row first "
            f"(with its api_key/secret) before running this script."
        )

    sb.table("api_keys").update({"cookie": cookie_value}).eq(
        "id", match["id"]
    ).execute()
    print(f"Updated '{match['exchange_name']}' row (id={match['id']}) with new cookie.")


# ---- Playwright fetch ---------------------------------------------------

def fetch_test_cookie() -> str:
    with sync_playwright() as p:
        browser = p.chromium.launch(headless=True)
        context = browser.new_context(user_agent=USER_AGENT)
        page = context.new_page()

        page.goto(URL, wait_until="networkidle")
        # InfinityFree's bot-check briefly redirects/refreshes before
        # setting the real __test cookie — give it a moment.
        page.wait_for_timeout(3000)

        cookies = context.cookies()
        browser.close()

    for c in cookies:
        if c["name"] == COOKIE_NAME:
            return c["value"]

    raise RuntimeError(
        f"'{COOKIE_NAME}' cookie not found — cookies seen: "
        f"{[c['name'] for c in cookies]}"
    )


def main():
    print(f"Fetching {URL} ...")
    value = fetch_test_cookie()
    print(f"Got {COOKIE_NAME} = {value}")

    failures = []
    for exchange_name in EXCHANGE_NAMES:
        try:
            store_cookie(exchange_name, value)
        except Exception as e:
            print(f"ERROR storing cookie for '{exchange_name}': {e}", file=sys.stderr)
            failures.append(exchange_name)

    if failures:
        raise RuntimeError(f"Failed to store cookie for: {', '.join(failures)}")
    print("Done.")


if __name__ == "__main__":
    try:
        main()
    except Exception as e:
        print(f"ERROR: {e}", file=sys.stderr)
        sys.exit(1)
