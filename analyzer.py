import ccxt
import re
import time
import json
import logging
import threading
import requests
from datetime import datetime
from collections import defaultdict
from concurrent.futures import ThreadPoolExecutor, as_completed
from urllib.parse import urlparse

PROXY_BASE_SCAN = "https://arb-bot.infinityfree.io/proxy.php"
PROXY_BASE_TRADE = "https://arb-bot.infinityfree.io/trade_proxy.php"

PROXY_EXCHANGES_SCAN = {'Bybit', 'KuCoin'}
PROXY_EXCHANGES_TRADE = {
    'Bybit', 'Bitget', 'MEXC', 'BingX',
    'OKX', 'KuCoin',
}
PROXY_EXCHANGE_IDS_SCAN = {name.lower() for name in PROXY_EXCHANGES_SCAN}
PROXY_EXCHANGE_IDS_TRADE = {name.lower() for name in PROXY_EXCHANGES_TRADE}

EXCHANGE_HOSTNAME_OVERRIDES = {}

_PROXY_HEADERS = {
    "User-Agent": "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/149.0.0.0 Safari/537.36",
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8",
    "Accept-Language": "en-US,en;q=0.9",
    "Referer": "https://arb-bot.infinityfree.io/",
    "Upgrade-Insecure-Requests": "1",
    "Cache-Control": "max-age=0",
}

_proxy_session = requests.Session()

TIMEOUT_MS = 10_000
PROXY_TIMEOUT_MS = 20_000

RETRY_ATTEMPTS = 1
RETRY_DELAY = 1

CAPITAL_USDT     = 100
DEPTH_CHECK_USDT = 100
MIN_STORE_PROFIT_USDT = 0.1

FEE_CACHE_TTL_SEC = 60 * 60
FEE_RETRY_AFTER_FAIL_SEC = 60

MAX_TRANSFER_TIME_SEC = 15 * 60

NETWORK_BLOCK_TIME_SEC = {
    'ETH': 12, 'BSC': 3, 'TRX': 3, 'MATIC': 2, 'ARB': 0.3, 'OP': 2,
    'SOL': 0.4, 'AVAX': 2, 'BTC': 600, 'BASE': 2, 'TON': 5, 'APT': 0.25,
    'SUI': 3, 'XRP': 4, 'DOGE': 60, 'LTC': 150, 'ZKSYNC': 1, 'BTC-LN': 1,
    'ROBINHOOD': 0.5, 'PLASMA': 2, 'CELO': 5,
}
DEFAULT_BLOCK_TIME_SEC = 15

ORDER_BOOK_LIMIT = 50
ORDER_BOOK_LIMIT_OVERRIDES = {
    'KuCoin': 100,
}

def order_book_limit_for(exchange_name):
    return ORDER_BOOK_LIMIT_OVERRIDES.get(exchange_name, ORDER_BOOK_LIMIT)

DEPTH_FETCH_RETRIES     = 3
DEPTH_FETCH_RETRY_DELAY = 1

WORKER1_INTERVAL_SEC = 5 * 60
FAIL_STREAK_LIMIT     = 3
ROW_PROCESS_MAX_WORKERS = 20

logging.basicConfig(
    level=logging.INFO,
    format="%(message)s",
    handlers=[logging.FileHandler("trader.log"), logging.StreamHandler()]
)
log = logging.getLogger()
logging.getLogger("httpx").setLevel(logging.WARNING)
logging.getLogger("httpcore").setLevel(logging.WARNING)

def with_retries(fn, label):
    last_err = None
    for attempt in range(RETRY_ATTEMPTS + 1):
        try:
            return fn()
        except Exception as e:
            last_err = e
            if attempt < RETRY_ATTEMPTS:
                time.sleep(RETRY_DELAY)
    raise last_err

def _load_db_config() -> dict:
    import os
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

_SUPABASE_CLIENT = None

def _get_supabase_cached():
    global _SUPABASE_CLIENT
    if _SUPABASE_CLIENT is None:
        _SUPABASE_CLIENT = _get_supabase()
    return _SUPABASE_CLIENT

def _clean_secret(value):
    if value is None:
        return None
    if isinstance(value, str):
        value = value.strip()
        if value.upper() in ("", "NULL"):
            return None
    return value

def load_credentials(mode='scanner'):
    try:
        sb = _get_supabase_cached()
        rows = sb.table("api_keys").select("*").execute()
    except Exception as e:
        log.warning(f"  WARNING  Supabase credentials: {str(e)[:150]}")
        return {}
    creds = {}
    for row in rows.data or []:
        name = (row.get("exchange_name") or "").strip().lower()
        if not name:
            continue
        if mode == 'scanner':
            key_col = "api_key"
            secret_col = "api_secret"
        else:
            key_col = "api_key_restricted"
            secret_col = "api_secret_restricted"
        creds[name] = {
            'apiKey':   _clean_secret(row.get(key_col)),
            'secret':   _clean_secret(row.get(secret_col)),
            'password': _clean_secret(row.get("passphrase")),
            'uid':      _clean_secret(row.get("uid")),
            'cookie':   _clean_secret(row.get("cookie")),
        }
    return creds

CREDENTIALS_SCAN = load_credentials('scanner')
CREDENTIALS_TRADE = load_credentials('trader')

_exchange_mode = threading.local()

def set_exchange_mode(mode):
    _exchange_mode.mode = mode

def get_exchange_mode():
    return getattr(_exchange_mode, 'mode', 'scanner')

def route_through_proxy(ex, mode):
    exchange_key = ex.id
    if mode == 'scanner':
        proxy_base = PROXY_BASE_SCAN
        creds = CREDENTIALS_SCAN.get(exchange_key, {})
        proxy_cookie = creds.get('cookie')
    else:
        proxy_base = PROXY_BASE_TRADE
        creds = CREDENTIALS_TRADE.get(exchange_key, {})
        proxy_cookie = creds.get('cookie')

    if not proxy_cookie:
        log.warning(
            f"  WARNING  no proxy cookie found for '{exchange_key}' in mode {mode} "
            f"— InfinityFree's bot-check may block requests"
        )
    proxy_cookies = {"__test": proxy_cookie} if proxy_cookie else {}

    def proxied_fetch(url, method='GET', headers=None, body=None):
        parsed = urlparse(url)
        request_headers = dict(headers or {})
        request_headers.update(_PROXY_HEADERS)
        request_headers['X-Proxy-Target-Host'] = parsed.netloc
        request_headers['X-Proxy-Exchange'] = exchange_key
        new_url = f"{proxy_base}/{exchange_key}{parsed.path}"
        if parsed.query:
            new_url += f"?{parsed.query}"

        def do_request():
            resp = _proxy_session.request(
                method,
                new_url,
                headers=request_headers,
                cookies=proxy_cookies,
                data=body if method != 'GET' else None,
                timeout=20,
            )
            if resp.status_code >= 400:
                raise Exception(f"{exchange_key} {method} {new_url} -> {resp.status_code}: {resp.text[:300]}")
            try:
                return json.loads(resp.text)
            except ValueError:
                return resp.text

        try:
            return do_request()
        except Exception:
            return do_request()

    ex.fetch = proxied_fetch
    if exchange_key == 'bybit':
        ex.has['fetchCurrencies'] = False
    return ex

def build_exchange(name, mode):
    name_lower = name.lower()
    if mode == 'scanner':
        proxied = name_lower in PROXY_EXCHANGE_IDS_SCAN
        creds = CREDENTIALS_SCAN.get(name_lower, {})
        timeout = PROXY_TIMEOUT_MS if proxied else TIMEOUT_MS
    else:
        proxied = name_lower in PROXY_EXCHANGE_IDS_TRADE
        creds = CREDENTIALS_TRADE.get(name_lower, {})
        timeout = PROXY_TIMEOUT_MS if proxied else TIMEOUT_MS

    cfg = {
        'enableRateLimit': True,
        'timeout': timeout,
        'options': {'adjustForTimeDifference': True},
    }

    hostname_override = EXCHANGE_HOSTNAME_OVERRIDES.get(name_lower)
    if hostname_override:
        cfg['hostname'] = hostname_override

    if creds.get('apiKey'):
        cfg['apiKey'] = creds['apiKey']
    if creds.get('secret'):
        cfg['secret'] = creds['secret']
    if creds.get('password'):
        cfg['password'] = creds['password']
    if creds.get('uid'):
        cfg['uid'] = creds['uid']

    exchange_class = getattr(ccxt, name_lower, None)
    if exchange_class is None:
        raise ValueError(f"Unsupported exchange: {name}")
    ex = exchange_class(cfg)

    if proxied:
        ex = route_through_proxy(ex, mode)
        if name_lower == 'kucoin':
            try:
                ex.set_markets(ex.fetch_markets())
            except Exception as e:
                log.warning(f"  WARNING  KuCoin set_markets bootstrap failed: {str(e)[:300]}")

    if not proxied:
        try:
            ex.load_markets()
            if not ex.markets:
                log.warning(f"  WARNING  {name}: load_markets() returned 0 markets")
        except Exception as e:
            log.warning(f"  WARNING  {name}: load_markets() failed — {str(e)[:400]}")

    return ex

EXCHANGES_CACHE = {'scanner': {}, 'trader': {}}

def ensure_exchange(name):
    mode = get_exchange_mode()
    cache = EXCHANGES_CACHE[mode]
    if name in cache and cache[name] is not None:
        return cache[name]
    try:
        ex = with_retries(lambda: build_exchange(name, mode), f"{name} ({mode})")
    except Exception as e:
        log.warning(f"  WARNING  {name} ({mode}): could not initialize — {str(e)[:400]}")
        cache[name] = None
        return None
    cache[name] = ex
    return ex

def _clean_networks_dict(networks):
    if not isinstance(networks, dict):
        return {}
    return {code: net for code, net in networks.items() if isinstance(net, dict)}

def _sanitize_currencies(data):
    if not isinstance(data, dict):
        return {}
    clean = {}
    for code, cur in data.items():
        if not isinstance(cur, dict):
            continue
        networks = cur.get('networks')
        if networks is not None:
            cur['networks'] = _clean_networks_dict(networks)
        clean[code] = cur
    return clean

CURRENCY_CACHE = {}
CURRENCY_CACHE_TTL = 1800
CURRENCY_LOCKS = defaultdict(threading.Lock)

def get_currencies(exchange_name):
    now = time.time()
    cached = CURRENCY_CACHE.get(exchange_name)
    if cached and (now - cached['ts']) < CURRENCY_CACHE_TTL:
        return cached['data']
    with CURRENCY_LOCKS[exchange_name]:
        cached = CURRENCY_CACHE.get(exchange_name)
        now = time.time()
        if cached and (now - cached['ts']) < CURRENCY_CACHE_TTL:
            return cached['data']
        ex = ensure_exchange(exchange_name)
        if ex is None:
            return cached['data'] if cached else {}
        try:
            data = ex.fetch_currencies() or {}
            data = _sanitize_currencies(data)
        except Exception as e:
            log.warning(f"  WARNING  {exchange_name} currencies: {str(e)[:120]}")
            data = {}
        if not data and cached:
            data = cached['data']
        CURRENCY_CACHE[exchange_name] = {'ts': now, 'data': data}
        return data

NETWORK_ALIASES = {
    'ETH': 'ETH', 'ERC20': 'ETH', 'ETHEREUM': 'ETH', 'ETHER': 'ETH',
    'BSC': 'BSC', 'BEP20': 'BSC', 'BNB': 'BSC', 'BNBSMARTCHAIN': 'BSC', 'BNBBEP20': 'BSC',
    'TRX': 'TRX', 'TRC20': 'TRX', 'TRON': 'TRX',
    'MATIC': 'MATIC', 'POLYGON': 'MATIC', 'POLYGONPOS': 'MATIC',
    'ARBITRUM': 'ARB', 'ARB': 'ARB', 'ARBITRUMONE': 'ARB',
    'OPTIMISM': 'OP', 'OP': 'OP',
    'SOL': 'SOL', 'SOLANA': 'SOL',
    'AVAX': 'AVAX', 'AVALANCHE': 'AVAX', 'AVAXC': 'AVAX', 'AVALANCHEC': 'AVAX',
    'BTC': 'BTC', 'BITCOIN': 'BTC',
    'LIGHTNING': 'BTC-LN', 'LN': 'BTC-LN',
    'BASE': 'BASE',
    'TON': 'TON',
    'APT': 'APT', 'APTOS': 'APT',
    'SUI': 'SUI',
    'XRP': 'XRP', 'RIPPLE': 'XRP',
    'DOGE': 'DOGE', 'DOGECOIN': 'DOGE',
    'LTC': 'LTC', 'LITECOIN': 'LTC',
    'ZKSYNC': 'ZKSYNC', 'ZKSYNCERA': 'ZKSYNC',
    'ROBINHOOD': 'ROBINHOOD', 'ROBINHOODCHAIN': 'ROBINHOOD',
    'PLASMA': 'PLASMA',
    'CELO': 'CELO',
}

_NETWORK_PAREN_RE = re.compile(r'\(([^)]+)\)\s*$')

def normalize_network(code):
    if not code:
        return None
    raw = str(code).strip()
    m = _NETWORK_PAREN_RE.search(raw)
    if m:
        raw = m.group(1)
    key = raw.upper().replace('-', '').replace('_', '').replace(' ', '')
    return NETWORK_ALIASES.get(key, key)

_NAME_JUNK_SUFFIXES = (' token', ' coin', ' protocol', ' network', ' finance', ' chain', ' project')

def normalize_name(name):
    if not name:
        return ''
    n = name.strip().lower()
    for junk in _NAME_JUNK_SUFFIXES:
        if n.endswith(junk):
            n = n[: -len(junk)]
    return re.sub(r'[^a-z0-9]', '', n)

def name_is_informative(name, code):
    if not name:
        return False
    return name.strip().upper() != (code or '').strip().upper()

def compare_names(buy_name, sell_name, code):
    buy_ok  = name_is_informative(buy_name, code)
    sell_ok = name_is_informative(sell_name, code)
    if buy_ok and sell_ok:
        return (normalize_name(buy_name) == normalize_name(sell_name)), False
    if buy_ok or sell_ok:
        real_name = buy_name if buy_ok else sell_name
        norm_real = normalize_name(real_name)
        norm_code = normalize_name(code)
        if norm_code and (norm_code in norm_real or norm_real in norm_code):
            return None, False
        return None, True
    return None, False

_CONTRACT_KEYS = ('contractAddress', 'contract_address', 'contract', 'tokenAddress', 'token_address', 'address')

def _extract_contract(network_data, currency_data=None):
    for source in (network_data, currency_data):
        if not isinstance(source, dict):
            continue
        info = (source or {}).get('info')
        if isinstance(info, dict):
            for key in _CONTRACT_KEYS:
                val = info.get(key)
                if val:
                    return val
    return None

def _fallback_networks(exchange_name, base):
    ex = ensure_exchange(exchange_name)
    if ex is None:
        return {}
    try:
        if getattr(ex, 'has', {}).get('fetchDepositWithdrawFee'):
            fee_data = ex.fetch_deposit_withdraw_fee(base)
            return _clean_networks_dict((fee_data or {}).get('networks') or {})
        if getattr(ex, 'has', {}).get('fetchDepositWithdrawFees'):
            fee_data = ex.fetch_deposit_withdraw_fees([base])
            return _clean_networks_dict(((fee_data or {}).get(base) or {}).get('networks') or {})
    except Exception as e:
        log.warning(f"  WARNING  {exchange_name} deposit/withdraw fee fallback for {base}: {str(e)[:400]}")
    return {}

def _to_float(val):
    if val is None:
        return None
    try:
        return float(val)
    except (TypeError, ValueError):
        return None

_TIME_TEXT_RE = re.compile(
    r'(?:(?P<hours>\d+(?:\.\d+)?)\s*h(?:ou)?rs?)?[\s,]*'
    r'(?:(?P<mins>\d+(?:\.\d+)?)\s*m(?:in)?s?)?[\s,]*'
    r'(?:(?P<secs>\d+(?:\.\d+)?)\s*s(?:ec)?s?)?',
    re.IGNORECASE,
)

_TIME_KEYS = (
    'estimatedArrivalTime', 'estimated_arrival_time',
    'arrivalTime', 'arrival_time',
    'expectedArrivalTime', 'expected_arrival_time',
    'expectedArrival', 'expected_arrival',
    'estimateArrivalTime', 'estimate_arrival_time',
    'estimatedTime', 'estimated_time',
    'estimatedDuration', 'estimated_duration',
    'transferTime', 'transfer_time',
    'networkTime', 'network_time',
    'networkDuration', 'network_duration',
    'avgArrivalTime', 'avg_arrival_time',
    'averageArrivalTime', 'average_arrival_time',
    'processingTime', 'processing_time',
    'depositProcessingTime', 'deposit_processing_time',
    'withdrawalProcessingTime', 'withdrawal_processing_time',
    'depositTime', 'deposit_time',
    'withdrawTime', 'withdraw_time',
    'blockTime', 'block_time',
    'avgTime', 'avg_time',
    'averageTime', 'average_time',
    'time',
)

_CONFIRM_KEYS = (
    'confirmations', 'confirms', 'confirmation', 'minConfirm', 'min_confirm',
    'requiredConfirmCount', 'confirmationCount', 'confirmNumber', 'unLockConfirm',
    'depositConfirm', 'deposit_confirm', 'withdrawConfirm', 'withdraw_confirm',
    'minConfirmCount',
)

_TIME_PREFIX_JUNK_RE = re.compile(
    r'^\s*(?:[≈~≃≅~]|about|approx(?:\.|imately)?|around|circa|~|est(?:imated)?\.?|'
    r'expected|est\.|takes?\s+|up\s+to\s+)+[\s:]*',
    re.IGNORECASE,
)

def _parse_time_text_to_seconds(value):
    if value is None:
        return None
    if isinstance(value, (int, float)):
        return float(value) * 60 if value > 0 else None

    text = str(value).strip()
    if not text:
        return None

    text = _TIME_PREFIX_JUNK_RE.sub('', text).strip()
    if not text:
        return None

    m = _TIME_TEXT_RE.search(text)
    if not m or not any(m.group(g) for g in ('hours', 'mins', 'secs')):
        return None

    hours = float(m.group('hours') or 0)
    mins  = float(m.group('mins') or 0)
    secs  = float(m.group('secs') or 0)
    total = hours * 3600 + mins * 60 + secs
    return total if total > 0 else None

def _find_first_key(d, keys):
    for k in keys:
        if k in d and d[k] not in (None, ''):
            return k, d[k]
    return None, None

def _extract_time_estimate(network_data, network_norm):
    if not isinstance(network_data, dict):
        return None, None
    info = network_data.get('info')
    info = info if isinstance(info, dict) else {}

    for source in (network_data, info):
        if not isinstance(source, dict):
            continue
        key, val = _find_first_key(source, _TIME_KEYS)
        if key is not None:
            seconds = _parse_time_text_to_seconds(val)
            if seconds is not None:
                return seconds, f"{key}={val!r}"

    return None, None

def _extract_confirm_hint(network_data):
    if not isinstance(network_data, dict):
        return None
    info = network_data.get('info')
    info = info if isinstance(info, dict) else {}
    for source in (network_data, info):
        if not isinstance(source, dict):
            continue
        key, val = _find_first_key(source, _CONFIRM_KEYS)
        if key is not None:
            return f"{key}={val!r}"
    return None

def _fmt_duration(seconds):
    if seconds is None:
        return "unknown"
    seconds = int(round(seconds))
    mins, secs = divmod(seconds, 60)
    if mins and secs:
        return f"{mins}min {secs}s"
    if mins:
        return f"{mins}min"
    return f"{secs}s"

def _fmt_networks(networks):
    if not networks or not isinstance(networks, dict):
        return "(none)"
    parts = []
    for code, data in list(networks.items())[:10]:
        w   = data.get('withdraw', True)
        d   = data.get('deposit', True)
        act = data.get('active', True)
        fee = data.get('fee')
        min_wd = ((data.get('limits') or {}).get('withdraw') or {}).get('min')
        parts.append(f"{code}[w={w},d={d},act={act},fee={fee},min={min_wd}]")
    suffix = "" if len(networks) <= 10 else f"  (+{len(networks) - 10} more)"
    return ", ".join(parts) + suffix

def find_currency_by_name(exchange_name, target_name, exclude_code=None):
    currencies = get_currencies(exchange_name)
    norm_target = normalize_name(target_name)
    if not norm_target:
        return None
    for code, cur in currencies.items():
        if code == exclude_code:
            continue
        if not isinstance(cur, dict):
            continue
        cur_name = (cur.get('name') or '').strip()
        if not cur_name:
            continue
        if normalize_name(cur_name) == norm_target:
            return code, cur
    return None

def fetch_symbol_price(exchange_name, symbol, side):
    ex = ensure_exchange(exchange_name)
    if ex is None:
        return None
    try:
        params = EXTRA_PARAMS.get(exchange_name, {})
        t = ex.fetch_ticker(symbol, params=params)
    except Exception as e:
        log.warning(f"  WARNING  {exchange_name} fetch_ticker({symbol}): {str(e)[:400]}")
        return None
    val = t.get(side)
    return val if val else t.get('last')

def other_deposit_blocks(base, norm_key, exclude, all_prices):
    blocked_others = []
    for ex in sorted(all_prices):
        if ex in exclude:
            continue
        cur = get_currencies(ex).get(base)
        if not isinstance(cur, dict):
            continue
        networks = cur.get('networks') or {}
        if not networks:
            networks = _fallback_networks(ex, base)
        by_norm = defaultdict(list)
        for code, data in networks.items():
            by_norm[normalize_network(code)].append((code, data))
        entries = by_norm.get(norm_key)
        if not entries:
            continue
        code, data = entries[0]
        can_deposit = data.get('deposit', True) is not False and data.get('active', True) is not False
        if not can_deposit:
            blocked_others.append((ex, code))
    return blocked_others

def _network_entries_matching_contract(networks, cur, contract):
    if not isinstance(networks, dict):
        return []
    if not contract:
        return list(networks.items())
    out = [
        (code, data) for code, data in networks.items()
        if (c := _extract_contract(data, cur)) and c.lower() == contract.lower()
    ]
    return out or list(networks.items())

def meta_summary_for(exchange_name, base):
    cur = get_currencies(exchange_name).get(base)
    if not isinstance(cur, dict):
        return f"{exchange_name}: '{base}' currency data is not a dict (type {type(cur).__name__})"
    name = (cur.get('name') or '').strip()
    networks = cur.get('networks') or {}
    if not networks:
        networks = _fallback_networks(exchange_name, base)
    contract = None
    for net_data in networks.values():
        if isinstance(net_data, dict):
            contract = _extract_contract(net_data, cur)
            if contract:
                break
    return (
        f"{exchange_name}: name={name!r}  networks: "
        f"{_fmt_networks(networks)}  contract={contract}"
    )

def find_confirmed_pair(base, all_prices):
    info = {}
    for ex in all_prices:
        cur = get_currencies(ex).get(base)
        if not isinstance(cur, dict):
            continue
        name = (cur.get('name') or '').strip()
        if not name_is_informative(name, base):
            continue
        info[ex] = (name, cur)
    ex_list = list(info.keys())
    matched_pairs = []
    for i in range(len(ex_list)):
        for j in range(i + 1, len(ex_list)):
            ex1, ex2 = ex_list[i], ex_list[j]
            n1, cur1 = info[ex1]
            n2, cur2 = info[ex2]
            if normalize_name(n1) == normalize_name(n2):
                matched_pairs.append((ex1, ex2, cur1, cur2))
    if not matched_pairs:
        return None

    best = None
    for ex1, ex2, cur1, cur2 in matched_pairs:
        p1, p2 = all_prices.get(ex1, {}).get('price'), all_prices.get(ex2, {}).get('price')
        if not p1 or not p2 or p1 == p2:
            continue
        if p1 < p2:
            buy_c, sell_c, buy_p, sell_p, buy_cur, sell_cur = ex1, ex2, p1, p2, cur1, cur2
        else:
            buy_c, sell_c, buy_p, sell_p, buy_cur, sell_cur = ex2, ex1, p2, p1, cur2, cur1
        gap_pct = ((sell_p - buy_p) / buy_p) * 100
        if best is None or gap_pct > best['gap_pct']:
            best = {
                'buy_ex': buy_c, 'sell_ex': sell_c,
                'buy_price': buy_p, 'sell_price': sell_p,
                'gap_pct': gap_pct,
                'buy_cur': buy_cur, 'sell_cur': sell_cur,
            }
    if best is None:
        return None

    buy_networks = best['buy_cur'].get('networks') or {}
    if not buy_networks:
        buy_networks = _fallback_networks(best['buy_ex'], base)
    sell_networks = best['sell_cur'].get('networks') or {}
    if not sell_networks:
        sell_networks = _fallback_networks(best['sell_ex'], base)
    buy_by_norm = defaultdict(list)
    for code, data in buy_networks.items():
        buy_by_norm[normalize_network(code)].append((code, data))
    sell_by_norm = defaultdict(list)
    for code, data in sell_networks.items():
        sell_by_norm[normalize_network(code)].append((code, data))
    tradable_network = None
    blocked_msgs = []
    for norm_key, buy_entries in buy_by_norm.items():
        sell_entries = sell_by_norm.get(norm_key)
        if not sell_entries:
            continue
        buy_code, buy_data = buy_entries[0]
        sell_code, sell_data = sell_entries[0]
        can_withdraw = buy_data.get('withdraw', True) is not False and buy_data.get('active', True) is not False
        can_deposit  = sell_data.get('deposit', True) is not False and sell_data.get('active', True) is not False
        if can_withdraw and can_deposit:
            tradable_network = norm_key
            tradable_fee = _to_float(buy_data.get('fee'))
            tradable_min = _to_float(((buy_data.get('limits') or {}).get('withdraw') or {}).get('min'))
            best['min_withdrawal'] = tradable_min
            best['fee'] = tradable_fee
            break
        else:
            if not can_withdraw:
                blocked_msgs.append(f"{best['buy_ex']} withdraw disabled for {buy_code}")
            if not can_deposit:
                blocked_msgs.append(f"{best['sell_ex']} deposit disabled for {sell_code}")
    else:
        tradable_fee = None
        best['min_withdrawal'] = None
        best['fee'] = None
    best['tradable_network'] = tradable_network
    best['blocked_msgs'] = blocked_msgs
    return best

def _route_profit(route, symbol):
    if route['direction'] == 'alt_buy':
        buy_ex_for_fee, sell_ex_for_fee = route['alt_ex'], route['sell_ex']
        buy_price, sell_price = route['alt_price'], route['sell_price_ref']
    else:
        buy_ex_for_fee, sell_ex_for_fee = route['buy_ex'], route['alt_ex']
        buy_price, sell_price = route['buy_price_ref'], route['alt_price']
    buy_fees  = get_taker_fee(buy_ex_for_fee, symbol)
    sell_fees = get_taker_fee(sell_ex_for_fee, symbol)
    return calc_arb_profit(
        CAPITAL_USDT, buy_price, sell_price,
        fee_tokens=route.get('fee'),
        min_withdrawal_tokens=route.get('min_withdrawal'),
        buy_fees=buy_fees,
        sell_fees=sell_fees,
    )

def evaluate_network_block(
    base,
    buy_ex,
    sell_ex,
    buy_price,
    sell_price,
    all_prices,
    blocked_details,
    contract=None,
    symbol=None,
):
    other_exchanges = sorted(ex for ex in all_prices if ex not in (buy_ex, sell_ex))
    if not other_exchanges:
        return None

    routes = []

    for entry in blocked_details:
        norm_key = entry['norm_key']

        if not entry['can_withdraw'] and entry['can_deposit']:
            for alt_ex in other_exchanges:
                alt_price = all_prices.get(alt_ex, {}).get('ask')
                if not alt_price or alt_price <= 0 or alt_price >= sell_price:
                    continue
                gap_pct = ((sell_price - alt_price) / alt_price) * 100
                if gap_pct < 0:
                    continue
                cur = get_currencies(alt_ex).get(base)
                if not isinstance(cur, dict):
                    continue
                networks = cur.get('networks') or {}
                if not networks:
                    networks = _fallback_networks(alt_ex, base)
                candidates = _network_entries_matching_contract(networks, cur, contract)
                matched = next(
                    (c_d for c_d in candidates if normalize_network(c_d[0]) == norm_key),
                    None,
                )
                if not matched:
                    continue
                code, data = matched
                withdraw_ok = (
                    data.get('withdraw', True) is not False
                    and data.get('active', True) is not False
                )
                if withdraw_ok:
                    routes.append({
                        'direction': 'alt_buy',
                        'blocked_ex': buy_ex,
                        'alt_ex': alt_ex,
                        'sell_ex': sell_ex,
                        'network': norm_key,
                        'gap_pct': gap_pct,
                        'alt_price': alt_price,
                        'sell_price_ref': sell_price,
                        'fee': _to_float(data.get('fee')),
                        'min_withdrawal': _to_float(
                            ((data.get('limits') or {}).get('withdraw') or {}).get('min')
                        ),
                        'blocked_reason': f"{buy_ex} withdraw disabled for {norm_key}",
                    })

        if entry['can_withdraw'] and not entry['can_deposit']:
            for alt_ex in other_exchanges:
                alt_price = all_prices.get(alt_ex, {}).get('bid')
                if not alt_price or alt_price <= buy_price:
                    continue
                gap_pct = ((alt_price - buy_price) / buy_price) * 100
                if gap_pct < 0:
                    continue
                cur = get_currencies(alt_ex).get(base)
                if not isinstance(cur, dict):
                    continue
                networks = cur.get('networks') or {}
                if not networks:
                    networks = _fallback_networks(alt_ex, base)
                candidates = _network_entries_matching_contract(networks, cur, contract)
                matched = next(
                    (c_d for c_d in candidates if normalize_network(c_d[0]) == norm_key),
                    None,
                )
                if not matched:
                    continue
                code, data = matched
                deposit_ok = (
                    data.get('deposit', True) is not False
                    and data.get('active', True) is not False
                )
                if deposit_ok:
                    buy_data_entry = entry.get('buy_data') or {}
                    routes.append({
                        'direction': 'alt_sell',
                        'blocked_ex': sell_ex,
                        'alt_ex': alt_ex,
                        'buy_ex': buy_ex,
                        'network': norm_key,
                        'gap_pct': gap_pct,
                        'alt_price': alt_price,
                        'buy_price_ref': buy_price,
                        'fee': _to_float(buy_data_entry.get('fee')),
                        'min_withdrawal': _to_float(
                            ((buy_data_entry.get('limits') or {}).get('withdraw') or {}).get('min')
                        ),
                        'blocked_reason': f"{sell_ex} deposit disabled for {norm_key}",
                    })

    if not routes:
        return None

    if symbol:
        for route in routes:
            profit = _route_profit(route, symbol)
            route['profit'] = profit
            route['net_pnl'] = profit['net_pnl'] if profit else float('-inf')
        best = max(routes, key=lambda route: route['net_pnl'])
        return best

    return max(routes, key=lambda route: route['gap_pct'])

def _is_confirmed_tradable(r):
    if not r.get('meta_checked'):
        return False
    if r['verified'] is True:
        return True
    if r['verified'] is False and r.get('alt_route'):
        return True
    if r['verified'] is None:
        cp = r.get('confirmed_pair')
        if cp and cp.get('tradable_network'):
            return True
    return False

def check_metadata(r):
    base = r['symbol'].split('/')[0]
    buy_ex, sell_ex = r['buy_ex'], r['sell_ex']
    other_exchanges = sorted(ex for ex in r['all_prices'] if ex not in (buy_ex, sell_ex))
    r['extra_meta_summaries'] = [meta_summary_for(ex, base) for ex in other_exchanges]
    buy_currencies  = get_currencies(buy_ex)
    sell_currencies = get_currencies(sell_ex)
    buy_cur  = buy_currencies.get(base)
    sell_cur = sell_currencies.get(base)
    r['meta_checked'] = True

    if buy_cur is not None and not isinstance(buy_cur, dict):
        buy_cur = None
    if sell_cur is not None and not isinstance(sell_cur, dict):
        sell_cur = None

    if not buy_cur or not sell_cur:
        r['buy_meta_summary']  = f"{buy_ex}: '{base}' not found in currency list" if not buy_cur else f"{buy_ex}: currency data invalid"
        r['sell_meta_summary'] = f"{sell_ex}: '{base}' not found in currency list" if not sell_cur else f"{sell_ex}: currency data invalid"
        r['verified'] = None
        return r

    buy_name  = (buy_cur.get('name')  or '').strip()
    sell_name = (sell_cur.get('name') or '').strip()
    r['buy_name'], r['sell_name'] = buy_name, sell_name
    r['name_match'], r['name_suspect'] = compare_names(buy_name, sell_name, base)
    is_confirmed_conflict = r['name_match'] is False
    should_attempt_resolution = is_confirmed_conflict or (r['name_match'] is None and name_is_informative(buy_name, base))
    if should_attempt_resolution:
        found = find_currency_by_name(sell_ex, buy_name, exclude_code=base)
        if not found:
            if is_confirmed_conflict:
                r['verified'] = False
                r['buy_meta_summary']  = f"{buy_ex}: name={buy_name!r}"
                r['sell_meta_summary'] = (
                    f"{sell_ex}: '{base}' is name={sell_name!r} — CONFIRMED different token; "
                    f"'{buy_name}' does not exist on {sell_ex} under any ticker"
                )
                return r
        else:
            real_code, real_cur = found
            real_symbol = f"{real_code}/USDT"
            real_price  = fetch_symbol_price(sell_ex, real_symbol, 'bid')
            if not real_price:
                if is_confirmed_conflict:
                    r['verified'] = False
                    r['buy_meta_summary'] = (
                        f"{buy_ex}: name={buy_name!r} — this project also lists on {sell_ex} as "
                        f"'{real_code}', but its price couldn't be fetched to re-confirm any gap"
                    )
                    r['sell_meta_summary'] = f"{sell_ex}: '{base}' is name={sell_name!r} — a DIFFERENT token from {buy_ex}'s '{buy_name}'"
                    return r
            else:
                corrected_gap = ((real_price - r['buy_price']) / r['buy_price']) * 100
                r['corrected_symbol']     = real_symbol
                r['corrected_sell_price'] = real_price
                r['corrected_gap_pct']    = corrected_gap
                try:
                    sell_venue = ensure_exchange(sell_ex)
                    ob = sell_venue.fetch_order_book(real_symbol, limit=order_book_limit_for(sell_ex), params=EXTRA_PARAMS.get(sell_ex, {}))
                    corrected_bids = _sort_book_side(ob.get('bids', []) or [], 'bids')
                    corrected_sell_depth, _, _ = walk_book(corrected_bids, DEPTH_CHECK_USDT)
                    if corrected_sell_depth and r.get('buy_depth_price'):
                        r['corrected_depth_gap_pct']    = ((corrected_sell_depth - r['buy_depth_price']) / r['buy_depth_price']) * 100
                        r['corrected_sell_depth_price'] = corrected_sell_depth
                except Exception as e:
                    log.warning(f"  WARNING  corrected depth check {real_symbol} on {sell_ex}: {str(e)[:400]}")
                sell_cur  = real_cur
                sell_name = (real_cur.get('name') or '').strip()
                r['sell_name'] = sell_name
                r['name_match'], r['name_suspect'] = True, False

    buy_networks  = buy_cur.get('networks')  or {}
    sell_networks = sell_cur.get('networks') or {}
    if not buy_networks:
        buy_networks = _fallback_networks(buy_ex, base)
    if not sell_networks:
        sell_networks = _fallback_networks(sell_ex, base)
    r['networks_known'] = bool(buy_networks) and bool(sell_networks)
    r['buy_meta_summary']  = f"{buy_ex}: name={buy_name!r}  networks: {_fmt_networks(buy_networks)}"
    r['sell_meta_summary'] = f"{sell_ex}: name={sell_name!r}  networks: {_fmt_networks(sell_networks)}"

    sell_contracts_by_addr = defaultdict(list)
    for s_code, s_data in sell_networks.items():
        c = _extract_contract(s_data, sell_cur)
        if c:
            sell_contracts_by_addr[c.lower()].append((s_code, s_data))
    contract_matches = []
    for b_code, b_data in buy_networks.items():
        c = _extract_contract(b_data, buy_cur)
        if not c:
            continue
        for s_code, s_data in sell_contracts_by_addr.get(c.lower(), []):
            contract_matches.append((b_code, b_data, s_code, s_data, c))

    buy_by_norm = defaultdict(list)
    for code, data in buy_networks.items():
        buy_by_norm[normalize_network(code)].append((code, data))
    sell_by_norm = defaultdict(list)
    for code, data in sell_networks.items():
        sell_by_norm[normalize_network(code)].append((code, data))
    common = []
    blocked = []
    blocked_details = []
    seen_pairs = set()
    for norm_key, buy_entries in buy_by_norm.items():
        sell_entries = sell_by_norm.get(norm_key)
        if not sell_entries:
            continue
        buy_code, buy_data   = buy_entries[0]
        sell_code, sell_data = sell_entries[0]
        seen_pairs.add((buy_code, sell_code))
        can_withdraw = buy_data.get('withdraw', True) is not False and buy_data.get('active', True) is not False
        can_deposit  = sell_data.get('deposit', True) is not False and sell_data.get('active', True) is not False
        if can_withdraw and can_deposit:
            common.append((norm_key, buy_code, buy_data, sell_code, sell_data))
        else:
            reasons = []
            if not can_withdraw:
                reasons.append(f"{buy_ex} withdraw disabled for {buy_code}")
            if not can_deposit:
                reasons.append(f"{sell_ex} deposit disabled for {sell_code}")
            blocked.append(f"{norm_key} ({', '.join(reasons)})")
            blocked_details.append({
                'norm_key':     norm_key,
                'buy_code':     buy_code,
                'buy_data':     buy_data,
                'sell_code':    sell_code,
                'can_withdraw': can_withdraw,
                'can_deposit':  can_deposit,
                'other_blocked': (
                    other_deposit_blocks(base, norm_key, {buy_ex, sell_ex}, r['all_prices'])
                    if can_withdraw and not can_deposit else []
                ),
            })

    for buy_code, buy_data, sell_code, sell_data, contract in contract_matches:
        if (buy_code, sell_code) in seen_pairs:
            continue
        seen_pairs.add((buy_code, sell_code))
        label = f"{buy_code}→{sell_code} [contract match]" if buy_code != sell_code else f"{buy_code} [contract match]"
        can_withdraw = buy_data.get('withdraw', True) is not False and buy_data.get('active', True) is not False
        can_deposit  = sell_data.get('deposit', True) is not False and sell_data.get('active', True) is not False
        if can_withdraw and can_deposit:
            common.append((label, buy_code, buy_data, sell_code, sell_data))
        else:
            reasons = []
            if not can_withdraw:
                reasons.append(f"{buy_ex} withdraw disabled for {buy_code}")
            if not can_deposit:
                reasons.append(f"{sell_ex} deposit disabled for {sell_code}")
            blocked.append(f"{label} ({', '.join(reasons)})")
            blocked_details.append({
                'norm_key':      label,
                'buy_code':      buy_code,
                'buy_data':      buy_data,
                'sell_code':     sell_code,
                'can_withdraw':  can_withdraw,
                'can_deposit':   can_deposit,
                'other_blocked': [],
            })

    r['common_networks'] = [c[0] for c in common]
    r['blocked_networks'] = blocked
    r['blocked_details'] = blocked_details

    if contract_matches:
        r['contract_buy']   = contract_matches[0][4]
        r['contract_sell']  = contract_matches[0][4]
        r['contract_match'] = True

    r['alt_route'] = None
    if not common and blocked_details and r['n_exchanges'] >= 3:
        r['alt_route'] = evaluate_network_block(
            base, buy_ex, sell_ex, r['buy_price'], r['sell_price'], r['all_prices'],
            blocked_details, contract=r['contract_buy'], symbol=r['symbol']
        )

    if common:
        norm_key, buy_code, buy_data, sell_code, sell_data = min(
            common,
            key=lambda c: _to_float(c[2].get('fee')) if c[2].get('fee') is not None else float('inf')
        )
        r['withdrawal_network'] = (
            norm_key if ('[contract match]' in norm_key or buy_code == sell_code)
            else f"{norm_key} ({buy_code}→{sell_code})"
        )
        r['withdrawal_network_norm'] = normalize_network(buy_code)
        r['withdrawal_fee']        = _to_float(buy_data.get('fee'))
        r['withdrawal_min_tokens'] = _to_float(((buy_data.get('limits') or {}).get('withdraw') or {}).get('min'))

        withdraw_secs, withdraw_desc = _extract_time_estimate(buy_data, r['withdrawal_network_norm'])
        deposit_secs,  deposit_desc  = _extract_time_estimate(sell_data, r['withdrawal_network_norm'])
        r['transfer_withdraw_seconds'] = withdraw_secs
        r['transfer_deposit_seconds']  = deposit_secs

        known_parts = [s for s in (withdraw_secs, deposit_secs) if s is not None]
        known_total = sum(known_parts)

        if known_total > MAX_TRANSFER_TIME_SEC:
            r['transfer_seconds'] = known_total
            r['transfer_time_estimated'] = False
        elif len(known_parts) == 2:
            r['transfer_seconds'] = known_total
            r['transfer_time_estimated'] = False
        else:
            r['transfer_seconds'] = MAX_TRANSFER_TIME_SEC
            r['transfer_time_estimated'] = True

        desc_parts = []
        if withdraw_secs is not None:
            desc_parts.append(f"withdraw ~{_fmt_duration(withdraw_secs)} ({withdraw_desc})")
        elif (hint := _extract_confirm_hint(buy_data)):
            desc_parts.append(f"withdraw time unknown ({hint}; assumed {_fmt_duration(MAX_TRANSFER_TIME_SEC)})")
        else:
            desc_parts.append(f"withdraw time unknown (assumed {_fmt_duration(MAX_TRANSFER_TIME_SEC)})")
        if deposit_secs is not None:
            desc_parts.append(f"deposit ~{_fmt_duration(deposit_secs)} ({deposit_desc})")
        elif (hint := _extract_confirm_hint(sell_data)):
            desc_parts.append(f"deposit time unknown ({hint}; assumed {_fmt_duration(MAX_TRANSFER_TIME_SEC)})")
        else:
            desc_parts.append(f"deposit time unknown (assumed {_fmt_duration(MAX_TRANSFER_TIME_SEC)})")
        r['transfer_time_desc'] = " + ".join(desc_parts)

        if r['contract_match'] is None:
            contract_buy  = _extract_contract(buy_data, buy_cur)
            contract_sell = _extract_contract(sell_data, sell_cur)
            r['contract_buy']  = contract_buy
            r['contract_sell'] = contract_sell
            if contract_buy and contract_sell:
                r['contract_match'] = contract_buy.lower() == contract_sell.lower()

    r['buy_meta_summary']  += f"  contract={r['contract_buy']}"
    r['sell_meta_summary'] += f"  contract={r['contract_sell']}"
    if r['contract_match'] is True:
        r['verified'] = bool(common)
    elif r['contract_match'] is False:
        r['verified'] = False
    elif r['name_match'] is False:
        r['verified'] = False
    elif r['name_suspect']:
        r['verified'] = None
    elif common and r['name_match'] is not False:
        r['verified'] = True
    elif not r['networks_known']:
        r['verified'] = None
    else:
        r['verified'] = False

    r['confirmed_pair'] = None
    if r['name_suspect'] and r['verified'] is not True and r['n_exchanges'] >= 3:
        r['confirmed_pair'] = find_confirmed_pair(base, r['all_prices'])
    return r

_FEE_CACHE = {}
_FEE_CACHE_LOCK = threading.Lock()

def _valid_fee(val):
    v = _to_float(val)
    if v is None or v != v or v < 0:
        return None
    return v

def _cache_get(key, ttl):
    with _FEE_CACHE_LOCK:
        hit = _FEE_CACHE.get(key)
    if hit and time.time() - hit[0] < ttl:
        return hit
    return None

def _cache_put(key, value):
    with _FEE_CACHE_LOCK:
        _FEE_CACHE[key] = (time.time(), value)

def _find_key(obj, names):
    if isinstance(obj, dict):
        for k, v in obj.items():
            if k in names and not isinstance(v, (dict, list)):
                return v
        for v in obj.values():
            found = _find_key(v, names)
            if found is not None:
                return found
    elif isinstance(obj, list):
        for v in obj:
            found = _find_key(v, names)
            if found is not None:
                return found
    return None

def _bingx_spot_taker_fee(ex, symbol):
    if not ex.markets:
        ex.load_markets()
    market = ex.market(symbol)
    if not market.get('spot'):
        raise ValueError(f"{symbol} is not a spot market")
    method = (
        getattr(ex, 'spotV1PrivateGetUserCommissionRate', None)
        or getattr(ex, 'spot_v1_private_get_user_commission_rate', None)
    )
    if method is None:
        raise ValueError("this ccxt version has no spot commissionRate endpoint for BingX")
    response = method({'symbol': market['id']})
    return _find_key(response, ('takerCommissionRate', 'taker'))

EXCHANGE_FEE_FETCHERS = {
    'BingX': _bingx_spot_taker_fee,
}

def _fetch_pair_taker_fee(exchange_name, symbol):
    ex = ensure_exchange(exchange_name)
    if ex is None:
        return None, "exchange not initialised"
    if not getattr(ex, 'apiKey', None):
        return None, "no API credentials (fee endpoints are private)"

    has = getattr(ex, 'has', {}) or {}
    errors = []

    custom_fetcher = EXCHANGE_FEE_FETCHERS.get(exchange_name)
    if custom_fetcher:
        try:
            fee = _valid_fee(custom_fetcher(ex, symbol))
            if fee is not None:
                return fee, None
            errors.append(f"{exchange_name} spot fee endpoint returned no taker rate")
        except Exception as e:
            errors.append(f"{exchange_name} spot fee endpoint: {str(e)[:150]}")

    if has.get('fetchTradingFee'):
        try:
            fee = _valid_fee((ex.fetch_trading_fee(symbol) or {}).get('taker'))
            if fee is not None:
                return fee, None
            errors.append("fetchTradingFee returned no taker rate")
        except Exception as e:
            errors.append(f"fetchTradingFee: {str(e)[:150]}")

    if has.get('fetchTradingFees'):
        bulk_key = (exchange_name, '__all__')
        bulk = _cache_get(bulk_key, FEE_CACHE_TTL_SEC)
        try:
            if bulk is None:
                _cache_put(bulk_key, ex.fetch_trading_fees() or {})
                bulk = _cache_get(bulk_key, FEE_CACHE_TTL_SEC)
            fee = _valid_fee(((bulk[1] if bulk else {}).get(symbol) or {}).get('taker'))
            if fee is not None:
                return fee, None
            errors.append("fetchTradingFees has no taker rate for this pair")
        except Exception as e:
            errors.append(f"fetchTradingFees: {str(e)[:150]}")

    if not errors:
        errors.append("exchange has no fee endpoint in ccxt")
    return None, "; ".join(errors)

def get_taker_fee(exchange_name, symbol):
    key = (exchange_name, symbol)
    ok = _cache_get(key, FEE_CACHE_TTL_SEC)
    if ok is not None and ok[1] is not None:
        return ok[1]
    failed = _cache_get((exchange_name, symbol, 'fail'), FEE_RETRY_AFTER_FAIL_SEC)
    if failed is not None:
        return None

    rate, reason = _fetch_pair_taker_fee(exchange_name, symbol)
    if rate is None:
        _cache_put((exchange_name, symbol, 'fail'), reason)
        log.warning(
            f"  WARNING  {exchange_name} taker fee for {symbol} could not be fetched — {reason} "
            "— profit not calculated"
        )
        return None
    fees = {'exchange': exchange_name, 'taker': rate}
    _cache_put(key, fees)
    return fees

def _sort_book_side(levels, side):
    if not levels:
        return []
    return sorted(levels, key=lambda lvl: lvl[0], reverse=(side == 'bids'))

def walk_book(levels, target_usdt):
    filled_quote = 0.0
    filled_base  = 0.0
    for level in levels:
        if len(level) < 2:
            continue
        price, amount = level[0], level[1]
        if price <= 0 or amount <= 0:
            continue
        level_quote = price * amount
        if filled_quote + level_quote >= target_usdt:
            remaining_quote = target_usdt - filled_quote
            remaining_base  = remaining_quote / price
            filled_quote += remaining_quote
            filled_base  += remaining_base
            return filled_quote / filled_base, filled_quote, True
        filled_quote += level_quote
        filled_base  += amount
    if filled_base <= 0:
        return None, 0.0, False
    return filled_quote / filled_base, filled_quote, False

def _fetch_order_book_safe(exchange_name, venue, symbol):
    params   = EXTRA_PARAMS.get(exchange_name, {})
    last_err = None
    for attempt in range(1, DEPTH_FETCH_RETRIES + 1):
        try:
            return venue.fetch_order_book(
                symbol, limit=order_book_limit_for(exchange_name), params=params
            )
        except Exception as e:
            last_err = e
            if attempt < DEPTH_FETCH_RETRIES:
                time.sleep(DEPTH_FETCH_RETRY_DELAY)
    err_summary = f"{type(last_err).__name__}: {str(last_err)[:150]}"
    log.warning(
        f"  WARNING  depth check {symbol} on {exchange_name} "
        f"(failed all {DEPTH_FETCH_RETRIES} attempts): {err_summary}"
    )
    return None

def check_depth(r):
    buy_ex, sell_ex, symbol = r['buy_ex'], r['sell_ex'], r['symbol']
    buy_venue  = ensure_exchange(buy_ex)
    sell_venue = ensure_exchange(sell_ex)
    if buy_venue is None or sell_venue is None:
        return r

    buy_ob  = _fetch_order_book_safe(buy_ex,  buy_venue,  symbol)
    sell_ob = _fetch_order_book_safe(sell_ex, sell_venue, symbol)

    if buy_ob is None or sell_ob is None:
        r['depth_checked'] = True
        return r

    asks = _sort_book_side(buy_ob.get('asks', []) or [], 'asks')
    bids = _sort_book_side(sell_ob.get('bids', []) or [], 'bids')

    buy_price, _, buy_ok   = walk_book(asks, DEPTH_CHECK_USDT)
    sell_price, _, sell_ok = walk_book(bids, DEPTH_CHECK_USDT)

    r['depth_checked'] = True
    if buy_price is None or sell_price is None or buy_price <= 0:
        r['depth_ok'] = False
        return r
    r['buy_depth_price']  = buy_price
    r['sell_depth_price'] = sell_price
    r['depth_gap_pct']    = ((sell_price - buy_price) / buy_price) * 100
    r['depth_ok']         = buy_ok and sell_ok
    return r

def fmt_price(p):
    if   p >= 1:      return f"${p:,.4f}"
    elif p >= 0.0001: return f"${p:.6f}"
    else:             return f"${p:.8f}"

def fmt_vol(v):
    if   v >= 1_000_000: return f"${v/1_000_000:.2f}M"
    elif v >= 1_000:     return f"${v/1_000:.1f}K"
    else:                return f"${v:.0f}"

def calc_arb_profit(capital_usdt, buy_price, sell_price, fee_tokens=None,
                    min_withdrawal_tokens=None,
                    buy_fees=None, sell_fees=None):
    if not buy_price or buy_price <= 0 or not sell_price or sell_price <= 0:
        return None
    if not buy_fees or not sell_fees:
        return None

    buy_fee_rate  = buy_fees['taker']
    sell_fee_rate = sell_fees['taker']
    fee_tokens    = _to_float(fee_tokens)
    min_withdrawal_tokens = _to_float(min_withdrawal_tokens)

    tokens_bought       = capital_usdt / buy_price
    buy_fee_tokens      = tokens_bought * buy_fee_rate
    tokens_after_buy_fee = tokens_bought - buy_fee_tokens

    min_withdrawal_met = (
        tokens_after_buy_fee >= min_withdrawal_tokens
        if min_withdrawal_tokens is not None and min_withdrawal_tokens > 0
        else True
    )

    gas_tokens       = fee_tokens if fee_tokens is not None else 0.0
    tokens_remaining = tokens_after_buy_fee - gas_tokens

    gross_sell_usdt = tokens_remaining * sell_price if tokens_remaining > 0 else 0.0
    sell_fee_usdt   = gross_sell_usdt * sell_fee_rate
    total_received  = gross_sell_usdt - sell_fee_usdt

    total_cost = capital_usdt
    net_pnl    = total_received - total_cost
    roi_pct    = (net_pnl / capital_usdt) * 100 if capital_usdt else 0.0

    buy_fee_usd = buy_fee_tokens * buy_price

    return {
        'capital':                capital_usdt,
        'tokens_bought':          tokens_bought,
        'buy_fees':               buy_fees,
        'buy_fee_rate':           buy_fee_rate,
        'buy_fee_tokens':         buy_fee_tokens,
        'buy_fee_usd':            buy_fee_usd,
        'tokens_after_buy_fee':   tokens_after_buy_fee,
        'min_withdrawal_tokens':  min_withdrawal_tokens,
        'min_withdrawal_met':     min_withdrawal_met,
        'gas_tokens':             gas_tokens,
        'tokens_remaining':       tokens_remaining,
        'sell_fees':              sell_fees,
        'sell_fee_rate':          sell_fee_rate,
        'sell_fee_usdt':          sell_fee_usdt,
        'sell_fee_usd':           sell_fee_usdt,
        'total_cost':             total_cost,
        'total_received':         total_received,
        'net_pnl':                net_pnl,
        'roi_pct':                roi_pct,
    }

def log_profit_block(profit, symbol=None):
    if not profit:
        log.info("       PROFIT not calculated (taker fee unavailable — see warning above)")
        return
    base = symbol.split('/')[0] if symbol else "tokens"
    log.info(f"       PROFIT Capital= {profit['capital']:.0f}USDT")
    log.info(f"       PROFIT Tokens bought= {profit['tokens_bought']:.6f}")
    log.info(
        f"       PROFIT Buy taker fee ({profit['buy_fee_rate'] * 100:.4f}% "
        f"@ {profit['buy_fees']['exchange']})=  -{profit['buy_fee_tokens']:.6f} {base}"
    )
    log.info(f"       PROFIT Tokens after buy fee= {profit['tokens_after_buy_fee']:.6f}")
    if profit['min_withdrawal_tokens'] is not None:
        mark = "✅" if profit['min_withdrawal_met'] else "❌"
        note = "met" if profit['min_withdrawal_met'] else "NOT MET — trade size too small to withdraw"
        log.info(f"       PROFIT Min withdrawal= {profit['min_withdrawal_tokens']:.6f} tokens  [{mark} {note}]")
    log.info(f"       PROFIT Gas deducted= -{profit['gas_tokens']:.6f}")
    log.info(f"       PROFIT Tokens remaining= {profit['tokens_remaining']:.6f}")
    log.info(
        f"       PROFIT Sell taker fee ({profit['sell_fee_rate'] * 100:.4f}% "
        f"@ {profit['sell_fees']['exchange']})=  -{profit['sell_fee_usdt']:.4f} USDT"
    )
    log.info(f"       PROFIT Total cost= {profit['total_cost']:.4f} USDT")
    log.info(f"       PROFIT Total received= {profit['total_received']:.4f} USDT")
    if profit.get('usdt_transfer_network') is not None:
        log.info(
            f"       PROFIT USDT transfer fee ({profit['usdt_transfer_from']} -> {profit['usdt_transfer_to']}"
            f" via {profit['usdt_transfer_network']})=  -{profit['usdt_transfer_fee_usd']:.4f}"
        )
    log.info(f"       PROFIT Net P&L {profit['net_pnl']:+.4f} USDT")
    log.info(f"       PROFIT ROI {profit['roi_pct']:+.2f}%")

TRADE_COINS_REQUIRED_FIELDS = (
    'pair', 'exchange', 'coin_wd_network', 'arrival_time', 'usdt_holder',
    'usdt_transfer_fee', 'usdt_d_address', 'coin_d_address',
    'buy_sell_trading_fee', 'min_withdrawal', 'gas_deducted',
)

def save_trade_coin(r, profit):
    if not r or not profit:
        return

    pair     = r.get('symbol')
    exchange = (
        f"{r['buy_ex']}/{r['sell_ex']}"
        if r.get('buy_ex') and r.get('sell_ex') else None
    )
    coin_wd_network = r.get('withdrawal_network_norm')

    transfer_secs = r.get('transfer_seconds')
    if transfer_secs is None:
        arrival_time = None
    elif r.get('transfer_time_estimated'):
        arrival_time = f"est. ≤{_fmt_duration(transfer_secs)} (assumed)"
    else:
        arrival_time = f"est. {_fmt_duration(transfer_secs)}"

    usdt_holder  = profit.get('usdt_transfer_from')
    usdt_network = profit.get('usdt_transfer_network')
    usdt_fee     = profit.get('usdt_transfer_fee_usd')
    usdt_transfer_fee = (
        f"{usdt_network}/{usdt_fee:.4f}"
        if usdt_network and usdt_fee is not None else None
    )

    usdt_d_address = profit.get('usdt_dest_address')
    coin_d_address = profit.get('coin_dest_address')

    buy_fee  = profit.get('buy_fee_usd')
    sell_fee = profit.get('sell_fee_usd')
    buy_sell_trading_fee = (
        f"{buy_fee:.4f}/{sell_fee:.4f}"
        if buy_fee is not None and sell_fee is not None else None
    )

    min_withdrawal = profit.get('min_withdrawal_tokens')
    gas_deducted   = profit.get('gas_tokens')

    row = {
        'pair':                  pair,
        'exchange':              exchange,
        'coin_wd_network':       coin_wd_network,
        'arrival_time':          arrival_time,
        'usdt_holder':           usdt_holder,
        'usdt_transfer_fee':     usdt_transfer_fee,
        'usdt_d_address':        usdt_d_address,
        'coin_d_address':        coin_d_address,
        'buy_sell_trading_fee':  buy_sell_trading_fee,
        'min_withdrawal':        min_withdrawal,
        'gas_deducted':          gas_deducted,
    }

    missing = [k for k in TRADE_COINS_REQUIRED_FIELDS if row.get(k) is None or row.get(k) == '']
    if missing:
        log.info(f"       SAVE   skipped — missing: {', '.join(missing)}")
        return

    try:
        sb = _get_supabase_cached()
        sb.table("trade_coins").upsert(row, on_conflict="pair").execute()
        log.info(f"       SAVE   ✅ {pair} saved to trade_coins")
    except Exception as e:
        log.warning(f"  WARNING  saving trade_coins ({pair}): {str(e)[:150]}")

def fetch_arb_coins_rows():
    try:
        sb = _get_supabase_cached()
        rows = sb.table("arb_coins").select("*").execute()
        return rows.data or []
    except Exception as e:
        log.warning(f"  WARNING  arb_coins fetch: {str(e)[:200]}")
        return []

def delete_arb_coin(symbol):
    try:
        sb = _get_supabase_cached()
        sb.table("arb_coins").delete().eq("symbol", symbol).execute()
        log.info(f"[worker1] 🗑️  removed {symbol} from arb_coins")
    except Exception as e:
        log.warning(f"  WARNING  arb_coins delete for {symbol}: {str(e)[:200]}")

def load_exchange_config() -> dict:
    try:
        sb = _get_supabase_cached()
        rows = sb.table("exchange_config").select("*").execute()
    except Exception as e:
        log.warning(f"  WARNING  exchange_config fetch: {str(e)[:200]}")
        return {}
    cfg = {}
    for row in rows.data or []:
        name = (row.get("exchange_name") or "").strip().lower()
        if not name:
            continue
        cfg[name] = {
            'usdt_withdrawal_networks':     row.get('usdt_withdrawal_networks') or [],
            'max_withdrawal_fee_usdt':       _to_float(row.get('max_withdrawal_fee_usdt')),
            'requires_withdrawal_whitelist': str(row.get('requires_withdrawal_whitelist')).strip().lower() == 'true',
            'is_disabled':                   str(row.get('is_disabled')).strip().lower() == 'true',
            'buy_only':                      str(row.get('buy_only')).strip().lower() == 'true',
        }
    return cfg

def load_bot_state() -> dict:
    try:
        sb = _get_supabase_cached()
        rows = sb.table("bot_state").select("*").eq("id", 1).execute()
        data = rows.data or []
        return data[0] if data else {}
    except Exception as e:
        log.warning(f"  WARNING  bot_state fetch: {str(e)[:200]}")
        return {}

def load_addresses() -> dict:
    try:
        sb = _get_supabase_cached()
        rows = sb.table("Addresses").select("*").execute()
    except Exception as e:
        log.warning(f"  WARNING  Addresses fetch: {str(e)[:200]}")
        return {}
    out = {}
    for row in rows.data or []:
        name = row.get("exchange")
        if not name:
            continue
        out[name] = {
            'evm':                 str(row.get('evm')).strip().lower() == 'true',
            'universal':           str(row.get('universal')).strip().lower() == 'true',
            'whitelisted_network': row.get('whitelisted_network') or [],
            'ETH': row.get('ETH'),
            'BSC': row.get('BSC'),
            'SOL': row.get('SOL'),
            'TON': row.get('TON'),
            'APTOS': row.get('APTOS'),
            'PLASMA': row.get('PLASMA'),
            'ROBINHOOD': row.get('ROBINHOOD'),
        }
    return out

EVM_NETWORKS = {'ETH', 'BSC', 'MATIC', 'ARB', 'OP', 'BASE', 'CELO', 'PLASMA'}

ADDRESS_COLUMN_FOR_NETWORK = {
    'ETH':    'ETH',
    'BSC':    'BSC',
    'TRX':    'TRC20',
    'MATIC':  'POLYGON',
    'ARB':    'ARBITRUM',
    'OP':     'OPTIMISM',
    'SOL':    'SOL',
    'AVAX':   'AVAX',
    'BTC':    'BTC',
    'BASE':   'BASE',
    'TON':    'TON',
    'APT':    'APTOS',
    'SUI':    'SUI',
    'XRP':    'XRP',
    'DOGE':   'DOGE',
    'LTC':    'LTC',
    'PLASMA': 'PLASMA',
    'CELO':   'CELO',
    'ROBINHOOD': 'ROBINHOOD',
}

def is_network_whitelisted(addresses_cfg, exchange_name, network_code):
    row = addresses_cfg.get(exchange_name)
    if row is None:
        return None
    norm = normalize_network(network_code)
    if row['evm'] and norm in EVM_NETWORKS:
        return True
    whitelisted_norm = {normalize_network(n) for n in row['whitelisted_network']}
    return norm in whitelisted_norm

def _resolve_destination_column(sender_row, destination_row, network_norm):
    if destination_row is None:
        return None
    sender_evm = bool((sender_row or {}).get('evm', False))
    if sender_evm and network_norm in EVM_NETWORKS:
        return 'ETH'
    return ADDRESS_COLUMN_FOR_NETWORK.get(network_norm)

def validate_withdrawal_whitelist(sender_ex, destination_ex, network_norm, exchange_cfg, addresses):
    sender_cfg = exchange_cfg.get(sender_ex.lower()) or {}
    if not sender_cfg.get('requires_withdrawal_whitelist'):
        return True, f"{sender_ex} does not require a withdrawal whitelist"

    sender_row = addresses.get(sender_ex)
    if not sender_row:
        return False, f"{sender_ex} has no row in the Addresses table (required because whitelist is on)"

    dest_row = addresses.get(destination_ex)
    if not dest_row:
        return False, f"{destination_ex} has no row in the Addresses table"

    is_evm_network   = network_norm in EVM_NETWORKS
    sender_evm       = bool(sender_row.get('evm', False))
    sender_universal = bool(sender_row.get('universal', False))
    whitelisted_norm = {normalize_network(n) for n in (sender_row.get('whitelisted_network') or [])}

    if sender_evm and is_evm_network:
        dest_col = 'ETH'
        if not dest_row.get(dest_col):
            return False, (
                f"{sender_ex} treats {network_norm} as EVM-compatible but "
                f"{destination_ex} has no {dest_col} address"
            )
        return True, (
            f"{sender_ex} EVM compatibility accepts {network_norm} "
            f"(destination uses {dest_col})"
        )

    if sender_universal:
        if network_norm not in whitelisted_norm:
            return False, f"{network_norm} is not whitelisted on {sender_ex}"
        dest_col = ADDRESS_COLUMN_FOR_NETWORK.get(network_norm)
        if not dest_col:
            return False, f"no Addresses column mapped for network '{network_norm}'"
        if not dest_row.get(dest_col):
            return False, (
                f"{destination_ex} has no {dest_col} address for network {network_norm}"
            )
        return True, (
            f"{network_norm} is whitelisted on {sender_ex} "
            f"(destination uses {dest_col})"
        )

    if network_norm not in whitelisted_norm:
        return False, f"{network_norm} is not whitelisted on {sender_ex}"
    dest_col = ADDRESS_COLUMN_FOR_NETWORK.get(network_norm)
    if not dest_col:
        return False, f"no Addresses column mapped for network '{network_norm}'"
    if not dest_row.get(dest_col):
        return False, (
            f"{destination_ex} has no {dest_col} address for network {network_norm}"
        )
    return True, (
        f"{network_norm} is whitelisted on {sender_ex} "
        f"(strict, destination uses {dest_col})"
    )

def get_usdt_network_data(exchange_name):
    currencies = get_currencies(exchange_name)
    usdt_cur = currencies.get('USDT') or {}
    networks = {
        code: dict(data) for code, data in (usdt_cur.get('networks') or {}).items()
        if isinstance(data, dict)
    }

    fallback = _fallback_networks(exchange_name, 'USDT')
    for code, fb_data in fallback.items():
        if not isinstance(fb_data, dict):
            continue
        existing = networks.get(code)
        if existing is None:
            networks[code] = dict(fb_data)
            continue
        if existing.get('fee') is None and fb_data.get('fee') is not None:
            existing['fee'] = fb_data['fee']
        if existing.get('withdraw') is None and fb_data.get('withdraw') is not None:
            existing['withdraw'] = fb_data['withdraw']
        if existing.get('deposit') is None and fb_data.get('deposit') is not None:
            existing['deposit'] = fb_data['deposit']
        if not existing.get('limits') and fb_data.get('limits'):
            existing['limits'] = fb_data['limits']

    return networks

def _networks_by_norm(networks_raw, enabled_key):
    by_norm = defaultdict(list)
    for code, data in networks_raw.items():
        if not isinstance(data, dict):
            continue
        enabled = data.get(enabled_key, True) is not False and data.get('active', True) is not False
        if enabled:
            by_norm[normalize_network(code)].append((code, data))
    return by_norm

def plan_usdt_transfer(holder_ex, buy_ex, exchange_cfg):
    if holder_ex == buy_ex:
        return {'ok': True, 'network': None, 'fee_usdt': 0.0}

    holder_cfg = exchange_cfg.get(holder_ex.lower())
    buy_cfg    = exchange_cfg.get(buy_ex.lower())
    if not holder_cfg:
        return {'ok': False, 'reason': f"{holder_ex} (holds USDT) has no exchange_config row"}
    if not buy_cfg:
        return {'ok': False, 'reason': f"{buy_ex} (buy side) has no exchange_config row"}

    configured_source_networks = {normalize_network(n) for n in holder_cfg['usdt_withdrawal_networks']}
    configured_source_networks.discard(None)
    if not configured_source_networks:
        return {'ok': False, 'reason': f"{holder_ex} has no usdt_withdrawal_networks configured"}

    source_networks_raw = get_usdt_network_data(holder_ex)
    if not source_networks_raw:
        return {'ok': False, 'reason': f"{holder_ex}: could not retrieve USDT network data from the exchange API"}
    source_withdraw_by_norm = _networks_by_norm(source_networks_raw, 'withdraw')
    live_source_networks = set(source_withdraw_by_norm.keys())

    valid_source_networks = configured_source_networks & live_source_networks
    if not valid_source_networks:
        return {
            'ok': False,
            'reason': (
                f"none of {holder_ex}'s configured USDT withdrawal networks "
                f"({sorted(n for n in configured_source_networks if n)}) are confirmed "
                f"live/withdrawable by the {holder_ex} API "
                f"(API reports withdrawable: {sorted(n for n in live_source_networks if n)})"
            ),
        }

    dest_networks_raw = get_usdt_network_data(buy_ex)
    if not dest_networks_raw:
        return {'ok': False, 'reason': f"{buy_ex}: could not retrieve USDT network data from the exchange API"}
    dest_deposit_by_norm = _networks_by_norm(dest_networks_raw, 'deposit')
    live_dest_networks = set(dest_deposit_by_norm.keys())

    common = valid_source_networks & live_dest_networks
    if not common:
        return {
            'ok': False,
            'reason': (
                f"no common live USDT network between {holder_ex} (confirmed withdrawable: "
                f"{sorted(n for n in valid_source_networks if n)}) and {buy_ex} (confirmed "
                f"deposit-enabled: {sorted(n for n in live_dest_networks if n)})"
            ),
        }

    candidates = []
    for norm_key in common:
        for code, data in source_withdraw_by_norm.get(norm_key, []):
            fee = _to_float(data.get('fee'))
            if fee is None:
                continue
            candidates.append((norm_key, code, fee))

    if not candidates:
        return {
            'ok': False,
            'reason': (
                f"{holder_ex} reports no withdrawal fee data for any of the shared live "
                f"networks {sorted(common)}"
            ),
        }

    max_fee = holder_cfg.get('max_withdrawal_fee_usdt')
    within_cap = [c for c in candidates if max_fee is None or c[2] <= max_fee]
    if not within_cap:
        cheapest = min(candidates, key=lambda c: c[2])
        checked = ", ".join(f"{code}=${fee:.4f}" for _, code, fee in sorted(candidates, key=lambda c: c[2]))
        return {
            'ok': False,
            'reason': (
                f"cheapest of {len(candidates)} verified shared USDT network(s) on {holder_ex} "
                f"({cheapest[1]}) costs ${cheapest[2]:.4f}, over its max_withdrawal_fee_usdt cap "
                f"of ${max_fee}  [checked: {checked}]"
            ),
        }

    _, network_code, fee = min(within_cap, key=lambda c: c[2])
    return {'ok': True, 'network': network_code, 'fee_usdt': fee}

EXTRA_PARAMS = {'Bybit': {'category': 'spot'}}

def fetch_ticker_data(exchange_name, symbol):
    ex = ensure_exchange(exchange_name)
    if ex is None:
        return None
    try:
        params = EXTRA_PARAMS.get(exchange_name, {})
        t = with_retries(lambda: ex.fetch_ticker(symbol, params=params), exchange_name)
    except Exception as e:
        log.warning(f"  WARNING  {exchange_name} fetch_ticker({symbol}): {str(e)[:300]}")
        return None
    last   = t.get('last',        0) or 0
    bid    = t.get('bid',         0) or 0
    ask    = t.get('ask',         0) or 0
    volume = t.get('quoteVolume', 0) or 0
    change = t.get('percentage',  0) or 0
    if bid <= 0: bid = last
    if ask <= 0: ask = last
    return {'price': last, 'bid': bid, 'ask': ask, 'volume': volume, 'change': change}

def fetch_tickers_grouped(needed):
    results = {}
    if not needed:
        return results

    def _worker(exchange_name, symbols):
        local = {}
        for symbol in sorted(symbols):
            local[(exchange_name, symbol)] = fetch_ticker_data(exchange_name, symbol)
        return local

    with ThreadPoolExecutor(max_workers=len(needed)) as pool:
        futures = {
            pool.submit(_worker, exchange_name, symbols): exchange_name
            for exchange_name, symbols in needed.items()
        }
        for future in as_completed(futures):
            exchange_name = futures[future]
            try:
                results.update(future.result())
            except Exception as e:
                log.warning(f"  WARNING  batched ticker fetch for {exchange_name}: {str(e)[:200]}")
    return results

def _seed_result(symbol, buy_ex, buy_data, sell_ex, sell_data):
    buy_price, sell_price = buy_data['ask'], sell_data['bid']
    gap_pct = ((sell_price - buy_price) / buy_price) * 100 if buy_price else 0.0
    return {
        'symbol':     symbol,
        'buy_ex':     buy_ex,
        'buy_price':  buy_price,
        'buy_vol':    buy_data['volume'],
        'buy_chg':    buy_data['change'],
        'sell_ex':    sell_ex,
        'sell_price': sell_price,
        'sell_vol':   sell_data['volume'],
        'sell_chg':   sell_data['change'],
        'gap_pct':    gap_pct,
        'n_exchanges': 2,
        'all_prices': {
            buy_ex:  {'price': buy_data['price'],  'bid': buy_data['bid'],  'ask': buy_data['ask']},
            sell_ex: {'price': sell_data['price'], 'bid': sell_data['bid'], 'ask': sell_data['ask']},
        },
        'depth_checked':  False,
        'depth_ok':       None,
        'buy_depth_price':  None,
        'sell_depth_price': None,
        'depth_gap_pct':  None,
        'meta_checked':     False,
        'buy_name':         None,
        'sell_name':        None,
        'name_match':       None,
        'name_suspect':     False,
        'common_networks':  [],
        'blocked_networks': [],
        'blocked_details':  [],
        'alt_route':        None,
        'confirmed_pair':   None,
        'networks_known':   False,
        'withdrawal_network':    None,
        'withdrawal_network_norm': None,
        'withdrawal_fee':        None,
        'withdrawal_min_tokens': None,
        'transfer_withdraw_seconds': None,
        'transfer_deposit_seconds':  None,
        'transfer_seconds':          None,
        'transfer_time_estimated':   False,
        'transfer_time_desc':        None,
        'contract_buy':     None,
        'contract_sell':    None,
        'contract_match':   None,
        'verified':         False,
        'buy_meta_summary':  None,
        'sell_meta_summary': None,
        'extra_meta_summaries': [],
        'corrected_symbol':       None,
        'corrected_sell_price':   None,
        'corrected_gap_pct':      None,
        'corrected_depth_gap_pct': None,
        'corrected_sell_depth_price': None,
    }

def _check_transfer_time(r):
    seconds = r.get('transfer_seconds')
    if seconds is None or seconds <= MAX_TRANSFER_TIME_SEC:
        return None
    return {
        'ok': False,
        'reason': (
            f"estimated deposit/arrival time too slow ({r.get('transfer_time_desc')}, "
            f"~{_fmt_duration(seconds)}) — exceeds {MAX_TRANSFER_TIME_SEC // 60}min limit"
        ),
        'r': r, 'profit': None, 'delete': True,
    }

def verify_pair_light(symbol, buy_ex_name, sell_ex_name, buy_data, sell_data):
    try:
        if not buy_data or not sell_data:
            return {'ok': False, 'reason': 'could not fetch fresh prices from one or both exchanges', 'r': None, 'profit': None}
        if buy_data['ask'] <= 0 or sell_data['bid'] <= 0:
            return {'ok': False, 'reason': 'invalid ask/bid price data', 'r': None, 'profit': None}

        r = _seed_result(symbol, buy_ex_name, buy_data, sell_ex_name, sell_data)
        check_depth(r)
        check_metadata(r)

        if r['verified'] is not True:
            return {'ok': False, 'reason': f"transfer network no longer verified (verified={r['verified']})", 'r': r, 'profit': None}

        slow = _check_transfer_time(r)
        if slow:
            return slow

        buy_fees  = get_taker_fee(buy_ex_name,  symbol)
        sell_fees = get_taker_fee(sell_ex_name, symbol)
        if not buy_fees or not sell_fees:
            return {'ok': False, 'reason': 'taker fee unavailable (see warnings)', 'r': r, 'profit': None}

        profit = calc_arb_profit(
            CAPITAL_USDT, r['buy_price'], r['sell_price'],
            fee_tokens=r['withdrawal_fee'],
            min_withdrawal_tokens=r['withdrawal_min_tokens'],
            buy_fees=buy_fees,
            sell_fees=sell_fees,
        )
        if not profit:
            return {'ok': False, 'reason': 'profit calc failed (bad price data)', 'r': r, 'profit': None}

        ok = profit['net_pnl'] >= MIN_STORE_PROFIT_USDT
        reason = None if ok else f"profit {profit['net_pnl']:+.4f} USDT below ${MIN_STORE_PROFIT_USDT} threshold"
        return {'ok': ok, 'reason': reason, 'r': r, 'profit': profit}
    except Exception as e:
        log.warning(f"  WARNING  light verify({symbol}, {buy_ex_name}->{sell_ex_name}): {type(e).__name__}: {str(e)[:300]}")
        return {'ok': False, 'reason': f"unexpected error: {str(e)[:150]}", 'r': None, 'profit': None}

def _verify_full(symbol, buy_ex_name, sell_ex_name, buy_data, sell_data,
                 exchange_cfg, addresses, holder_ex):
    try:
        if not buy_data or not sell_data:
            return {'ok': False, 'reason': 'could not fetch fresh prices from one or both exchanges', 'r': None, 'profit': None}
        if buy_data['ask'] <= 0 or sell_data['bid'] <= 0:
            return {'ok': False, 'reason': 'invalid ask/bid price data', 'r': None, 'profit': None}

        for ex in (buy_ex_name, sell_ex_name):
            if ex.lower() not in exchange_cfg:
                return {
                    'ok': False,
                    'reason': f"{ex} has no exchange_config row — refusing to verify",
                    'r': None, 'profit': None, 'delete': True,
                }

        r = _seed_result(symbol, buy_ex_name, buy_data, sell_ex_name, sell_data)
        check_depth(r)
        check_metadata(r)

        if r['verified'] is not True:
            return {'ok': False, 'reason': f"transfer network no longer verified (verified={r['verified']})", 'r': r, 'profit': None}

        slow = _check_transfer_time(r)
        if slow:
            return slow

        if not holder_ex:
            return {'ok': False, 'reason': 'bot_state.holds_usdt is not set — cannot validate the USDT transfer', 'r': r, 'profit': None}
        usdt_plan = plan_usdt_transfer(holder_ex, buy_ex_name, exchange_cfg)
        if not usdt_plan['ok']:
            return {
                'ok': False,
                'reason': usdt_plan['reason'],
                'r': r,
                'profit': None,
                'delete': True,
            }

        if usdt_plan['network'] is not None:
            ok1, reason1 = validate_withdrawal_whitelist(
                holder_ex, buy_ex_name, normalize_network(usdt_plan['network']), exchange_cfg, addresses
            )
            if not ok1:
                return {
                    'ok': False,
                    'reason': f"USDT transfer ({holder_ex} -> {buy_ex_name}) whitelist check failed: {reason1}",
                    'r': r, 'profit': None, 'delete': True,
                }

        network_norm = r.get('withdrawal_network_norm')
        if network_norm:
            ok2, reason2 = validate_withdrawal_whitelist(buy_ex_name, sell_ex_name, network_norm, exchange_cfg, addresses)
            if not ok2:
                return {
                    'ok': False,
                    'reason': f"{symbol} transfer ({buy_ex_name} -> {sell_ex_name}) whitelist check failed: {reason2}",
                    'r': r, 'profit': None, 'delete': True,
                }

        buy_fees  = get_taker_fee(buy_ex_name,  symbol)
        sell_fees = get_taker_fee(sell_ex_name, symbol)
        if not buy_fees or not sell_fees:
            return {'ok': False, 'reason': 'taker fee unavailable (see warnings)', 'r': r, 'profit': None}

        profit = calc_arb_profit(
            CAPITAL_USDT, r['buy_price'], r['sell_price'],
            fee_tokens=r['withdrawal_fee'],
            min_withdrawal_tokens=r['withdrawal_min_tokens'],
            buy_fees=buy_fees,
            sell_fees=sell_fees,
        )
        if not profit:
            return {'ok': False, 'reason': 'profit calc failed (bad price data)', 'r': r, 'profit': None}

        if usdt_plan and usdt_plan['network'] is not None:
            profit['usdt_transfer_fee_usd'] = usdt_plan['fee_usdt']
            profit['usdt_transfer_network'] = usdt_plan['network']
            profit['usdt_transfer_from']    = holder_ex
            profit['usdt_transfer_to']      = buy_ex_name
            profit['net_pnl']  -= usdt_plan['fee_usdt']
            profit['roi_pct']   = (profit['net_pnl'] / profit['capital']) * 100 if profit['capital'] else 0.0
        else:
            profit['usdt_transfer_fee_usd'] = 0.0
            profit['usdt_transfer_network'] = None
            profit['usdt_transfer_from']    = holder_ex
            profit['usdt_transfer_to']      = buy_ex_name

        if usdt_plan['network']:
            buy_addresses   = addresses.get(buy_ex_name)
            sender_row_usdt = addresses.get(holder_ex)
            if buy_addresses:
                usdt_net_norm = normalize_network(usdt_plan['network'])
                col = _resolve_destination_column(sender_row_usdt, buy_addresses, usdt_net_norm)
                if col:
                    profit['usdt_dest_address'] = buy_addresses.get(col)

        if network_norm:
            sell_addresses  = addresses.get(sell_ex_name)
            sender_row_coin = addresses.get(buy_ex_name)
            if sell_addresses:
                col = _resolve_destination_column(sender_row_coin, sell_addresses, network_norm)
                if col:
                    profit['coin_dest_address'] = sell_addresses.get(col)

        if usdt_plan['network'] is not None and not profit.get('usdt_dest_address'):
            return {
                'ok': False,
                'reason': (
                    f"no USDT destination address on {buy_ex_name} for network "
                    f"{normalize_network(usdt_plan['network'])} — dropping pair"
                ),
                'r': r, 'profit': None, 'delete': True,
            }
        if network_norm and not profit.get('coin_dest_address'):
            base = symbol.split('/')[0]
            return {
                'ok': False,
                'reason': (
                    f"no {base} deposit address on {sell_ex_name} for network "
                    f"{network_norm} — dropping pair"
                ),
                'r': r, 'profit': None, 'delete': True,
            }

        ok = profit['net_pnl'] >= MIN_STORE_PROFIT_USDT
        reason = None if ok else f"profit {profit['net_pnl']:+.4f} USDT below ${MIN_STORE_PROFIT_USDT} threshold"
        return {'ok': ok, 'reason': reason, 'r': r, 'profit': profit, 'delete': False}
    except Exception as e:
        log.warning(f"  WARNING  full verify({symbol}, {buy_ex_name}->{sell_ex_name}): {type(e).__name__}: {str(e)[:300]}")
        return {'ok': False, 'reason': f"unexpected error: {str(e)[:150]}", 'r': None, 'profit': None}

def reverify_pair(symbol, buy_ex_name, sell_ex_name):
    old_mode = get_exchange_mode()
    set_exchange_mode('trader')
    try:
        buy_data  = fetch_ticker_data(buy_ex_name,  symbol)
        sell_data = fetch_ticker_data(sell_ex_name, symbol)
        exchange_cfg = load_exchange_config()
        addresses    = load_addresses()
        holder_ex    = load_bot_state().get('holds_usdt')
        return _verify_full(symbol, buy_ex_name, sell_ex_name, buy_data, sell_data,
                            exchange_cfg, addresses, holder_ex)
    finally:
        set_exchange_mode(old_mode)

def print_pair_report_full(r, profit):
    log.info(f"{r['symbol']}  |  gap {r['gap_pct']:.2f}%  |  {r['n_exchanges']} exchanges")
    log.info(f"       BUY   {r['buy_ex']:<10}  ask {fmt_price(r['buy_price'])}  vol {fmt_vol(r['buy_vol'])}  24h {r['buy_chg']:+.1f}%")
    log.info(f"       SELL  {r['sell_ex']:<10}  bid {fmt_price(r['sell_price'])}  vol {fmt_vol(r['sell_vol'])}  24h {r['sell_chg']:+.1f}%")
    prices = "  ".join(f"{ex}:{fmt_price(p['price'])}" for ex, p in sorted(r['all_prices'].items()))
    log.info(f"       ALL   {prices}")
    if r['depth_checked']:
        if r['buy_depth_price'] is None:
            log.info(f"       DEPTH  insufficient liquidity to verify — SKIP")
        else:
            status = "OK" if r['depth_ok'] else f"THIN (book ran out before ${DEPTH_CHECK_USDT} filled)"
            log.info(
                f"       DEPTH  buy avg {fmt_price(r['buy_depth_price'])}  "
                f"sell avg {fmt_price(r['sell_depth_price'])}  "
                f"real gap {r['depth_gap_pct']:.2f}%  [{status}]"
            )
    else:
        log.info(f"       DEPTH  not checked (exchange unavailable)")

    if r['corrected_symbol']:
        depth_note = (
            f"  |  depth-adjusted: {r['corrected_depth_gap_pct']:.2f}%"
            if r['corrected_depth_gap_pct'] is not None else ""
        )
        log.info(
            f"       NOTE   Ticker collision resolved by name — '{r['buy_name']}' "
            f"actually trades as {r['corrected_symbol']} on {r['sell_ex']}. "
            f"Re-priced sell: {fmt_price(r['corrected_sell_price'])}  "
            f"corrected gap: {r['corrected_gap_pct']:.2f}%{depth_note}"
        )

    if r['verified'] is True:
        basis = "contract address match" if r['contract_match'] is True else f"name '{r['buy_name']}' on both"
        min_wd = r['withdrawal_min_tokens']
        log.info(
            f"       VERIFY  ✅ confirmed via {basis}  |  "
            f"network {r['withdrawal_network']}  fee {r['withdrawal_fee']}  "
            f"min_withdraw {min_wd if min_wd is not None else 'unknown'}"
        )
        if r.get('transfer_seconds') is not None:
            if r.get('transfer_time_estimated'):
                log.info(f"       ARRIVAL  ⚠️  time unknown — default to 15mins")
            else:
                mark = "✅" if r['transfer_seconds'] <= MAX_TRANSFER_TIME_SEC else "❌"
                log.info(
                    f"       ARRIVAL {mark} est. {_fmt_duration(r['transfer_seconds'])}  "
                    f"({r['transfer_time_desc']})"
                )
        else:
            log.info(f"       ARRIVAL  unknown (exchange returned no time/confirmation data)")

        if profit and profit.get('usdt_transfer_from'):
            if profit.get('usdt_transfer_network') is None:
                log.info(f"       USDT   already on {profit['usdt_transfer_from']} — no transfer needed")
            else:
                dest_addr = profit.get('usdt_dest_address', 'N/A')
                log.info(
                    f"       USDT HOLDER :  {profit['usdt_transfer_from']} -> {profit['usdt_transfer_to']} "
                    f"via {profit['usdt_transfer_network']}  fee ${profit['usdt_transfer_fee_usd']:.4f}"
                )
                log.info(f"               destination address: {dest_addr}")

        if profit and profit.get('coin_dest_address'):
            log.info(f"       COIN   deposit address on {r['sell_ex']}: {profit['coin_dest_address']}")

        if profit:
            log_profit_block(profit, r['symbol'])

        save_trade_coin(r, profit)
    elif r['verified'] is False and r.get('alt_route'):
        ar = r['alt_route']
        log.info(f"       VERIFY  ✅ Same token existed on {r['n_exchanges']} exchanges.")
        log.info(f"       VERIFY  ❌ {ar['blocked_reason']}")
        if ar['direction'] == 'alt_buy':
            log.info(
                f"       VERIFY  ✅ {ar['alt_ex']} withdraw allowed in {ar['network']}  |  "
                f"ask {fmt_price(ar['alt_price'])}  |  gap {ar['gap_pct']:.2f}%"
            )
            log.info(f"       VERIFY  📉 Tradable at {ar['alt_ex']} → {ar['sell_ex']} | {ar['network']} 🚀")
        else:
            log.info(
                f"       VERIFY  ✅ {ar['alt_ex']} deposit allowed in {ar['network']}  |  "
                f"bid {fmt_price(ar['alt_price'])}  |  gap {ar['gap_pct']:.2f}%"
            )
            log.info(f"       VERIFY  📉 Tradable at {r['buy_ex']} → {ar['alt_ex']} | {ar['network']} 🚀")
    else:
        log.info(f"       VERIFY  ❌ transfer network no longer valid (verified={r['verified']})")

    if r['buy_meta_summary']:
        log.info(f"       META   {r['buy_meta_summary']}")
    if r['sell_meta_summary']:
        log.info(f"       META   {r['sell_meta_summary']}")
    for extra_summary in r.get('extra_meta_summaries') or []:
        log.info(f"       META   {extra_summary}")
    log.info("")

class ActiveTrade:
    def __init__(self):
        self._lock = threading.Lock()
        self._active = None
        self._changed = threading.Event()

    def try_assign(self, candidate):
        with self._lock:
            current = self._active
            if current is None or candidate['profit_usdt'] > current['profit_usdt']:
                self._active = candidate
                self._changed.set()
                return True, current
            return False, current

    def get(self):
        with self._lock:
            return dict(self._active) if self._active else None

    def wait_for_change(self):
        self._changed.wait()
        with self._lock:
            self._changed.clear()

    def clear_if_matches(self, symbol):
        with self._lock:
            if self._active and self._active['symbol'] == symbol:
                self._active = None

active_trade = ActiveTrade()
fail_streaks = defaultdict(int)

def worker1_loop():
    log.info(f"[worker1] started — scanning arb_coins every {WORKER1_INTERVAL_SEC}s (scanner mode)")
    set_exchange_mode('scanner')
    while True:
        try:
            rows = fetch_arb_coins_rows()
            if not rows:
                log.info("[worker1] arb_coins table is empty — nothing to scan")
                time.sleep(WORKER1_INTERVAL_SEC)
                continue

            exchange_cfg = load_exchange_config()
            rows_to_keep = []
            for row in rows:
                symbol = row.get('symbol')
                buy_ex = row.get('buy_exchange')
                sell_ex = row.get('sell_exchange')
                if not (symbol and buy_ex and sell_ex):
                    continue

                buy_cfg  = exchange_cfg.get(buy_ex.lower())
                sell_cfg = exchange_cfg.get(sell_ex.lower())

                if buy_cfg is None or sell_cfg is None:
                    missing = buy_ex if buy_cfg is None else sell_ex
                    log.info(f"[worker1] 🗑️  {symbol}: {missing} has no exchange_config row — removing")
                    delete_arb_coin(symbol)
                    active_trade.clear_if_matches(symbol)
                    fail_streaks.pop(symbol, None)
                    continue

                if buy_cfg.get('is_disabled') or sell_cfg.get('is_disabled'):
                    disabled = buy_ex if buy_cfg.get('is_disabled') else sell_ex
                    log.info(f"[worker1] 🗑️  {symbol}: {disabled} is disabled in exchange_config — removing")
                    delete_arb_coin(symbol)
                    active_trade.clear_if_matches(symbol)
                    fail_streaks.pop(symbol, None)
                    continue
                if sell_cfg.get('buy_only'):
                    log.info(f"[worker1] 🗑️  {symbol}: {sell_ex} is buy-only but used as sell side — removing")
                    delete_arb_coin(symbol)
                    active_trade.clear_if_matches(symbol)
                    fail_streaks.pop(symbol, None)
                    continue
                rows_to_keep.append(row)

            if not rows_to_keep:
                time.sleep(WORKER1_INTERVAL_SEC)
                continue

            needed = defaultdict(set)
            for row in rows_to_keep:
                symbol, buy_ex, sell_ex = row.get('symbol'), row.get('buy_exchange'), row.get('sell_exchange')
                needed[buy_ex].add(symbol)
                needed[sell_ex].add(symbol)
            ticker_map = fetch_tickers_grouped(needed)

            outcomes = []
            with ThreadPoolExecutor(max_workers=min(len(rows_to_keep), ROW_PROCESS_MAX_WORKERS)) as pool:
                futures = {}
                for row in rows_to_keep:
                    symbol = row['symbol']
                    buy_ex = row['buy_exchange']
                    sell_ex = row['sell_exchange']
                    buy_data = ticker_map.get((buy_ex, symbol))
                    sell_data = ticker_map.get((sell_ex, symbol))
                    futures[pool.submit(verify_pair_light, symbol, buy_ex, sell_ex, buy_data, sell_data)] = row
                for future in as_completed(futures):
                    row = futures[future]
                    try:
                        result = future.result()
                    except Exception as e:
                        log.error(f"[worker1] row processing error for {row.get('symbol')}: {e}")
                        result = None
                    if result:
                        outcomes.append((row.get('symbol'), row.get('buy_exchange'), row.get('sell_exchange'), result))

            for symbol, buy_ex, sell_ex, check in outcomes:
                if check['ok']:
                    fail_streaks.pop(symbol, None)
                    profit_usdt = check['profit']['net_pnl']
                    candidate = {
                        'symbol': symbol,
                        'buy_ex': buy_ex,
                        'sell_ex': sell_ex,
                        'profit_usdt': profit_usdt,
                    }
                    assigned, previous = active_trade.try_assign(candidate)
                    if assigned:
                        prev_desc = (
                            f"(replacing {previous['symbol']} @ ${previous['profit_usdt']:+.4f})"
                            if previous and previous['symbol'] != symbol else
                            "" if previous else "(worker2 was idle)"
                        )
                        log.info(f"[worker1] ✅ {symbol}: profit ${profit_usdt:+.4f} — handed to worker2 {prev_desc}".rstrip())
                    else:
                        log.info(f"[worker1]    {symbol}: profit ${profit_usdt:+.4f} — not better than worker2's current pick")
                elif check.get('delete'):
                    log.info(f"[worker1] 🗑️  {symbol}: {check['reason']} — removing immediately")
                    delete_arb_coin(symbol)
                    fail_streaks.pop(symbol, None)
                    active_trade.clear_if_matches(symbol)
                else:
                    fail_streaks[symbol] += 1
                    streak = fail_streaks[symbol]
                    log.info(f"[worker1] ⚠️  {symbol}: {check['reason']}  (streak {streak}/{FAIL_STREAK_LIMIT})")
                    if streak >= FAIL_STREAK_LIMIT:
                        delete_arb_coin(symbol)
                        fail_streaks.pop(symbol, None)
                        active_trade.clear_if_matches(symbol)

        except Exception as e:
            log.error(f"[worker1] unexpected error: {e}")
        time.sleep(WORKER1_INTERVAL_SEC)

def worker2_loop():
    log.info("[worker2] started — waits idle, runs once each time worker1 hands off a new pair (trader mode)")

    set_exchange_mode('trader')
    while True:
        try:
            active_trade.wait_for_change()
            active = active_trade.get()
            if not active:
                continue
            log.info(f"[worker2] now watching {active['symbol']} ({active['buy_ex']} -> {active['sell_ex']})")
            check = reverify_pair(active['symbol'], active['buy_ex'], active['sell_ex'])
            if not check['ok']:
                log.info(f"[worker2] {active['symbol']}: {check['reason']}")
                if check.get('delete'):
                    log.info(f"[worker2] 🗑️  {active['symbol']}: removing from arb_coins (confirmed whitelist rejection)")
                    delete_arb_coin(active['symbol'])
                    fail_streaks.pop(active['symbol'], None)
                    active_trade.clear_if_matches(active['symbol'])
                continue

            print_pair_report_full(check['r'], check['profit'])

        except Exception as e:
            log.error(f"[worker2] unexpected error: {e}")

def main():
    log.info("trader.py — Stage 2: worker1 (lightweight scanner) + worker2 (full verifier)")
    log.info(f"worker1 interval: {WORKER1_INTERVAL_SEC}s  |  worker2: event-driven")
    log.info(f"Min qualifying profit: ${MIN_STORE_PROFIT_USDT}  |  fail-streak limit: {FAIL_STREAK_LIMIT}")
    log.info(f"Capital: {CAPITAL_USDT} USDT  |  depth check: {DEPTH_CHECK_USDT} USDT")
    log.info(
        f"Transfer time policy: reject only when exchange explicitly reports > "
        f"{MAX_TRANSFER_TIME_SEC // 60}min; unknown times assume the ceiling and pass"
    )
    log.info("Scanner mode: uses general API keys, proxies Bybit+KuCoin via arb-bot")
    log.info("Trader mode: uses restricted API keys, proxies via trade-proxy")
    log.info("Taker fees: fetched live per pair (no hardcoded fallback)")
    log.info("")

    t1 = threading.Thread(target=worker1_loop, name="worker1", daemon=True)
    t2 = threading.Thread(target=worker2_loop, name="worker2", daemon=True)
    t1.start()
    t2.start()

    try:
        while True:
            time.sleep(3600)
    except KeyboardInterrupt:
        log.info("trader.py stopped.")

if __name__ == "__main__":
    main()