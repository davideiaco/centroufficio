# =============================================================================
# VERSIONE VERIFICATA - sconti percentuali DBF
# - SCONTO letto come percentuale, anche con virgola: 5,04 / 5,04%
# - Calcolo con Decimal, senza float
# - Prezzo finale troncato a 2 decimali con ROUND_DOWN, non arrotondato
#   Esempio: 17.90 con 5% = 17.005 -> 17.00
# =============================================================================
from dotenv import load_dotenv
from pathlib import Path
import sys
import os
import re
import json
import struct
import datetime
import requests
import time
import logging
from logging.handlers import RotatingFileHandler
from typing import Any, Dict, List, Optional, Tuple, Iterable
from decimal import Decimal, InvalidOperation, ROUND_DOWN


# =============================================================================
# PATHS APP: compatibili sia con .py che con .exe
# =============================================================================

def get_app_dir() -> Path:
    """
    Restituisce la cartella applicazione:
    - se script Python: cartella del file .py
    - se eseguibile PyInstaller: cartella del file .exe
    """
    if getattr(sys, "frozen", False):
        return Path(sys.executable).resolve().parent
    return Path(__file__).resolve().parent


APP_DIR = get_app_dir()
ENV_PATH = APP_DIR / "config.env"
load_dotenv(dotenv_path=ENV_PATH)


# =============================================================================
# LOGGING
# =============================================================================

SCRIPT_DIR = str(APP_DIR)
LOG_PATH = str(APP_DIR / "applica_sconti_bulk.log")

logger = logging.getLogger("applica_sconti_bulk")
logger.setLevel(logging.INFO)
logger.propagate = False

_fmt = logging.Formatter("%(asctime)s | %(levelname)s | %(message)s")

if not logger.handlers:
    _file = RotatingFileHandler(
        LOG_PATH,
        maxBytes=5_000_000,
        backupCount=3,
        encoding="utf-8",
    )
    _file.setLevel(logging.INFO)
    _file.setFormatter(_fmt)

    _console = logging.StreamHandler()
    _console.setLevel(logging.INFO)
    _console.setFormatter(_fmt)

    logger.addHandler(_file)
    logger.addHandler(_console)


def log_error(msg: str, details: Any = None, *, max_chars: int = 2500) -> None:
    if details is None:
        logger.error(msg)
        return

    try:
        s = json.dumps(details, ensure_ascii=False, indent=2, default=str)
    except Exception:
        s = str(details)

    if len(s) > max_chars:
        s = s[:max_chars] + f"\n…(troncato, {len(s)} chars totali)"

    logger.error(f"{msg}\n{s}")


# =============================================================================
# CONFIG
# =============================================================================

def _env(name: str, default: str = "") -> str:
    return os.getenv(name, default).strip()


def _env_bool(name: str, default: str = "0") -> bool:
    return _env(name, default) == "1"


def _env_int(name: str, default: str = "0") -> int:
    try:
        return int(_env(name, default))
    except ValueError:
        return int(default)


DATA_DIR = _env("DATA_DIR", r"C:\WinVaria\data")
DBF_FILENAME = _env("DBF_FILENAME", "testi.dbf")
DBF_PATH = os.path.join(DATA_DIR, DBF_FILENAME)

SHOPIFY_SHOP = _env("SHOPIFY_SHOP", "")
SHOPIFY_ACCESS_TOKEN = _env("SHOPIFY_ACCESS_TOKEN", "")
SHOPIFY_API_VERSION = _env("SHOPIFY_API_VERSION", "2026-01")

# Stessi identificatori usati nello script principale per collegare DBF e Shopify.
EXTERNAL_ID_NAMESPACE = _env("EXTERNAL_ID_NAMESPACE", "custom")
EXTERNAL_ID_KEY = _env("EXTERNAL_ID_KEY", "external_id")

# Modalità test: legge DBF e prepara gli aggiornamenti, ma non modifica Shopify.
DRY_RUN = _env_bool("DRY_RUN", "0")

# Limita i record letti dal DBF, utile per test. 0 = tutti.
LIMIT_RECORDS = _env_int("LIMIT_RECORDS", "0")

# Numero di prodotti per ogni bulk mutation JSONL.
DISCOUNT_BULK_CHUNK_SIZE = _env_int("DISCOUNT_BULK_CHUNK_SIZE", "5000")

# Quante varianti leggere per prodotto quando si cercano gli ID su Shopify.
# Per i libri di questo flusso di solito basta 1, ma 10 lascia margine.
SHOPIFY_VARIANTS_FIRST = _env_int("SHOPIFY_VARIANTS_FIRST", "10")

# Se SCONTO è 0, questo script può rimuovere compareAtPrice e riportare price al prezzo pieno.
CLEAR_COMPARE_AT_PRICE_WHEN_NO_DISCOUNT = _env_bool("CLEAR_COMPARE_AT_PRICE_WHEN_NO_DISCOUNT", "1")

# Token refresh, stessi nomi usati dallo script principale.
TOKEN_ENV_PATH = ENV_PATH
SHOPIFY_REFRESH_CLIENT_ID = _env("SHOPIFY_REFRESH_CLIENT_ID", "")
SHOPIFY_REFRESH_CLIENT_SECRET = _env("SHOPIFY_REFRESH_CLIENT_SECRET", "")


# =============================================================================
# DBF minimal reader
# =============================================================================

FieldDef = Tuple[str, str, int, int]  # (name, type, length, decimals)


def _read_all_bytes(path: str) -> bytes:
    with open(path, "rb") as f:
        return f.read()


def parse_dbf_fields(dbf: bytes) -> Tuple[List[FieldDef], int, int, int]:
    num_records = struct.unpack("<I", dbf[4:8])[0]
    header_len = struct.unpack("<H", dbf[8:10])[0]
    record_len = struct.unpack("<H", dbf[10:12])[0]

    fields: List[FieldDef] = []
    off = 32
    while off < header_len:
        if dbf[off] == 0x0D:
            break
        name = dbf[off:off + 11].split(b"\x00", 1)[0].decode("ascii", errors="ignore")
        ftype = chr(dbf[off + 11])
        length = dbf[off + 16]
        dec = dbf[off + 17]
        fields.append((name, ftype, length, dec))
        off += 32

    return fields, header_len, record_len, num_records


def build_field_offsets(fields: List[FieldDef]) -> List[Tuple[str, str, int, int, int]]:
    offsets = []
    off = 1  # deletion flag
    for name, ftype, length, dec in fields:
        offsets.append((name, ftype, length, dec, off))
        off += length
    return offsets


def parse_record(
    dbf: bytes,
    field_offsets: List[Tuple[str, str, int, int, int]],
    header_len: int,
    record_len: int,
    rec_index: int,
) -> Optional[Dict[str, Any]]:
    base = header_len + rec_index * record_len
    rec = dbf[base:base + record_len]
    if not rec:
        return None
    if rec[0] == 0x2A:  # deleted
        return None

    out: Dict[str, Any] = {}
    for name, ftype, length, dec, off in field_offsets:
        raw = rec[off:off + length]

        if ftype == "C":
            out[name] = raw.decode("cp1252", errors="ignore").rstrip()
        elif ftype == "N":
            s = raw.decode("ascii", errors="ignore").strip()
            out[name] = Decimal(s.replace(",", ".")) if s else None
        elif ftype == "I":
            out[name] = struct.unpack("<i", raw)[0]
        elif ftype == "D":
            s = raw.decode("ascii", errors="ignore").strip()
            if s:
                out[name] = datetime.date(int(s[0:4]), int(s[4:6]), int(s[6:8]))
            else:
                out[name] = None
        elif ftype == "L":
            c = raw[:1].decode("ascii", errors="ignore").upper()
            out[name] = c in ("Y", "T")
        elif ftype == "M":
            out[name] = struct.unpack("<I", raw)[0]
        else:
            out[name] = raw

    return out


# =============================================================================
# Helpers DBF / money
# =============================================================================

def get_ci(row: Dict[str, Any], *keys: str, default=None):
    if not row:
        return default
    lower_map = {k.lower(): k for k in row.keys()}
    for k in keys:
        if k in row:
            return row.get(k, default)
        lk = k.lower()
        if lk in lower_map:
            return row.get(lower_map[lk], default)
    return default


def clean_str(value: Any) -> str:
    return str(value or "").strip()


def normalize_decimal_text(value: str) -> str:
    """
    Normalizza numeri scritti in formato italiano o internazionale senza usare float.

    Esempi gestiti:
    - 5,04      -> 5.04
    - 5.04      -> 5.04
    - 5,04%     -> 5.04
    - 5.04%     -> 5.04
    - 1.234,56  -> 1234.56
    - 1,234.56  -> 1234.56
    """
    s = str(value or "").strip()
    s = s.replace("\u00a0", "")
    s = s.replace("%", "")
    s = s.replace("€", "")
    s = s.replace(" ", "")

    if not s:
        return ""

    # Se sono presenti sia virgola sia punto, considera l'ultimo separatore
    # come separatore decimale e l'altro come separatore delle migliaia.
    if "," in s and "." in s:
        if s.rfind(",") > s.rfind("."):
            s = s.replace(".", "").replace(",", ".")
        else:
            s = s.replace(",", "")
    elif "," in s:
        s = s.replace(",", ".")

    return s


def to_decimal(value: Any, default: Decimal = Decimal("0")) -> Decimal:
    if value is None:
        return default
    try:
        if isinstance(value, Decimal):
            return value
        if isinstance(value, str):
            value = normalize_decimal_text(value)
            if not value:
                return default
        return Decimal(str(value))
    except (InvalidOperation, ValueError, TypeError):
        return default


def clamp_percent(value: Decimal) -> Decimal:
    if value < Decimal("0"):
        return Decimal("0")
    if value > Decimal("100"):
        return Decimal("100")
    return value


def money_str(value: Decimal) -> str:
    truncated = value.quantize(Decimal("0.01"), rounding=ROUND_DOWN)
    return f"{truncated:.2f}"


def compute_prices_from_row(row: Dict[str, Any]) -> Optional[Dict[str, Any]]:
    ean = clean_str(get_ci(row, "CODICE_EAN", default=""))
    if not ean:
        return None

    prezzo = to_decimal(get_ci(row, "PREZZO_EUR", "Prezzo_eur", "prezzo_eur", "PREZZO", default=Decimal("0")))
    sconto_percentuale = to_decimal(get_ci(row, "SCONTO", "Sconto", "sconto", default=Decimal("0")))
    sconto_percentuale = clamp_percent(sconto_percentuale)

    if prezzo < Decimal("0"):
        prezzo = Decimal("0")

    prezzo_finale = prezzo * (Decimal("1") - (sconto_percentuale / Decimal("100")))
    if prezzo_finale < Decimal("0"):
        prezzo_finale = Decimal("0")

    price = money_str(prezzo_finale)
    compare_at_price: Optional[str] = None

    if sconto_percentuale > 0 and prezzo_finale < prezzo:
        compare_at_price = money_str(prezzo)

    return {
        "ean": ean,
        "prezzo": money_str(prezzo),
        "sconto_percentuale": str(sconto_percentuale.normalize()),
        "price": price,
        "compareAtPrice": compare_at_price,
    }


def read_testi_discount_records() -> Dict[str, Dict[str, Any]]:
    if not os.path.exists(DBF_PATH):
        raise FileNotFoundError(f"DBF non trovato: {DBF_PATH}")

    dbf = _read_all_bytes(DBF_PATH)
    fields, header_len, record_len, num_records = parse_dbf_fields(dbf)
    field_offsets = build_field_offsets(fields)

    field_names = {name.lower() for name, _ftype, _length, _dec in fields}
    if "sconto" not in field_names:
        raise RuntimeError("Campo SCONTO non trovato nel DBF testi")

    selected: Dict[str, Dict[str, Any]] = {}
    duplicates = 0

    for i in range(num_records):
        row = parse_record(dbf, field_offsets, header_len, record_len, i)
        if not row:
            continue

        raw_ean = clean_str(get_ci(row, "CODICE_EAN", default=""))
        if " " in raw_ean:
            log_error("Record scartato: CODICE_EAN contiene spazi", {"raw_ean": raw_ean})
            continue

        payload = compute_prices_from_row(row)
        if not payload:
            continue

        ean = payload["ean"]
        if ean in selected:
            duplicates += 1

        # Se ci sono duplicati nel DBF, l'ultima riga vince.
        selected[ean] = payload

        if LIMIT_RECORDS and len(selected) >= LIMIT_RECORDS:
            break

    if duplicates:
        logger.warning(f"EAN duplicati nel DBF: {duplicates}. Per ogni EAN viene usata l'ultima riga letta.")

    return selected


# =============================================================================
# Shopify GraphQL + token refresh
# =============================================================================

def file_path_str(p: Path) -> str:
    return str(p.resolve())


def _shop_name_only(shop_value: str) -> str:
    shop_value = (shop_value or "").strip()
    if not shop_value:
        return ""
    return (
        shop_value
        .replace("https://", "")
        .replace("http://", "")
        .replace(".myshopify.com", "")
        .strip("/")
    )


def update_config_env_access_token(new_token: str, file_path: Path = TOKEN_ENV_PATH) -> None:
    if not new_token:
        raise ValueError("Nuovo access token vuoto")

    if file_path.exists():
        content = file_path.read_text(encoding="utf-8")
    else:
        content = ""

    pattern = r"(?m)^SHOPIFY_ACCESS_TOKEN=.*$"
    replacement = f"SHOPIFY_ACCESS_TOKEN={new_token}"

    if re.search(pattern, content):
        new_content = re.sub(pattern, replacement, content)
    else:
        if content and not content.endswith("\n"):
            content += "\n"
        new_content = content + replacement + "\n"

    file_path.write_text(new_content, encoding="utf-8")


def refresh_shopify_access_token() -> str:
    global SHOPIFY_ACCESS_TOKEN

    if not SHOPIFY_REFRESH_CLIENT_ID or not SHOPIFY_REFRESH_CLIENT_SECRET:
        raise RuntimeError(
            "Token Shopify non valido/scaduto e SHOPIFY_REFRESH_CLIENT_ID/SECRET mancanti in config.env"
        )

    shop_name = _shop_name_only(SHOPIFY_SHOP)
    if not shop_name:
        raise RuntimeError("SHOPIFY_SHOP mancante o non valido")

    refresh_url = f"https://{shop_name}.myshopify.com/admin/oauth/access_token"
    payload = {
        "grant_type": "client_credentials",
        "client_id": SHOPIFY_REFRESH_CLIENT_ID,
        "client_secret": SHOPIFY_REFRESH_CLIENT_SECRET,
    }
    headers = {"Content-Type": "application/x-www-form-urlencoded"}

    logger.info("Token Shopify non valido/scaduto: rigenerazione access token in corso")

    resp = requests.post(refresh_url, headers=headers, data=payload, timeout=60)
    resp.raise_for_status()

    data = resp.json()
    new_token = (data.get("access_token") or "").strip()
    if not new_token:
        log_error("Rigenerazione token fallita: risposta senza access_token", data)
        raise RuntimeError("Risposta refresh token senza access_token")

    update_config_env_access_token(new_token)
    SHOPIFY_ACCESS_TOKEN = new_token
    os.environ["SHOPIFY_ACCESS_TOKEN"] = new_token

    logger.info(f"Nuovo access token Shopify generato e salvato in: {file_path_str(TOKEN_ENV_PATH)}")
    return new_token


def _graphql_has_auth_error(data: Dict[str, Any]) -> bool:
    if not isinstance(data, dict):
        return False

    errors = data.get("errors") or []
    for err in errors:
        msg = str((err or {}).get("message") or "").lower()
        ext_code = str(((err or {}).get("extensions") or {}).get("code") or "").lower()

        if (
            "invalid api key or access token" in msg
            or "access denied" in msg
            or "unauthorized" in msg
            or "forbidden" in msg
            or ext_code in {"unauthorized", "forbidden", "access_denied"}
        ):
            return True

    return False


def _is_auth_http_error(resp: Optional[requests.Response]) -> bool:
    if resp is None:
        return False
    return resp.status_code in (401, 403)


def shopify_graphql(
    endpoint: str,
    token: str,
    query: str,
    variables: Dict[str, Any],
    *,
    max_retries: int = 6,
) -> Dict[str, Any]:
    global SHOPIFY_ACCESS_TOKEN

    current_token = token
    auth_refresh_done = False

    for attempt in range(max_retries):
        headers = {
            "Content-Type": "application/json",
            "X-Shopify-Access-Token": current_token,
        }

        try:
            resp = requests.post(
                endpoint,
                headers=headers,
                json={"query": query, "variables": variables},
                timeout=180,
            )

            if _is_auth_http_error(resp):
                if auth_refresh_done:
                    resp.raise_for_status()
                current_token = refresh_shopify_access_token()
                SHOPIFY_ACCESS_TOKEN = current_token
                auth_refresh_done = True
                logger.info("Retry chiamata Shopify GraphQL con nuovo access token")
                continue

            if resp.status_code in (429, 500, 502, 503, 504):
                raise requests.HTTPError(f"HTTP {resp.status_code}", response=resp)

            resp.raise_for_status()
            data = resp.json()

            if _graphql_has_auth_error(data):
                if auth_refresh_done:
                    log_error("Errore autenticazione Shopify anche dopo refresh token", data)
                    raise RuntimeError("Autenticazione Shopify fallita anche dopo refresh token")

                current_token = refresh_shopify_access_token()
                SHOPIFY_ACCESS_TOKEN = current_token
                auth_refresh_done = True
                logger.info("Retry GraphQL dopo refresh token per errore auth applicativo")
                continue

            if "errors" in data and data["errors"]:
                log_error("GraphQL top-level errors", data["errors"])

            return data

        except Exception as e:
            if attempt >= max_retries - 1:
                log_error(
                    "Errore chiamata Shopify GraphQL (ultimo tentativo)",
                    {
                        "error": str(e),
                        "status": getattr(getattr(e, "response", None), "status_code", None),
                    },
                )
                raise

            sleep_s = min(2 ** attempt, 30)
            time.sleep(sleep_s)

    raise RuntimeError("Unreachable")


# =============================================================================
# Shopify Bulk Operations
# =============================================================================

MUTATION_STAGED_UPLOADS_CREATE = """
mutation StagedUploadsCreate($input: [StagedUploadInput!]!) {
  stagedUploadsCreate(input: $input) {
    userErrors { field message }
    stagedTargets {
      url
      resourceUrl
      parameters { name value }
    }
  }
}
""".strip()

MUTATION_BULK_RUN_MUTATION = """
mutation BulkRun($mutation: String!, $stagedUploadPath: String!, $clientIdentifier: String) {
  bulkOperationRunMutation(mutation: $mutation, stagedUploadPath: $stagedUploadPath, clientIdentifier: $clientIdentifier) {
    bulkOperation { id status }
    userErrors { field message }
  }
}
""".strip()

MUTATION_BULK_RUN_QUERY = """
mutation BulkRunQuery($query: String!) {
  bulkOperationRunQuery(query: $query) {
    bulkOperation { id status }
    userErrors { field message }
  }
}
""".strip()

QUERY_BULK_OPERATION = """
query BulkOp($id: ID!) {
  bulkOperation(id: $id) {
    id
    status
    errorCode
    createdAt
    completedAt
    objectCount
    fileSize
    url
    partialDataUrl
  }
}
""".strip()

BULK_MUTATION_PRODUCT_VARIANTS_UPDATE = """
mutation ProductVariantsBulkUpdate($productId: ID!, $variants: [ProductVariantsBulkInput!]!) {
  productVariantsBulkUpdate(productId: $productId, variants: $variants, allowPartialUpdates: true) {
    product { id }
    productVariants { id price compareAtPrice }
    userErrors { field message code }
  }
}
""".strip()


def build_bulk_query_products() -> str:
    variants_first = max(1, min(SHOPIFY_VARIANTS_FIRST, 250))
    return f"""
{{
  products(first: 250) {{
    edges {{
      node {{
        id
        handle
        title
        metafield(namespace: \"{EXTERNAL_ID_NAMESPACE}\", key: \"{EXTERNAL_ID_KEY}\") {{
          value
        }}
        variants(first: {variants_first}) {{
          edges {{
            node {{
              id
              sku
              title
              price
              compareAtPrice
            }}
          }}
        }}
      }}
    }}
  }}
}}
""".strip()


def staged_upload_jsonl(endpoint: str, token: str, *, filename: str, jsonl_bytes: bytes) -> str:
    res = shopify_graphql(
        endpoint,
        token,
        MUTATION_STAGED_UPLOADS_CREATE,
        {
            "input": [{
                "resource": "BULK_MUTATION_VARIABLES",
                "filename": filename,
                "mimeType": "text/jsonl",
                "httpMethod": "POST",
            }]
        },
    )

    payload = (res.get("data") or {}).get("stagedUploadsCreate") or {}

    errs = payload.get("userErrors") or []
    if errs:
        log_error("stagedUploadsCreate userErrors", errs)
        raise RuntimeError("stagedUploadsCreate userErrors")

    targets = payload.get("stagedTargets") or []
    if not targets:
        log_error("stagedUploadsCreate stagedTargets vuoto", res)
        raise RuntimeError("stagedUploadsCreate stagedTargets vuoto")

    t0 = targets[0]
    upload_url = t0.get("url")
    params_list = t0.get("parameters") or []
    params = {p["name"]: p["value"] for p in params_list if "name" in p and "value" in p}
    staged_path = params.get("key")

    if not upload_url or not staged_path:
        log_error("stagedUploadsCreate: url/key mancanti", t0)
        raise RuntimeError("stagedUploadsCreate url/key mancanti")

    up = requests.post(
        upload_url,
        data=list(params.items()),
        files=[("file", (filename, jsonl_bytes, "text/jsonl"))],
        timeout=240,
    )

    if up.status_code not in (200, 201, 204):
        log_error("Upload JSONL fallito", {"status": up.status_code, "text": up.text[:1500]})
        raise RuntimeError("Upload JSONL fallito")

    return staged_path


def bulk_run_query(endpoint: str, token: str, *, query_str: str) -> str:
    res = shopify_graphql(endpoint, token, MUTATION_BULK_RUN_QUERY, {"query": query_str})
    payload = (res.get("data") or {}).get("bulkOperationRunQuery") or {}

    uerrs = payload.get("userErrors") or []
    if uerrs:
        log_error("bulkOperationRunQuery userErrors", uerrs)
        raise RuntimeError("bulkOperationRunQuery userErrors")

    op_id = ((payload.get("bulkOperation") or {}).get("id"))
    if not op_id:
        log_error("bulkOperationRunQuery: bulkOperation.id mancante", res)
        raise RuntimeError("bulkOperationRunQuery id mancante")

    return op_id


def bulk_run_mutation(
    endpoint: str,
    token: str,
    *,
    mutation_str: str,
    staged_upload_path: str,
    client_identifier: str,
) -> str:
    variables = {
        "mutation": mutation_str,
        "stagedUploadPath": staged_upload_path,
        "clientIdentifier": client_identifier,
    }

    res = shopify_graphql(endpoint, token, MUTATION_BULK_RUN_MUTATION, variables)
    payload = (res.get("data") or {}).get("bulkOperationRunMutation") or {}

    uerrs = payload.get("userErrors") or []
    if uerrs:
        log_error("bulkOperationRunMutation userErrors", uerrs)
        raise RuntimeError("bulkOperationRunMutation userErrors")

    op_id = ((payload.get("bulkOperation") or {}).get("id"))
    if not op_id:
        log_error("bulkOperationRunMutation: bulkOperation.id mancante", res)
        raise RuntimeError("bulkOperationRunMutation id mancante")

    return op_id


def poll_bulk_operation(endpoint: str, token: str, op_id: str, *, poll_seconds: int = 5) -> Dict[str, Any]:
    while True:
        res = shopify_graphql(endpoint, token, QUERY_BULK_OPERATION, {"id": op_id})
        op = (res.get("data") or {}).get("bulkOperation") or {}
        status = (op.get("status") or "").upper()

        if status in ("COMPLETED", "FAILED", "CANCELED", "CANCELLED"):
            return op

        object_count = op.get("objectCount")
        logger.info(f"Bulk operation in corso: status={status}, objectCount={object_count}")
        time.sleep(poll_seconds)


def iter_jsonl_from_url(url: str) -> Iterable[Dict[str, Any]]:
    with requests.get(url, stream=True, timeout=600) as r:
        r.raise_for_status()
        for line in r.iter_lines(decode_unicode=True):
            if not line:
                continue
            yield json.loads(line)


def chunked(items: List[Any], size: int) -> Iterable[List[Any]]:
    for i in range(0, len(items), size):
        yield items[i:i + size]


# =============================================================================
# Shopify product lookup parsing
# =============================================================================

def _is_gid(value: Any, object_name: str) -> bool:
    return isinstance(value, str) and value.startswith(f"gid://shopify/{object_name}/")


def extract_external_id_from_product_line(obj: Dict[str, Any]) -> str:
    mf = obj.get("metafield")
    if isinstance(mf, dict):
        return clean_str(mf.get("value"))
    return ""


def fetch_existing_products_by_external_id(endpoint: str) -> Dict[str, Dict[str, Any]]:
    logger.info("Avvio bulk query Shopify: lettura prodotti esistenti e varianti")

    op_id = bulk_run_query(
        endpoint,
        SHOPIFY_ACCESS_TOKEN,
        query_str=build_bulk_query_products(),
    )
    op = poll_bulk_operation(endpoint, SHOPIFY_ACCESS_TOKEN, op_id)
    status = (op.get("status") or "").upper()

    if status != "COMPLETED":
        log_error("Bulk query prodotti non COMPLETED", op)
        raise RuntimeError("Bulk query prodotti fallita")

    out_url = op.get("url")
    if not out_url:
        logger.warning("Bulk query prodotti completata ma senza URL: nessun prodotto trovato")
        return {}

    products_by_id: Dict[str, Dict[str, Any]] = {}
    variants_by_parent: Dict[str, List[Dict[str, Any]]] = {}

    for obj in iter_jsonl_from_url(out_url):
        gid = obj.get("id")
        parent_id = obj.get("__parentId")

        if _is_gid(gid, "Product") and not parent_id:
            external_id = extract_external_id_from_product_line(obj)
            products_by_id[gid] = {
                "id": gid,
                "title": obj.get("title"),
                "handle": obj.get("handle"),
                "external_id": external_id,
                "variants": [],
            }
        elif _is_gid(gid, "ProductVariant") and parent_id:
            variants_by_parent.setdefault(parent_id, []).append(obj)

    existing_by_ean: Dict[str, Dict[str, Any]] = {}
    missing_external_id = 0

    for product_id, product in products_by_id.items():
        product["variants"] = variants_by_parent.get(product_id, [])
        ean = clean_str(product.get("external_id"))
        if not ean:
            missing_external_id += 1
            continue
        if ean in existing_by_ean:
            logger.warning(f"Metafield external_id duplicato su Shopify: {ean}. Uso il primo prodotto trovato.")
            continue
        existing_by_ean[ean] = product

    logger.info(
        f"Prodotti Shopify letti: {len(products_by_id)} | "
        f"con {EXTERNAL_ID_NAMESPACE}.{EXTERNAL_ID_KEY}: {len(existing_by_ean)} | "
        f"senza external_id: {missing_external_id}"
    )

    return existing_by_ean


def pick_variant_for_ean(product: Dict[str, Any], ean: str) -> Optional[Dict[str, Any]]:
    variants = product.get("variants") or []
    if not variants:
        return None

    for variant in variants:
        if clean_str(variant.get("sku")) == ean:
            return variant

    # I prodotti creati dallo script principale hanno una sola variante.
    # Se lo SKU non combacia, aggiorniamo comunque la prima variante trovata.
    return variants[0]


def build_variant_update_lines(
    dbf_by_ean: Dict[str, Dict[str, Any]],
    shopify_by_ean: Dict[str, Dict[str, Any]],
) -> Tuple[List[Dict[str, Any]], Dict[str, int]]:
    updates_by_product: Dict[str, List[Dict[str, Any]]] = {}
    stats = {
        "dbf_records": len(dbf_by_ean),
        "matched_products": 0,
        "missing_products": 0,
        "missing_variants": 0,
        "discounted": 0,
        "not_discounted": 0,
    }

    for ean, price_payload in dbf_by_ean.items():
        product = shopify_by_ean.get(ean)
        if not product:
            stats["missing_products"] += 1
            logger.warning(f"Prodotto Shopify non trovato per EAN={ean}")
            continue

        variant = pick_variant_for_ean(product, ean)
        if not variant or not variant.get("id"):
            stats["missing_variants"] += 1
            logger.warning(f"Variante Shopify non trovata per EAN={ean}, product_id={product.get('id')}")
            continue

        stats["matched_products"] += 1

        variant_input: Dict[str, Any] = {
            "id": variant["id"],
            "price": price_payload["price"],
        }

        compare_at_price = price_payload.get("compareAtPrice")
        if compare_at_price:
            variant_input["compareAtPrice"] = compare_at_price
            stats["discounted"] += 1
        else:
            stats["not_discounted"] += 1
            if CLEAR_COMPARE_AT_PRICE_WHEN_NO_DISCOUNT:
                variant_input["compareAtPrice"] = None

        product_id = product["id"]
        updates_by_product.setdefault(product_id, []).append(variant_input)

    lines = [
        {
            "productId": product_id,
            "variants": variants,
        }
        for product_id, variants in updates_by_product.items()
    ]

    return lines, stats


def run_discount_bulk_updates(endpoint: str, update_lines: List[Dict[str, Any]]) -> Dict[str, int]:
    result_stats = {
        "chunks": 0,
        "mutation_lines": 0,
        "updated_variants_reported": 0,
        "lines_with_errors": 0,
    }

    total_chunks = (len(update_lines) + DISCOUNT_BULK_CHUNK_SIZE - 1) // DISCOUNT_BULK_CHUNK_SIZE if update_lines else 0
    logger.info(f"Bulk aggiornamento sconti: {len(update_lines)} prodotti da aggiornare (chunk={DISCOUNT_BULK_CHUNK_SIZE})")

    for chunk_idx, chunk_items in enumerate(chunked(update_lines, DISCOUNT_BULK_CHUNK_SIZE), start=1):
        t_chunk0 = time.perf_counter()
        logger.info(f"[sconti] Chunk {chunk_idx}/{total_chunks} | preparo JSONL")

        lines = [json.dumps(item, ensure_ascii=False, separators=(",", ":")) for item in chunk_items]
        jsonl_bytes = ("\n".join(lines) + "\n").encode("utf-8")

        staged_path = staged_upload_jsonl(
            endpoint,
            SHOPIFY_ACCESS_TOKEN,
            filename=f"discount_update_chunk{chunk_idx}.jsonl",
            jsonl_bytes=jsonl_bytes,
        )

        op_id = bulk_run_mutation(
            endpoint,
            SHOPIFY_ACCESS_TOKEN,
            mutation_str=BULK_MUTATION_PRODUCT_VARIANTS_UPDATE,
            staged_upload_path=staged_path,
            client_identifier=f"discount_update_chunk{chunk_idx}",
        )

        op = poll_bulk_operation(endpoint, SHOPIFY_ACCESS_TOKEN, op_id)
        status = (op.get("status") or "").upper()
        if status != "COMPLETED":
            log_error("Bulk aggiornamento sconti non COMPLETED", op)
            raise RuntimeError("Bulk aggiornamento sconti fallita")

        out_url = op.get("url")
        if not out_url:
            logger.warning(f"[sconti] Chunk {chunk_idx}/{total_chunks} completato senza output URL")
            continue

        for out_line in iter_jsonl_from_url(out_url):
            result_stats["mutation_lines"] += 1
            payload = (out_line.get("data") or {}).get("productVariantsBulkUpdate") or {}
            user_errors = payload.get("userErrors") or []
            if user_errors:
                result_stats["lines_with_errors"] += 1
                log_error("productVariantsBulkUpdate userErrors", user_errors)
                continue

            updated_variants = payload.get("productVariants") or []
            result_stats["updated_variants_reported"] += len(updated_variants)

        result_stats["chunks"] += 1
        logger.info(
            f"[sconti] Chunk {chunk_idx}/{total_chunks} COMPLETED "
            f"in {time.perf_counter() - t_chunk0:.2f}s"
        )

    return result_stats


# =============================================================================
# Main
# =============================================================================

def main() -> None:
    logger.info("=== AVVIO APPLICAZIONE SCONTI BULK SHOPIFY ===")
    logger.info(f"APP_DIR:    {SCRIPT_DIR}")
    logger.info(f"Config env: {file_path_str(ENV_PATH)}")
    logger.info(f"Log file:   {LOG_PATH}")
    logger.info(f"DBF testi:  {DBF_PATH}")
    logger.info(f"External ID: {EXTERNAL_ID_NAMESPACE}.{EXTERNAL_ID_KEY}")
    logger.info(f"Clear compareAtPrice se sconto=0: {CLEAR_COMPARE_AT_PRICE_WHEN_NO_DISCOUNT}")

    dbf_by_ean = read_testi_discount_records()
    logger.info(f"Record DBF con EAN/prezzo/sconto: {len(dbf_by_ean)}")

    sample_discounted = [x for x in dbf_by_ean.values() if x.get("compareAtPrice")][:5]
    if sample_discounted:
        logger.info(f"Esempi sconti calcolati: {sample_discounted}")

    if DRY_RUN:
        logger.info("DRY_RUN=1: non chiamo Shopify e non aggiorno prodotti")
        return

    if not SHOPIFY_SHOP or not SHOPIFY_ACCESS_TOKEN:
        raise RuntimeError(
            "Config mancante: SHOPIFY_SHOP e SHOPIFY_ACCESS_TOKEN (oppure DRY_RUN=1)."
        )

    endpoint = f"https://{SHOPIFY_SHOP}/admin/api/{SHOPIFY_API_VERSION}/graphql.json"

    shopify_by_ean = fetch_existing_products_by_external_id(endpoint)
    update_lines, match_stats = build_variant_update_lines(dbf_by_ean, shopify_by_ean)

    logger.info(f"Riepilogo preparazione aggiornamenti: {match_stats}")

    if not update_lines:
        logger.info("Nessun prodotto da aggiornare")
        return

    mutation_stats = run_discount_bulk_updates(endpoint, update_lines)
    logger.info(f"Riepilogo bulk mutation: {mutation_stats}")
    logger.info("=== FINE APPLICAZIONE SCONTI BULK SHOPIFY ===")


if __name__ == "__main__":
    main()
