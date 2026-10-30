# =============================================================================
# VERSIONE SICURA V2 - migrazione da conn_sync_bulk originale
# Inventory ID riparati per SKU, bulk per __lineNumber, batch resilienti
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
import html
import json
import struct
import datetime
import hashlib
import uuid
import sqlite3
import requests
import time
import logging
from decimal import Decimal, InvalidOperation, ROUND_DOWN
from logging.handlers import RotatingFileHandler
from typing import Any, Dict, List, Optional, Tuple, Iterable
from urllib.parse import urlparse, unquote


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

# carica config.env dalla stessa cartella del .py / .exe
load_dotenv(dotenv_path=ENV_PATH)


# =============================================================================
# LOGGING
# =============================================================================

SCRIPT_DIR = str(APP_DIR)
LOG_PATH = str(APP_DIR / "shopify_sync.log")

logger = logging.getLogger("shopify_sync")
logger.setLevel(logging.INFO)
logger.propagate = False

_fmt = logging.Formatter("%(asctime)s | %(levelname)s | %(message)s")

if not logger.handlers:
    _file = RotatingFileHandler(
        LOG_PATH,
        maxBytes=5_000_000,
        backupCount=3,
        encoding="utf-8"
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
# CONFIG (da ENV)
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


def _env_csv(name: str, default: str = "") -> List[str]:
    raw = _env(name, default)
    return [x.strip() for x in raw.split(",") if x.strip()]


DATA_DIR = _env("DATA_DIR", r"C:\WinVaria\data")

DBF_FILENAME = _env("DBF_FILENAME", "testi.dbf")
FPT_FILENAME = _env("FPT_FILENAME", "testi.fpt")
TIPOLOGIE_DBF_FILENAME = _env("TIPOLOGIE_DBF_FILENAME", "tipologie.dbf")

DBF_PATH = os.path.join(DATA_DIR, DBF_FILENAME)
FPT_PATH = os.path.join(DATA_DIR, FPT_FILENAME)
TIPOLOGIE_DBF_PATH = os.path.join(DATA_DIR, TIPOLOGIE_DBF_FILENAME)

SHOPIFY_SHOP = _env("SHOPIFY_SHOP", "")
SHOPIFY_ACCESS_TOKEN = _env("SHOPIFY_ACCESS_TOKEN", "")
SHOPIFY_API_VERSION = _env("SHOPIFY_API_VERSION", "2026-01")
SHOPIFY_LOCATION_ID = _env("SHOPIFY_LOCATION_ID", "")

DRY_RUN = _env_bool("DRY_RUN", "0")
DEFAULT_TAGS = _env_csv("DEFAULT_TAGS", "libro")
PUBLICATION_IDS = _env_csv("SHOPIFY_PUBLICATION_IDS", "")

LIMIT_RECORDS = _env_int("LIMIT_RECORDS", "0")
COVER_URL_TEMPLATE = _env(
    "COVER_URL_TEMPLATE",
    "https://www.ibs.it/images/{isbn}_0_0_0_0_0.jpg"
)

# -----------------------------------------------------------------------------
# WordPress cover first
# -----------------------------------------------------------------------------
# Quando viene creato un prodotto nuovo, l'immagine viene scelta così:
# 1) immagine WordPress trovata tramite EAN
# 2) fallback al comportamento precedente: COVER_URL_TEMPLATE
#
# Gli aggiornamenti dei prodotti esistenti NON inviano mai files a Shopify.
# -----------------------------------------------------------------------------
WORDPRESS_COVER_ENABLED = _env_bool("WORDPRESS_COVER_ENABLED", "1")
WORDPRESS_MEDIA_SEARCH_URL = _env(
    "WORDPRESS_MEDIA_SEARCH_URL",
    "https://didalibri.com/wp-json/wp/v2/media"
)
WORDPRESS_PER_PAGE = _env_int("WORDPRESS_PER_PAGE", "100")
WORDPRESS_REQUEST_TIMEOUT = _env_int("WORDPRESS_REQUEST_TIMEOUT", "60")
WORDPRESS_INDEX_TIMEOUT_SECONDS = _env_int("WORDPRESS_INDEX_TIMEOUT_SECONDS", "1800")
WORDPRESS_SLEEP_BETWEEN_PAGES_MS = _env_int("WORDPRESS_SLEEP_BETWEEN_PAGES_MS", "100")
WORDPRESS_LIMIT_MEDIA = _env_int("WORDPRESS_LIMIT_MEDIA", "0")
EAN_FROM_MEDIA_RE = re.compile(r"(\d{8,18})")

# Stato SQLite: nella cartella dello script / exe
STATE_DB_PATH = str(APP_DIR / "shopify_sync_state.sqlite")

# Bulk tuning
BULK_ENABLED = _env_bool("BULK_ENABLED", "1")
BULK_CHUNK_SIZE = _env_int("BULK_CHUNK_SIZE", "5000")
SQLITE_UPSERT_BATCH = _env_int("SQLITE_UPSERT_BATCH", "2000")
INVENTORY_ZERO_BATCH = _env_int("INVENTORY_ZERO_BATCH", "200")
# Tempo massimo (secondi) di attesa per ogni operazione bulk Shopify.
BULK_POLL_TIMEOUT_SECONDS = _env_int("BULK_POLL_TIMEOUT_SECONDS", "7200")

# Sicurezza per sincronizzazioni frequenti. Gli azzeramenti degli EAN scomparsi
# sono inizialmente disattivati: lo stato esistente contiene associazioni duplicate.
ZERO_MISSING_PRODUCTS = _env_bool("ZERO_MISSING_PRODUCTS", "0")
REPAIR_COOLDOWN_MINUTES = _env_int("REPAIR_COOLDOWN_MINUTES", "60")
VERIFY_LINK_TTL_DAYS = _env_int("VERIFY_LINK_TTL_DAYS", "1")
MAX_NEW_PRODUCTS_PER_RUN = _env_int("MAX_NEW_PRODUCTS_PER_RUN", "500")
CHECK_ONLY = "--check" in sys.argv or "--dry-run" in sys.argv
if CHECK_ONLY:
    DRY_RUN = True



# =============================================================================
# SHOPIFY TOKEN REFRESH
# =============================================================================

TOKEN_ENV_PATH = ENV_PATH

SHOPIFY_REFRESH_CLIENT_ID = _env("SHOPIFY_REFRESH_CLIENT_ID", "")
SHOPIFY_REFRESH_CLIENT_SECRET = _env("SHOPIFY_REFRESH_CLIENT_SECRET", "")


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


# =============================================================================
# DBF / FPT minimal reader (Visual FoxPro DBF + FPT memo)
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


def read_fpt_memo(fpt: bytes, block_index: int) -> Optional[str]:
    if not block_index:
        return None

    block_size = struct.unpack(">H", fpt[6:8])[0]
    start = block_index * block_size
    if start + 8 > len(fpt):
        return None

    _mtype = struct.unpack(">I", fpt[start:start + 4])[0]
    mlen = struct.unpack(">I", fpt[start + 4:start + 8])[0]
    payload = fpt[start + 8:start + 8 + mlen]

    return payload.decode("cp1252", errors="ignore").rstrip("\x00").strip()


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
            # Mantiene esatti i decimali del DBF (prezzi/sconti), senza passare da float.
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
# Helpers: case-insensitive field access + tipologie map
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


def load_tipologie_map(dbf_path: str) -> Dict[int, str]:
    if not os.path.exists(dbf_path):
        raise FileNotFoundError(f"DBF tipologie non trovato: {dbf_path}")

    dbf = _read_all_bytes(dbf_path)
    fields, header_len, record_len, num_records = parse_dbf_fields(dbf)
    field_offsets = build_field_offsets(fields)

    m: Dict[int, str] = {}
    for i in range(num_records):
        r = parse_record(dbf, field_offsets, header_len, record_len, i)
        if not r:
            continue

        raw_id = get_ci(r, "ID", "Id", "ID_TIPO", "Id_tipo", "IDTIPO", default=None)
        desc = clean_str(get_ci(
            r,
            "DESCRIZIONE",
            "Descrizione",
            "DESC",
            "Description",
            "DESCRIZION",
            default=""
        ))

        if raw_id is None:
            continue

        try:
            id_int = int(raw_id)
        except (TypeError, ValueError):
            continue

        if desc:
            m[id_int] = desc

    return m


# =============================================================================
# Shopify GraphQL (retry) + error logs + auto refresh token
# =============================================================================

class ShopifyGraphQLApplicationError(RuntimeError):
    """Errore GraphQL applicativo non ritentabile automaticamente."""


def shopify_graphql(
    endpoint: str,
    token: str,
    query: str,
    variables: Dict[str, Any],
    *,
    max_retries: int = 6
) -> Dict[str, Any]:
    global SHOPIFY_ACCESS_TOKEN

    current_token = token
    auth_refresh_done = False
    for attempt in range(max_retries):
        try:
            headers = {
                "Content-Type": "application/json",
                "X-Shopify-Access-Token": current_token
            }
            resp = requests.post(
                endpoint, headers=headers,
                json={"query": query, "variables": variables}, timeout=180
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
                    raise ShopifyGraphQLApplicationError("Autenticazione Shopify fallita dopo refresh token")
                current_token = refresh_shopify_access_token()
                SHOPIFY_ACCESS_TOKEN = current_token
                auth_refresh_done = True
                logger.info("Retry Shopify GraphQL dopo refresh token")
                continue

            if data.get("errors"):
                errors = data["errors"]
                is_throttled = all(
                    str((err.get("extensions") or {}).get("code") or "").upper() == "THROTTLED"
                    for err in errors if isinstance(err, dict)
                )
                if is_throttled and attempt < max_retries - 1:
                    logger.warning("Shopify GraphQL throttled; nuovo tentativo con backoff")
                    time.sleep(min(2 ** attempt, 30))
                    continue
                log_error("GraphQL top-level errors", errors)
                raise ShopifyGraphQLApplicationError("Shopify GraphQL ha restituito errori top-level")
            if not isinstance(data.get("data"), dict):
                raise ShopifyGraphQLApplicationError("Shopify non ha restituito un payload data valido")
            return data
        except ShopifyGraphQLApplicationError:
            raise
        except Exception as e:
            if attempt >= max_retries - 1:
                log_error("Errore chiamata Shopify GraphQL (ultimo tentativo)", {
                    "error": str(e),
                    "status": getattr(getattr(e, "response", None), "status_code", None)
                })
                raise
            time.sleep(min(2 ** attempt, 30))

    raise RuntimeError("Numero massimo tentativi Shopify esaurito")


# =============================================================================
# Shopify standard mutation (inventory batch)
# =============================================================================

MUTATION_INVENTORY_SET_QUANTITIES = """
mutation InventorySet($input: InventorySetQuantitiesInput!, $idempotencyKey: String!) {
  inventorySetQuantities(input: $input) @idempotent(key: $idempotencyKey) {
    userErrors { field message code }
  }
}
""".strip()


def inventory_set_available_batch(
    endpoint: str,
    inventory_quantities: List[Tuple[str, int]],
    *,
    reference_document_uri: str = "gid://erp-connector/SyncJob/inventory-sync",
) -> Dict[str, Any]:
    """Idempotenza: i retry della stessa chiamata usano la stessa chiave.

    WinVaria e' la fonte autoritativa delle giacenze: changeFromQuantity null
    evita il confronto con il valore Shopify, che puo' cambiare tra i cicli.
    """
    if not inventory_quantities:
        raise ValueError("Impossibile inviare un inventario vuoto")

    q = [{
        "inventoryItemId": inv_id,
        "locationId": SHOPIFY_LOCATION_ID,
        "quantity": int(qty),
        "changeFromQuantity": None
    } for inv_id, qty in inventory_quantities]
    ids = [v["inventoryItemId"] for v in q]
    if len(ids) != len(set(ids)):
        raise ValueError("InventoryItem ID duplicati nella stessa chiamata: blocco di sicurezza")

    # Chiave stabile per retry di questa stessa richiesta, diversa per run/batch.
    key_material = reference_document_uri + "|" + json.dumps(q, sort_keys=True)
    idem = str(uuid.uuid5(uuid.NAMESPACE_URL, key_material))
    variables = {
        "idempotencyKey": idem,
        "input": {
            "name": "available", "reason": "correction",
            "referenceDocumentUri": reference_document_uri,
            "quantities": q
        }
    }
    return shopify_graphql(
        endpoint, SHOPIFY_ACCESS_TOKEN,
        MUTATION_INVENTORY_SET_QUANTITIES, variables
    )


def inventory_errors(res: Dict[str, Any]) -> List[Dict[str, Any]]:
    payload = ((res.get("data") or {}).get("inventorySetQuantities"))
    if not isinstance(payload, dict):
        raise RuntimeError("inventorySetQuantities senza payload: non registro alcun successo")
    return payload.get("userErrors") or []


# =============================================================================
# Bulk Operations
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
        }
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
        timeout=240
    )

    if up.status_code not in (200, 201, 204):
        log_error("Upload JSONL fallito", {"status": up.status_code, "text": up.text[:1500]})
        raise RuntimeError("Upload JSONL fallito")

    return staged_path


def bulk_run_mutation(
    endpoint: str,
    token: str,
    *,
    mutation_str: str,
    staged_upload_path: str,
    client_identifier: str
) -> str:
    variables = {
        "mutation": mutation_str,
        "stagedUploadPath": staged_upload_path,
        "clientIdentifier": client_identifier
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
    start = time.monotonic()
    last_log = start
    last_status = None
    timeout_s = max(1, BULK_POLL_TIMEOUT_SECONDS)

    while True:
        if time.monotonic() - start >= timeout_s:
            raise TimeoutError(f"Bulk Shopify {op_id}: attesa superata ({timeout_s}s), ultimo stato={last_status}")

        res = shopify_graphql(endpoint, token, QUERY_BULK_OPERATION, {"id": op_id})
        op = (res.get("data") or {}).get("bulkOperation") or {}
        status = (op.get("status") or "SCONOSCIUTO").upper()
        now = time.monotonic()

        # Registra i cambi di stato e un messaggio ogni minuto durante l'attesa.
        if status != last_status or now - last_log >= 60:
            logger.info(f"Bulk Shopify {op_id} | stato={status} | "
                        f"oggetti={op.get('objectCount')} | attesa={int(now - start)}s")
            last_log = now
            last_status = status

        if status in ("COMPLETED", "FAILED", "CANCELED", "CANCELLED"):
            return op

        if now - start >= timeout_s:
            raise TimeoutError(f"Bulk Shopify {op_id}: attesa superata ({timeout_s}s), ultimo stato={status}")

        time.sleep(min(poll_seconds, max(0, timeout_s - (now - start))))


def iter_jsonl_from_url(url: str) -> Iterable[Dict[str, Any]]:
    with requests.get(url, stream=True, timeout=600) as r:
        r.raise_for_status()
        for line in r.iter_lines(decode_unicode=True):
            if not line:
                continue
            yield json.loads(line)


# =============================================================================
# WordPress cover index
# =============================================================================

def extract_filename_from_url(url: str) -> str:
    path = urlparse(url or "").path
    return unquote(os.path.basename(path))


def get_wordpress_image_url(media_item: Dict[str, Any]) -> str:
    source_url = clean_str(media_item.get("source_url"))
    if source_url:
        return source_url

    guid = media_item.get("guid") or {}
    if isinstance(guid, dict):
        guid_url = clean_str(guid.get("rendered"))
        if guid_url:
            return guid_url

    return ""


def clean_wp_text(value: Any) -> str:
    if value is None:
        return ""

    if isinstance(value, dict):
        value = value.get("rendered") or ""

    return html.unescape(str(value)).strip()


def extract_ean_from_text(value: Any) -> Optional[str]:
    text = clean_wp_text(value)
    if not text:
        return None

    matches = EAN_FROM_MEDIA_RE.findall(text)
    if not matches:
        return None

    # Preferenza EAN standard 13 cifre
    for m in matches:
        if len(m) == 13:
            return m

    return matches[0]


def extract_ean_from_wordpress_media(media_item: Dict[str, Any]) -> Optional[str]:
    source_url = get_wordpress_image_url(media_item)

    values_to_check: List[Any] = [
        extract_filename_from_url(source_url),
        media_item.get("slug"),
        media_item.get("title"),
        media_item.get("alt_text"),
        source_url,
    ]

    guid = media_item.get("guid") or {}
    if isinstance(guid, dict):
        guid_url = clean_str(guid.get("rendered"))
        values_to_check.extend([
            extract_filename_from_url(guid_url),
            guid_url,
        ])

    for value in values_to_check:
        ean = extract_ean_from_text(value)
        if ean:
            return ean

    return None


def load_wordpress_cover_urls_by_ean() -> Dict[str, str]:
    """
    Indicizza una sola volta tutte le immagini WordPress con EAN.

    Ritorna:
        {
            "978...": "https://.../immagine.jpg",
            ...
        }

    Non filtra formato o dimensione: accetta jpg, png, webp, gif, ecc.
    In caso di errore WordPress, ritorna mappa vuota e lo script userà
    il fallback COVER_URL_TEMPLATE.
    """
    if not WORDPRESS_COVER_ENABLED:
        logger.info("WordPress cover disabilitate: WORDPRESS_COVER_ENABLED=0")
        return {}

    cover_by_ean: Dict[str, str] = {}

    total_seen = 0
    total_with_ean = 0
    duplicates = 0
    without_url = 0
    without_ean = 0

    page = 1

    session = requests.Session()
    session.headers.update({
        "User-Agent": "Mozilla/5.0 Python Shopify Sync WordPress Cover Indexer"
    })

    logger.info(f"Indicizzo immagini WordPress da: {WORDPRESS_MEDIA_SEARCH_URL}")

    wp_index_start = time.monotonic()
    while True:
        if time.monotonic() - wp_index_start >= max(1, WORDPRESS_INDEX_TIMEOUT_SECONDS):
            logger.warning("Tempo massimo indicizzazione WordPress raggiunto: uso il fallback per le copertine mancanti")
            break
        params = {
            "media_type": "image",
            "per_page": WORDPRESS_PER_PAGE,
            "page": page,
            "_fields": "id,source_url,slug,title,alt_text,guid,mime_type",
        }

        try:
            resp = session.get(
                WORDPRESS_MEDIA_SEARCH_URL,
                params=params,
                timeout=WORDPRESS_REQUEST_TIMEOUT,
            )

            # WordPress spesso risponde 400 quando la pagina non esiste più
            if resp.status_code == 400 and page > 1:
                break

            resp.raise_for_status()
            data = resp.json()

        except Exception as e:
            logger.warning(
                f"Errore lettura immagini WordPress pagina {page}: {e}. "
                f"Uso fallback COVER_URL_TEMPLATE per le immagini non indicizzate."
            )
            break

        if not isinstance(data, list):
            logger.warning(
                "Risposta WordPress non valida: attesa lista JSON. "
                "Uso fallback COVER_URL_TEMPLATE."
            )
            break

        if not data:
            break

        for item in data:
            if WORDPRESS_LIMIT_MEDIA and total_seen >= WORDPRESS_LIMIT_MEDIA:
                logger.info("Raggiunto WORDPRESS_LIMIT_MEDIA")
                break

            total_seen += 1

            image_url = get_wordpress_image_url(item)
            if not image_url:
                without_url += 1
                continue

            ean = extract_ean_from_wordpress_media(item)
            if not ean:
                without_ean += 1
                continue

            total_with_ean += 1

            # In caso di duplicati tiene la prima immagine trovata.
            if ean in cover_by_ean:
                duplicates += 1
                continue

            cover_by_ean[ean] = image_url

        logger.info(
            f"WordPress pagina {page} letta | "
            f"media viste={total_seen} | "
            f"EAN indicizzati={len(cover_by_ean)}"
        )

        if WORDPRESS_LIMIT_MEDIA and total_seen >= WORDPRESS_LIMIT_MEDIA:
            break

        total_pages_raw = resp.headers.get("X-WP-TotalPages") or "0"
        try:
            total_pages = int(total_pages_raw)
        except ValueError:
            total_pages = 0

        if total_pages and page >= total_pages:
            break

        if not total_pages and len(data) < WORDPRESS_PER_PAGE:
            break

        page += 1

        if WORDPRESS_SLEEP_BETWEEN_PAGES_MS > 0:
            time.sleep(WORDPRESS_SLEEP_BETWEEN_PAGES_MS / 1000.0)

    logger.info(
        "Indice immagini WordPress completato | "
        f"media viste={total_seen} | "
        f"con EAN={total_with_ean} | "
        f"EAN unici={len(cover_by_ean)} | "
        f"duplicati={duplicates} | "
        f"senza EAN={without_ean} | "
        f"senza URL={without_url}"
    )

    return cover_by_ean


# =============================================================================
# Mapping WinVaria -> Shopify + hash
# =============================================================================

EXTERNAL_ID_NAMESPACE = "custom"
EXTERNAL_ID_KEY = "external_id"

def text_to_shopify_html(value: Any) -> str:

    text = str(value or "")
    text = text.replace("\x00", "")
    text = text.replace("\r\n", "\n").replace("\r", "\n").strip()

    if not text:
        return ""

    paragraphs = re.split(r"\n[ \t]*\n+", text)
    html_paragraphs: List[str] = []

    for paragraph in paragraphs:
        lines = paragraph.split("\n")
        escaped_lines = [html.escape(line.strip()) for line in lines]
        escaped_lines = [line for line in escaped_lines if line != ""]

        if escaped_lines:
            html_paragraphs.append("<p>" + "<br>".join(escaped_lines) + "</p>")

    return "\n".join(html_paragraphs)




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

    # Se ci sono sia virgola sia punto, considera l'ultimo separatore come
    # separatore decimale e l'altro come separatore delle migliaia.
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


def build_productset_input_from_testi_row(
    row: Dict[str, Any],
    *,
    include_files: bool = True,
    wordpress_cover_by_ean: Optional[Dict[str, str]] = None,
) -> Dict[str, Any]:
    titolo = clean_str(get_ci(row, "TITOLO", default=""))
    sottotitolo = clean_str(get_ci(row, "Sotto_tit", "SOTTO_TIT", "SOTTOTIT", "SOTTOTITOLO", default=""))
    autore = clean_str(get_ci(row, "AUTORE", default=""))
    editore = clean_str(get_ci(row, "EDITORE", default=""))
    categoria = clean_str(get_ci(row, "CATEGORIA", default=""))

    ean = clean_str(get_ci(row, "CODICE_EAN", default=""))
    prezzo = to_decimal(get_ci(row, "PREZZO_EUR", "Prezzo_eur", "prezzo_eur", "PREZZO", default=Decimal("0")))
    sconto_percentuale = to_decimal(get_ci(row, "SCONTO", "Sconto", "sconto", default=Decimal("0")))
    # Il campo SCONTO nel DBF contiene una percentuale, es. 5,04 = 5,04%.
    # Usiamo Decimal per non perdere precisione e tronchiamo solo alla fine a 2 decimali.
    sconto_percentuale = clamp_percent(sconto_percentuale)

    if prezzo < Decimal("0"):
        prezzo = Decimal("0")

    prezzo_finale = prezzo * (Decimal("1") - (sconto_percentuale / Decimal("100")))
    if prezzo_finale < Decimal("0"):
        prezzo_finale = Decimal("0")
    giacenza = get_ci(row, "GIACENTI", default=0) or 0

    note = clean_str(get_ci(row, "NOTE_TEXT", default=""))

    description_parts: List[str] = []
    if sottotitolo:
        description_parts.append(f"<p><strong>{html.escape(sottotitolo)}</strong></p>")
    if note:
        description_parts.append(text_to_shopify_html(note))

    description_html = "\n".join(description_parts) if description_parts else "<p></p>"

    sku = ean
    isbn = ean

    # -------------------------------------------------------------------------
    # COVER:
    # 1. prima scelta: immagine WordPress indicizzata tramite EAN
    # 2. fallback: comportamento precedente, cioè COVER_URL_TEMPLATE
    # -------------------------------------------------------------------------
    wp_cover_url = ""
    if isbn and wordpress_cover_by_ean:
        wp_cover_url = clean_str(wordpress_cover_by_ean.get(isbn))

    if wp_cover_url:
        cover_url = wp_cover_url
        wp_filename = extract_filename_from_url(wp_cover_url)
        cover_filename = wp_filename or f"copertina-{isbn}.jpg"
    else:
        cover_url = COVER_URL_TEMPLATE.format(isbn=isbn) if isbn else None
        cover_filename = f"copertina-{isbn}.jpg" if isbn else "copertina.jpg"

    cover_alt = f"Copertina del libro {titolo}".strip()

    tags = list(dict.fromkeys([categoria] if categoria else []))
    if DEFAULT_TAGS:
        tags = list(dict.fromkeys(tags + DEFAULT_TAGS))

    # ATTENZIONE:
    # Shopify rifiuta i metafield con value vuoto.
    # Per questo i campi opzionali vengono aggiunti solo se valorizzati.
    metafields: List[Dict[str, Any]] = []

    metafields.append({
        "namespace": "custom",
        "key": "autore",
        "type": "single_line_text_field",
        "value": autore if autore else "Autore sconosciuto"
    })

    if sottotitolo:
        metafields.append({
            "namespace": "custom",
            "key": "sottotitolo",
            "type": "single_line_text_field",
            "value": sottotitolo
        })

    metafields.append({
        "namespace": "custom",
        "key": "isbn",
        "type": "single_line_text_field",
        "value": isbn if isbn else "-"
    })

    metafields.append({
        "namespace": "custom",
        "key": "categoria",
        "type": "single_line_text_field",
        "value": categoria if categoria else "Categoria sconosciuta"
    })

    metafields.append({
        "namespace": EXTERNAL_ID_NAMESPACE,
        "key": EXTERNAL_ID_KEY,
        "value": ean
    })

    variant_obj: Dict[str, Any] = {
        "sku": sku,
        # Prezzo effettivo Shopify: prezzo DBF meno sconto percentuale.
        "price": money_str(prezzo_finale),
        "optionValues": [{"optionName": "Title", "name": "Default Title"}],
    }

    # Se c'è uno sconto percentuale, Shopify mostra il prezzo originario come
    # prezzo di confronto / prezzo barrato.
    if sconto_percentuale > 0 and prezzo_finale < prezzo:
        variant_obj["compareAtPrice"] = money_str(prezzo)

    if is_scolastico(row):
        # Prodotti SCOLASTICO: scorte non monitorate e prodotto sempre acquistabile.
        # - tracked=False disattiva il tracking inventario sull'inventory item.
        # - inventoryPolicy=CONTINUE consente l'acquisto anche senza disponibilità.
        # - non inviamo inventoryQuantities perché la scorta non deve essere gestita.
        variant_obj["inventoryItem"] = {"tracked": False}
        variant_obj["inventoryPolicy"] = "CONTINUE"
    else:
        variant_obj["inventoryQuantities"] = [{
            "locationId": SHOPIFY_LOCATION_ID,
            "name": "available",
            "quantity": int(giacenza),
        }]

    input_obj: Dict[str, Any] = {
        "title": titolo or f"Libro {ean}",
        "descriptionHtml": description_html,
        "vendor": editore or "Editore non specificato",
        "productType": categoria or "Libro",
        "tags": tags,
        "category": "gid://shopify/TaxonomyCategory/me-1-3",
        "status": "ACTIVE",
        "productOptions": [{
            "name": "Title",
            "position": 1,
            "values": [{"name": "Default Title"}]
        }],
        "variants": [variant_obj],
        "metafields": metafields,
    }

    # IMPORTANTE:
    # L'immagine va inviata a Shopify solo in creazione.
    # In modifica NON bisogna passare il campo "files", così Shopify non tocca
    # l'immagine già presente sul prodotto.
    #
    # Se esiste immagine WordPress per l'EAN, usa quella.
    # Altrimenti mantiene il comportamento precedente con COVER_URL_TEMPLATE.
    if include_files and cover_url:
        input_obj["files"] = [{
            "originalSource": cover_url,
            "filename": cover_filename,
            "alt": cover_alt
        }]

    return input_obj


def get_row_stock_quantity(row: Dict[str, Any]) -> int:
    return int(get_ci(row, "GIACENTI", default=0) or 0)


def is_scolastico(row: Dict[str, Any]) -> bool:
    # Il valore arriva da tipologie.dbf ed è salvato in CATEGORIA.
    return clean_str(get_ci(row, "CATEGORIA", default="")).upper() == "SCOLASTICO"


def compute_row_hash(row: Dict[str, Any]) -> str:
    # Per i prodotti già esistenti il monitoraggio del cambiamento deve
    # dipendere solo dalla giacenza. Titolo, descrizione, prezzo, autore,
    # categoria, note, immagini e metafield non devono più causare update
    # del prodotto su Shopify.
    payload = {
        "ean": clean_str(get_ci(row, "CODICE_EAN", default="")),
        # Per SCOLASTICO la scorta non è monitorata su Shopify, quindi la
        # giacenza del DBF non deve generare update futuri.
        "giacenza": None if is_scolastico(row) else get_row_stock_quantity(row),
    }
    s = json.dumps(payload, ensure_ascii=False, sort_keys=True, separators=(",", ":"))
    return hashlib.sha256(s.encode("utf-8")).hexdigest()


def pick_ids_from_product_node(product: Dict[str, Any], sku: str) -> Tuple[Optional[str], Optional[str], Optional[str]]:
    if not product:
        return None, None, None

    product_id = product.get("id")
    variants = (((product.get("variants") or {}).get("nodes")) or [])
    chosen = None
    sku = (sku or "").strip()

    for v in variants:
        if (v or {}).get("sku") == sku:
            chosen = v
            break

    # Mai ripiegare sulla prima variante: potrebbe appartenere a un altro EAN.
    if not chosen:
        return product_id, None, None

    variant_id = (chosen or {}).get("id")
    inventory_item_id = ((chosen or {}).get("inventoryItem") or {}).get("id")
    return product_id, variant_id, inventory_item_id


# =============================================================================
# SQLite state (batch upsert)
# =============================================================================

class SyncState:
    def __init__(self, path: str, *, read_only: bool = False):
        self.path = path
        if read_only:
            # Anche la modalità check lascia invariato il file SQLite.
            self.conn = sqlite3.connect(Path(path).resolve().as_uri() + "?mode=ro", uri=True)
            self.conn.execute("PRAGMA query_only=ON;")
        else:
            db_dir = os.path.dirname(os.path.abspath(path))
            if db_dir:
                os.makedirs(db_dir, exist_ok=True)
            self.conn = sqlite3.connect(path)
            self.conn.execute("PRAGMA journal_mode=WAL;")
            self.conn.execute("PRAGMA synchronous=NORMAL;")
            self._init_schema()

    def _init_schema(self) -> None:
        self.conn.execute("""
        CREATE TABLE IF NOT EXISTS items (
            ean TEXT PRIMARY KEY,
            row_hash TEXT NOT NULL,
            product_id TEXT,
            variant_id TEXT,
            inventory_item_id TEXT,
            last_seen_run INTEGER NOT NULL,
            last_synced_at TEXT NOT NULL
        );
        """)
        self.conn.execute("""
        CREATE TABLE IF NOT EXISTS runs (
            run_id INTEGER PRIMARY KEY AUTOINCREMENT,
            started_at TEXT NOT NULL,
            finished_at TEXT
        );
        """)
        self.conn.execute("CREATE INDEX IF NOT EXISTS idx_items_last_seen ON items(last_seen_run);")
        self.conn.execute("""
        CREATE TABLE IF NOT EXISTS sync_issues (
            ean TEXT PRIMARY KEY,
            reason TEXT NOT NULL,
            last_attempt_utc TEXT NOT NULL
        )""")
        self.conn.execute("""
        CREATE TABLE IF NOT EXISTS verified_links (
            ean TEXT PRIMARY KEY,
            inventory_item_id TEXT NOT NULL,
            verified_at TEXT NOT NULL
        )""")
        self.conn.execute("""
        CREATE TABLE IF NOT EXISTS missing_zeroed (
            ean TEXT PRIMARY KEY,
            inventory_item_id TEXT NOT NULL,
            zeroed_at TEXT NOT NULL
        )""")
        self.conn.commit()

    def start_run(self) -> int:
        now = datetime.datetime.now(datetime.UTC).isoformat()
        cur = self.conn.execute("INSERT INTO runs(started_at) VALUES (?)", (now,))
        self.conn.commit()
        return int(cur.lastrowid)

    def finish_run(self, run_id: int) -> None:
        now = datetime.datetime.now(datetime.UTC).isoformat()
        self.conn.execute("UPDATE runs SET finished_at=? WHERE run_id=?", (now, run_id))
        self.conn.commit()

    def load_all(self) -> Dict[str, Dict[str, Any]]:
        cur = self.conn.execute(
            "SELECT ean, row_hash, product_id, variant_id, inventory_item_id, last_seen_run FROM items"
        )
        out: Dict[str, Dict[str, Any]] = {}
        for ean, row_hash, product_id, variant_id, inventory_item_id, last_seen_run in cur.fetchall():
            out[ean] = {
                "row_hash": row_hash,
                "product_id": product_id,
                "variant_id": variant_id,
                "inventory_item_id": inventory_item_id,
                "last_seen_run": last_seen_run,
            }
        return out

    def upsert_items_many(
        self,
        rows: List[Tuple[int, str, str, Optional[str], Optional[str], Optional[str]]]
    ) -> None:
        if not rows:
            return

        now = datetime.datetime.now(datetime.UTC).isoformat()
        payload = [
            (ean, row_hash, product_id, variant_id, inventory_item_id, run_id, now)
            for (run_id, ean, row_hash, product_id, variant_id, inventory_item_id) in rows
        ]

        self.conn.executemany("""
        INSERT INTO items(ean, row_hash, product_id, variant_id, inventory_item_id, last_seen_run, last_synced_at)
        VALUES(?,?,?,?,?,?,?)
        ON CONFLICT(ean) DO UPDATE SET
            row_hash=excluded.row_hash,
            product_id=COALESCE(excluded.product_id, items.product_id),
            variant_id=COALESCE(excluded.variant_id, items.variant_id),
            inventory_item_id=COALESCE(excluded.inventory_item_id, items.inventory_item_id),
            last_seen_run=excluded.last_seen_run,
            last_synced_at=excluded.last_synced_at
        """, payload)
        self.conn.commit()


    def backup_once_for_upgrade(self) -> None:
        """Copia consistente fatta con SQLite backup API alla prima esecuzione v2."""
        marker = Path(self.path).with_name(".shopify_sync_upgrade_v2_backup_done")
        if marker.exists():
            return
        target = Path(self.path).with_name(
            "shopify_sync_state_pre_v2_" + datetime.datetime.now().strftime("%Y%m%d_%H%M%S") + ".sqlite"
        )
        dest = sqlite3.connect(str(target))
        try:
            self.conn.backup(dest)
        finally:
            dest.close()
        marker.write_text(str(target), encoding="utf-8")
        logger.info(f"Backup iniziale dello stato SQLite: {target}")

    def link_verified_recently(self, ean: str, inventory_item_id: Optional[str]) -> bool:
        if not inventory_item_id:
            return False
        row = self.conn.execute(
            "SELECT inventory_item_id, verified_at FROM verified_links WHERE ean=?", (ean,)
        ).fetchone()
        if not row or row[0] != inventory_item_id:
            return False
        try:
            ts = datetime.datetime.fromisoformat(row[1])
            return (datetime.datetime.now(datetime.UTC) - ts).total_seconds() <= max(0, VERIFY_LINK_TTL_DAYS) * 86400
        except (ValueError, TypeError):
            return False

    def mark_link_verified(self, ean: str, inventory_item_id: str) -> None:
        self.conn.execute("""
          INSERT INTO verified_links(ean, inventory_item_id, verified_at) VALUES (?,?,?)
          ON CONFLICT(ean) DO UPDATE SET
            inventory_item_id=excluded.inventory_item_id, verified_at=excluded.verified_at
        """, (ean, inventory_item_id, datetime.datetime.now(datetime.UTC).isoformat()))
        self.conn.commit()

    def issue_due(self, ean: str) -> bool:
        minutes = max(0, REPAIR_COOLDOWN_MINUTES)
        if minutes == 0:
            return True
        row = self.conn.execute(
            "SELECT last_attempt_utc FROM sync_issues WHERE ean=?", (ean,)
        ).fetchone()
        if not row:
            return True
        try:
            date = datetime.datetime.fromisoformat(row[0])
            now = datetime.datetime.now(datetime.UTC)
            return (now - date).total_seconds() >= minutes * 60
        except (TypeError, ValueError):
            return True

    def record_issue(self, ean: str, reason: str) -> None:
        now = datetime.datetime.now(datetime.UTC).isoformat()
        self.conn.execute("""
          INSERT INTO sync_issues(ean, reason, last_attempt_utc)
          VALUES (?, ?, ?)
          ON CONFLICT(ean) DO UPDATE SET
            reason=excluded.reason, last_attempt_utc=excluded.last_attempt_utc
        """, (ean, reason[:500], now))
        self.conn.commit()

    def clear_issue(self, ean: str) -> None:
        self.conn.execute("DELETE FROM sync_issues WHERE ean=?", (ean,))
        self.conn.commit()

    def detach_stale_missing_link(self, ean: str, old_inventory_id: str) -> bool:
        """Rimuove SOLO il collegamento locale vecchio e duplicato, non Shopify.

        Da chiamare dopo conferma positiva che lo stesso ID appartiene a un
        altro EAN ancora presente nel gestionale.
        """
        cur = self.conn.execute("""
          UPDATE items SET product_id=NULL, variant_id=NULL, inventory_item_id=NULL
          WHERE ean=? AND inventory_item_id=?
        """, (ean, old_inventory_id))
        self.conn.commit()
        return bool(cur.rowcount)

    def was_zeroed(self, ean: str) -> bool:
        return self.conn.execute("SELECT 1 FROM missing_zeroed WHERE ean=?", (ean,)).fetchone() is not None

    def mark_zeroed(self, ean: str, inventory_item_id: str) -> None:
        self.conn.execute("""
          INSERT INTO missing_zeroed(ean,inventory_item_id,zeroed_at) VALUES(?,?,?)
          ON CONFLICT(ean) DO UPDATE SET
            inventory_item_id=excluded.inventory_item_id,zeroed_at=excluded.zeroed_at
        """, (ean, inventory_item_id, datetime.datetime.now(datetime.UTC).isoformat()))
        self.conn.commit()

    def clear_zeroed_for_present(self, eans: Iterable[str]) -> None:
        self.conn.executemany("DELETE FROM missing_zeroed WHERE ean=?", ((e,) for e in eans))
        self.conn.commit()


# =============================================================================
# Lettura testi + categoria
# =============================================================================

def read_testi_records() -> List[Dict[str, Any]]:
    if not os.path.exists(DBF_PATH):
        raise FileNotFoundError(f"DBF non trovato: {DBF_PATH}")
    if not os.path.exists(FPT_PATH):
        raise FileNotFoundError(f"FPT non trovato: {FPT_PATH}")
    if not os.path.exists(TIPOLOGIE_DBF_PATH):
        raise FileNotFoundError(f"DBF tipologie non trovato: {TIPOLOGIE_DBF_PATH}")

    tipologie_map = load_tipologie_map(TIPOLOGIE_DBF_PATH)

    dbf = _read_all_bytes(DBF_PATH)
    fpt = _read_all_bytes(FPT_PATH)

    fields, header_len, record_len, num_records = parse_dbf_fields(dbf)
    field_offsets = build_field_offsets(fields)

    selected: List[Dict[str, Any]] = []
    for i in range(num_records):
        row = parse_record(dbf, field_offsets, header_len, record_len, i)
        if not row:
            continue

        raw_ean = clean_str(get_ci(row, "CODICE_EAN", default=""))

        if " " in raw_ean:
            logger.warning(
                "Record DBF ignorato: CODICE_EAN contiene spazi | EAN=%r | Titolo=%r",
                raw_ean, get_ci(row, "TITOLO", default="")
            )
            continue

        ean = raw_ean.strip()
        if not ean:
            continue

        row["CODICE_EAN"] = ean

        note_ptr = int(get_ci(row, "NOTE", default=0) or 0)
        row["NOTE_TEXT"] = read_fpt_memo(fpt, note_ptr) or ""

        id_tipo_raw = get_ci(row, "Id_tipo", "ID_TIPO", "IDTIPO", default=None)
        categoria = ""
        try:
            if id_tipo_raw is not None:
                categoria = tipologie_map.get(int(id_tipo_raw), "") or ""
        except (TypeError, ValueError):
            categoria = ""
        row["CATEGORIA"] = categoria.strip()

        selected.append(row)

        if LIMIT_RECORDS and len(selected) >= LIMIT_RECORDS:
            break

    return selected


# =============================================================================
# Bulk mutation strings
# =============================================================================

BULK_MUTATION_PRODUCT_SET = """
mutation ProductSetBulk($identifier: ProductSetIdentifiers, $input: ProductSetInput!) {
  productSet(identifier: $identifier, synchronous: false, input: $input) {
    product {
      id
      handle
      status
      variants(first: 5) { nodes { id sku inventoryItem { id } } }
    }
    userErrors { field message }
  }
}
""".strip()

BULK_MUTATION_PUBLISH = """
mutation PublishBulk($id: ID!, $input: [PublicationInput!]!) {
  publishablePublish(id: $id, input: $input) {
    userErrors { field message }
  }
}
""".strip()


def chunked(items: List[Any], size: int) -> Iterable[List[Any]]:
    for i in range(0, len(items), size):
        yield items[i:i + size]


def chunk_log(kind: str, idx: int, total: int, msg: str) -> None:
    logger.info(f"[{kind}] Chunk {idx}/{total} | {msg}")


def file_path_str(p: Path) -> str:
    return str(p.resolve())


def is_running_as_exe() -> bool:
    return bool(getattr(sys, "frozen", False))


# =============================================================================
# Riconciliazione sicura SKU/EAN <-> variante Shopify
# =============================================================================

QUERY_VARIANTS_BY_SKU = """
query VariantsBySku($search: String!, $after: String) {
  productVariants(first: 50, after: $after, query: $search) {
    nodes { id sku product { id } inventoryItem { id tracked } }
    pageInfo { hasNextPage endCursor }
  }
}
""".strip()


class AmbiguousSkuMatch(RuntimeError):
    pass


def find_variant_by_exact_sku(endpoint: str, ean: str) -> Optional[Dict[str, Any]]:
    """Cerca su Shopify per SKU, usa SOLO corrispondenze esatte e univoche.

    Se non c'e' un solo risultato certo, non modifica giacenze ne' ID locali.
    """
    safe_sku = ean.replace('\\', '\\\\').replace('"', '\\"')
    search = f'sku:"{safe_sku}"'
    after = None
    candidates: Dict[str, Dict[str, Any]] = {}
    for _ in range(5):  # massimo 250 varianti; oltre e' un caso ambiguo
        data = shopify_graphql(
            endpoint, SHOPIFY_ACCESS_TOKEN,
            QUERY_VARIANTS_BY_SKU, {"search": search, "after": after}
        )
        conn = (data.get("data") or {}).get("productVariants")
        if not isinstance(conn, dict):
            raise RuntimeError(f"Risposta productVariants non valida per EAN {ean}")
        for variant in conn.get("nodes") or []:
            if str(variant.get("sku") or "").strip() != ean:
                continue
            variant_id = variant.get("id")
            inv = variant.get("inventoryItem") or {}
            prod = variant.get("product") or {}
            if variant_id:
                candidates[variant_id] = {
                    "ean": ean,
                    "product_id": prod.get("id"),
                    "variant_id": variant_id,
                    "inventory_item_id": inv.get("id"),
                    "tracked": inv.get("tracked", False)
                }
        pg = conn.get("pageInfo") or {}
        if not pg.get("hasNextPage"):
            break
        after = pg.get("endCursor")
        if not after:
            raise RuntimeError("Risposta productVariants senza endCursor")
    else:
        raise AmbiguousSkuMatch(f"EAN {ean}: ricerca Shopify troppo ampia (>250 risultati)")

    if len(candidates) > 1:
        raise AmbiguousSkuMatch(
            f"EAN {ean}: {len(candidates)} varianti Shopify con lo stesso SKU, intervento manuale necessario"
        )
    return next(iter(candidates.values()), None)


def shared_inventory_ids(prev: Dict[str, Dict[str, Any]]) -> Dict[str, List[str]]:
    by_id: Dict[str, List[str]] = {}
    for ean, row in prev.items():
        inv_id = row.get("inventory_item_id")
        if inv_id:
            by_id.setdefault(inv_id, []).append(ean)
    return {inv: eans for inv, eans in by_id.items() if len(eans) > 1}


def prepare_inventory_row(item: Dict[str, Any], found: Dict[str, Any]) -> Dict[str, Any]:
    if not found.get("inventory_item_id") or not found.get("tracked"):
        raise ValueError(f"EAN {item['ean']}: variante senza inventario monitorato; nessuna scrittura")
    return {
        **item,
        "product_id": found["product_id"],
        "variant_id": found["variant_id"],
        "inventory_item_id": found["inventory_item_id"],
        "repaired": bool(item.get("inventory_item_id") and item["inventory_item_id"] != found["inventory_item_id"]),
        "verified": True,
        "needs_verification": False
    }


def save_success(state: SyncState, run_id: int, batch: List[Dict[str, Any]]) -> None:
    state.upsert_items_many([
        (run_id, i["ean"], i["row_hash"], i.get("product_id"),
         i.get("variant_id"), i.get("inventory_item_id")) for i in batch
    ])
    for i in batch:
        if i.get("verified") and i.get("inventory_item_id"):
            state.mark_link_verified(i["ean"], i["inventory_item_id"])
        state.clear_issue(i["ean"])


def inventory_batch_update_resilient(
    endpoint: str, state: SyncState, run_id: int,
    batch: List[Dict[str, Any]], stats: Dict[str, int], *, stage: str = "inventory"
) -> None:
    """Divide i batch problematici, isola e riconcilia l'item errato per SKU."""
    if not batch:
        return

    def send(items: List[Dict[str, Any]], *, repair_allowed: bool = True) -> None:
        if not items:
            return
        keys = [i["inventory_item_id"] for i in items]
        if len(keys) != len(set(keys)):
            if len(items) > 1:
                mid = len(items) // 2
                send(items[:mid], repair_allowed=repair_allowed)
                send(items[mid:], repair_allowed=repair_allowed)
                return
            raise RuntimeError("InventoryItem ID duplicato impossibile da isolare")

        reference = f"gid://erp-connector/SyncJob/run-{run_id}-{stage}"
        res = inventory_set_available_batch(
            endpoint, [(i["inventory_item_id"], i["quantity"]) for i in items],
            reference_document_uri=reference
        )
        errors = inventory_errors(res)
        if not errors:
            save_success(state, run_id, items)
            stats["updated"] += len(items)
            stats["repaired"] += sum(1 for i in items if i.get("repaired"))
            return

        # Errori globali (permessi, sede, payload) non si risolvono spezzando.
        per_item = all(
            len(e.get("field") or []) >= 4 and
            (e.get("field") or [])[1] == "quantities" and
            str((e.get("field") or [])[2]).isdigit()
            for e in errors
        )
        if not per_item:
            raise RuntimeError(f"Errore inventario generale: {json.dumps(errors, ensure_ascii=False)}")
        if len(items) > 1:
            # API puo' avere applicato righe valide: il SET assoluto e' ripetibile.
            mid = len(items) // 2
            send(items[:mid], repair_allowed=repair_allowed)
            send(items[mid:], repair_allowed=repair_allowed)
            return

        item = items[0]
        ean = item["ean"]
        codes = {str(e.get("code") or "") for e in errors}
        if "INVALID_INVENTORY_ITEM" in codes and repair_allowed:
            logger.warning("EAN %s: InventoryItem ID rifiutato, cerco SKU esatto su Shopify", ean)
            try:
                resolved = find_variant_by_exact_sku(endpoint, ean)
                if resolved is None:
                    raise LookupError("Nessuna variante Shopify con SKU esatto")
                candidate = prepare_inventory_row(item, resolved)
                if candidate["inventory_item_id"] == item["inventory_item_id"]:
                    raise RuntimeError("Shopify restituisce ancora lo stesso InventoryItem ID rifiutato")
                # Mai usare per questo SKU un ID che appartiene a un altro
                # SKU in una lista gia' pronta: si evitano sovrascritture.
                logger.info("EAN %s: riparazione ID %s -> %s", ean,
                            item["inventory_item_id"], candidate["inventory_item_id"])
                send([candidate], repair_allowed=False)
                return
            except (LookupError, ValueError, AmbiguousSkuMatch) as exc:
                reason = str(exc)
            # Gli errori API non vengono nascosti: sono fatali per il run.
            except ShopifyGraphQLApplicationError:
                raise
            except requests.RequestException:
                raise
            except Exception as exc:
                reason = str(exc)
        else:
            reason = json.dumps(errors, ensure_ascii=False)

        log_error(f"EAN {ean} NON aggiornato: {reason}", {
            "ean": ean, "quantity": item.get("quantity"),
            "inventory_item_id": item.get("inventory_item_id"), "codes": list(codes)
        })
        state.record_issue(ean, reason)
        stats["failed"] += 1

    send(batch)


def main() -> None:
    logger.info("=== AVVIO SYNC SHOPIFY - VERSIONE SICURA V2 ===")
    logger.info(f"APP_DIR: {SCRIPT_DIR} | config: {ENV_PATH}")
    logger.info(f"State DB: {STATE_DB_PATH} | log: {LOG_PATH}")
    logger.info("WordPress cover: %s | azzera EAN scomparsi: %s | DRY_RUN: %s",
                WORDPRESS_COVER_ENABLED, ZERO_MISSING_PRODUCTS, DRY_RUN)

    records = read_testi_records()
    logger.info("Record validi letti dal DBF: %s", len(records))
    current_rows = {clean_str(get_ci(r, "CODICE_EAN", default="")): r for r in records}
    if len(current_rows) != len(records):
        logger.warning("EAN duplicati nel DBF: %s; prevale l'ultima occorrenza",
                       len(records) - len(current_rows))

    state = SyncState(STATE_DB_PATH, read_only=DRY_RUN)
    try:
        prev = state.load_all()
        dup = shared_inventory_ids(prev)
        for inv_id, eans in dup.items():
            logger.warning("ID inventario DUPLICATO nello stato: %s -> %s", inv_id, eans)

        newly_seen = [ean for ean in current_rows if ean not in prev]
        changed = []
        unchanged_rows = []
        dup_eans = {ean for group in dup.values() for ean in group}
        for ean, row in current_rows.items():
            old = prev.get(ean)
            if old is None:
                continue
            h = compute_row_hash(row)
            # I duplicati devono essere verificati anche se la scorta non varia.
            if not is_scolastico(row) and (old["row_hash"] != h or ean in dup_eans):
                changed.append({
                    "ean": ean, "row_hash": h, "quantity": get_row_stock_quantity(row),
                    "product_id": old.get("product_id"),
                    "variant_id": old.get("variant_id"),
                    "inventory_item_id": old.get("inventory_item_id"),
                    "needs_verification": ean in dup_eans
                })
            else:
                unchanged_rows.append((ean, h, old))

        missing = [ean for ean in prev if ean not in current_rows]
        logger.info("Nuovi rispetto a SQLite: %d | giacenze da aggiornare/verificare: %d | "
                    "invariati: %d | EAN scomparsi: %d", len(newly_seen), len(changed),
                    len(unchanged_rows), len(missing))

        if DRY_RUN:
            logger.info("CHECK SOLA LETTURA: non contatto Shopify, non modifico SQLite.")
            for item in changed[:25]:
                logger.info("DA AGGIORNARE: EAN=%s qta=%s item=%s", item["ean"],
                            item["quantity"], item.get("inventory_item_id"))
            logger.info("=== CHECK COMPLETATO ===")
            return

        if not SHOPIFY_SHOP or not SHOPIFY_ACCESS_TOKEN or not SHOPIFY_LOCATION_ID:
            raise RuntimeError("config.env: SHOPIFY_SHOP, SHOPIFY_ACCESS_TOKEN e SHOPIFY_LOCATION_ID obbligatori")
        if not BULK_ENABLED:
            raise RuntimeError("BULK_ENABLED=0: questa versione richiede BULK_ENABLED=1")
        if len(newly_seen) > max(0, MAX_NEW_PRODUCTS_PER_RUN):
            raise RuntimeError(
                f"Bloccata creazione: {len(newly_seen)} EAN sconosciuti a SQLite "
                f"(soglia {MAX_NEW_PRODUCTS_PER_RUN}). Controlla se e' il DB di stato corretto."
            )

        endpoint = f"https://{SHOPIFY_SHOP}/admin/api/{SHOPIFY_API_VERSION}/graphql.json"
        state.backup_once_for_upgrade()
        run_id = state.start_run()
        stats = {"created": 0, "updated": 0, "repaired": 0,
                 "unchanged": len(unchanged_rows), "zeroed": 0,
                 "failed": 0, "deferred": 0, "skipped_zero": 0}
        try:
            # Lo stato degli invariati puo' avanzare; quelli falliti non vengono
            # mai contrassegnati come sincronizzati.
            state.clear_zeroed_for_present(current_rows.keys())
            for part in chunked(unchanged_rows, SQLITE_UPSERT_BATCH):
                state.upsert_items_many([
                    (run_id, ean, h, o.get("product_id"), o.get("variant_id"), o.get("inventory_item_id"))
                    for ean, h, o in part
                ])

            # Non creiamo un EAN gia' esistente su Shopify: lo riagganciamo allo stato.
            truly_new = []
            for ean in newly_seen:
                row = current_rows[ean]
                h = compute_row_hash(row)
                try:
                    found = find_variant_by_exact_sku(endpoint, ean)
                    if found:
                        if is_scolastico(row):
                            state.upsert_items_many([(
                                run_id, ean, h, found["product_id"],
                                found["variant_id"], found["inventory_item_id"]
                            )])
                            logger.info("EAN scolastico %s: collegato a prodotto Shopify gia' esistente", ean)
                        else:
                            changed.append(prepare_inventory_row({
                                "ean": ean, "row_hash": h,
                                "quantity": get_row_stock_quantity(row)
                            }, found))
                            logger.info("EAN %s: presente su Shopify, collego senza ricrearlo", ean)
                    else:
                        truly_new.append({"ean": ean, "row_hash": h, "row": row})
                except (AmbiguousSkuMatch, ValueError) as exc:
                    logger.error("EAN %s non creato/collegato: %s", ean, exc)
                    state.record_issue(ean, str(exc))
                    stats["failed"] += 1

            # Solo i nuovi veri richiedono l'indicizzazione delle immagini.
            covers = {}
            if truly_new and WORDPRESS_COVER_ENABLED:
                covers = load_wordpress_cover_urls_by_ean()

            publish_queue: List[str] = []
            logger.info("Nuovi veri da creare con productSet: %d", len(truly_new))
            total = (len(truly_new) + BULK_CHUNK_SIZE - 1) // BULK_CHUNK_SIZE
            for ix, part in enumerate(chunked(truly_new, BULK_CHUNK_SIZE), 1):
                inputs = []
                for item in part:
                    ean = item["ean"]
                    inputs.append(json.dumps({
                        "identifier": {"customId": {
                            "namespace": EXTERNAL_ID_NAMESPACE,
                            "key": EXTERNAL_ID_KEY, "value": ean
                        }},
                        "input": build_productset_input_from_testi_row(
                            item["row"], include_files=True, wordpress_cover_by_ean=covers
                        )
                    }, ensure_ascii=False, separators=(",", ":")))
                staged = staged_upload_jsonl(
                    endpoint, SHOPIFY_ACCESS_TOKEN,
                    filename=f"productset_vars_run{run_id}_chunk{ix}.jsonl",
                    jsonl_bytes=("\n".join(inputs) + "\n").encode("utf-8")
                )
                op_id = bulk_run_mutation(
                    endpoint, SHOPIFY_ACCESS_TOKEN, mutation_str=BULK_MUTATION_PRODUCT_SET,
                    staged_upload_path=staged, client_identifier=f"productset_run{run_id}_chunk{ix}"
                )
                op = poll_bulk_operation(endpoint, SHOPIFY_ACCESS_TOKEN, op_id)
                if (op.get("status") or "").upper() != "COMPLETED" or not op.get("url"):
                    raise RuntimeError(f"Bulk productSet non completata: {op}")

                done_lines = set()
                for output in iter_jsonl_from_url(op["url"]):
                    num = output.get("__lineNumber")
                    if not isinstance(num, int) or not 0 <= num < len(part) or num in done_lines:
                        log_error("Risposta bulk senza __lineNumber valido/univoco: non associo ID", output)
                        continue
                    done_lines.add(num)
                    item = part[num]
                    ean = item["ean"]
                    payload = ((output.get("data") or {}).get("productSet") or {})
                    errors = payload.get("userErrors") or []
                    if errors:
                        log_error(f"productSet non riuscito EAN={ean}", errors)
                        state.record_issue(ean, json.dumps(errors, ensure_ascii=False))
                        stats["failed"] += 1
                        continue
                    prod = payload.get("product") or {}
                    pid, vid, iid = pick_ids_from_product_node(prod, ean)
                    if not (pid and vid and iid):
                        reason = "productSet senza corrispondenza variante SKU esatta / ID valido"
                        log_error(f"EAN {ean}: {reason}", output)
                        state.record_issue(ean, reason)
                        stats["failed"] += 1
                        continue
                    state.upsert_items_many([(run_id, ean, item["row_hash"], pid, vid, iid)])
                    state.clear_issue(ean)
                    stats["created"] += 1
                    if PUBLICATION_IDS:
                        publish_queue.append(pid)
                for i, item in enumerate(part):
                    if i not in done_lines:
                        state.record_issue(item["ean"], "Risposta assente dall'output bulk")
                        stats["failed"] += 1
                        logger.error("EAN %s: risultato bulk mancante", item["ean"])
                logger.info("[productSet] Chunk %s/%s terminato", ix, total)

            # Per ogni ID duplicato cerchiamo prima il vero SKU, mai usarlo
            # direttamente perche' potrebbe rappresentare un altro articolo.
            pending = []
            for item in changed:
                ean = item["ean"]
                if not state.issue_due(ean):
                    stats["deferred"] += 1
                    continue
                # Alla prima modifica di scorta controlla la relazione SKU -> ID
                # anche se l'ID e' ancora tecnicamente valido (evita scorte
                # assegnate al prodotto sbagliato). Cache per 7 giorni.
                needs_validation = (item.get("needs_verification") or
                                    not state.link_verified_recently(ean, item.get("inventory_item_id")))
                if needs_validation:
                    try:
                        found = find_variant_by_exact_sku(endpoint, ean)
                        if not found:
                            raise LookupError("EAN/SKU esatto non trovato su Shopify")
                        item = prepare_inventory_row(item, found)
                        logger.info("EAN %s: variante verificata tramite SKU; ID=%s",
                                    ean, item["inventory_item_id"])
                    except (LookupError, ValueError, AmbiguousSkuMatch) as exc:
                        log_error(f"EAN {ean}: impossibile verificare ID Shopify", str(exc))
                        state.record_issue(ean, str(exc))
                        stats["failed"] += 1
                        continue
                pending.append(item)

            # Conflitti interni al batch: ricerchiamo SKU per entrambi, poi
            # blocchiamo quelli che ancora condividono l'ID.
            by_inv: Dict[str, List[Dict[str, Any]]] = {}
            for item in pending:
                by_inv.setdefault(item["inventory_item_id"], []).append(item)
            if any(len(v) > 1 for v in by_inv.values()):
                for inv_id, records_with_id in list(by_inv.items()):
                    if len(records_with_id) < 2:
                        continue
                    for item in records_with_id:
                        if item.get("repaired"):
                            continue
                        try:
                            variant = find_variant_by_exact_sku(endpoint, item["ean"])
                            if not variant:
                                raise LookupError("SKU non trovato")
                            candidate = prepare_inventory_row(item, variant)
                            item.update(candidate)
                        except (LookupError, ValueError, AmbiguousSkuMatch) as exc:
                            state.record_issue(item["ean"], str(exc))
                            item["unsafe"] = True
                counts: Dict[str, int] = {}
                for item in pending:
                    counts[item["inventory_item_id"]] = counts.get(item["inventory_item_id"], 0) + 1
                safe = []
                for item in pending:
                    if item.get("unsafe") or counts[item["inventory_item_id"]] > 1:
                        logger.error("EAN %s BLOCCATO: ID inventario condiviso/incerto", item["ean"])
                        state.record_issue(item["ean"], "ID inventario condiviso o incerto")
                        stats["failed"] += 1
                    else:
                        safe.append(item)
                pending = safe

            logger.info("Aggiornamento scorte: %d EAN in %d batch", len(pending),
                        (len(pending) + INVENTORY_ZERO_BATCH - 1) // INVENTORY_ZERO_BATCH)
            for ix, part in enumerate(chunked(pending, INVENTORY_ZERO_BATCH), 1):
                t = time.perf_counter()
                inventory_batch_update_resilient(endpoint, state, run_id, part, stats)
                logger.info("[inventory] Batch %d completato in %.2fs", ix, time.perf_counter() - t)

            # Pulisce i vecchi link duplicati per EAN scomparsi SOLO dopo
            # aver dimostrato che quell'InventoryItem appartiene allo SKU attivo.
            # Non elimina mai prodotti o varianti da Shopify.
            for old_inv, group in dup.items():
                active_eans = [e for e in group if e in current_rows]
                missing_eans = [e for e in group if e not in current_rows]
                if len(active_eans) == 1 and missing_eans:
                    if state.link_verified_recently(active_eans[0], old_inv):
                        for missing_ean in missing_eans:
                            if state.detach_stale_missing_link(missing_ean, old_inv):
                                logger.warning(
                                    "EAN scomparso %s: rimosso soltanto il collegamento SQLite duplicato; "
                                    "Shopify non modificato", missing_ean
                                )

            if PUBLICATION_IDS and publish_queue:
                pub_total = (len(publish_queue) + BULK_CHUNK_SIZE - 1) // BULK_CHUNK_SIZE
                for ix, pids in enumerate(chunked(publish_queue, BULK_CHUNK_SIZE), 1):
                    lines = [json.dumps({
                        "id": pid,
                        "input": [{"publicationId": pub} for pub in PUBLICATION_IDS]
                    }, ensure_ascii=False, separators=(",", ":")) for pid in pids]
                    staged = staged_upload_jsonl(
                        endpoint, SHOPIFY_ACCESS_TOKEN,
                        filename=f"publish_vars_run{run_id}_chunk{ix}.jsonl",
                        jsonl_bytes=("\n".join(lines) + "\n").encode("utf-8")
                    )
                    op_id = bulk_run_mutation(
                        endpoint, SHOPIFY_ACCESS_TOKEN, mutation_str=BULK_MUTATION_PUBLISH,
                        staged_upload_path=staged, client_identifier=f"publish_run{run_id}_chunk{ix}"
                    )
                    op = poll_bulk_operation(endpoint, SHOPIFY_ACCESS_TOKEN, op_id)
                    if (op.get("status") or "").upper() != "COMPLETED" or not op.get("url"):
                        raise RuntimeError(f"Bulk publish non completata: {op}")
                    for output in iter_jsonl_from_url(op["url"]):
                        errors = (((output.get("data") or {}).get("publishablePublish") or {})
                                  .get("userErrors") or [])
                        if errors:
                            log_error("publishablePublish userErrors", errors)
                            stats["failed"] += 1
                    logger.info("[publish] Chunk %d/%d terminato", ix, pub_total)

            # Evitiamo azzeramenti pericolosi al primo deploy. Se l'utente
            # li riabilita, ogni SKU deve essere unico e verificato su Shopify.
            if missing and not ZERO_MISSING_PRODUCTS:
                stats["skipped_zero"] = len(missing)
                logger.warning("Azzeramento di %d EAN scomparsi DISABILITATO per sicurezza. "
                               "Per abilitarlo: ZERO_MISSING_PRODUCTS=1 in config.env", len(missing))
            if missing and ZERO_MISSING_PRODUCTS:
                ids_owned_by_active = {
                    r.get("inventory_item_id") for ean, r in state.load_all().items()
                    if ean in current_rows and r.get("inventory_item_id")
                }
                for ean in missing:
                    if state.was_zeroed(ean):
                        continue
                    old = prev[ean]
                    # Nessun azzeramento se un vecchio ID compare anche altrove.
                    if old.get("inventory_item_id") in dup:
                        stats["skipped_zero"] += 1
                        logger.warning("EAN scomparso %s: ID condiviso, azzeramento BLOCCATO", ean)
                        continue
                    if not state.issue_due(ean):
                        stats["deferred"] += 1
                        continue
                    try:
                        found = find_variant_by_exact_sku(endpoint, ean)
                        if not found:
                            # Non esiste SKU Shopify da azzerare; registriamo la
                            # condizione, non tocchiamo altri SKU.
                            state.record_issue(ean, "SKU assente da Shopify")
                            stats["skipped_zero"] += 1
                            continue
                        inv = found["inventory_item_id"]
                        if not inv or not found.get("tracked") or inv in ids_owned_by_active:
                            raise ValueError("InventoryItem senza tracking o condiviso con EAN attivo")
                        zero = inventory_set_available_batch(
                            endpoint, [(inv, 0)],
                            reference_document_uri=f"gid://erp-connector/SyncJob/run-{run_id}-missing"
                        )
                        errs = inventory_errors(zero)
                        if errs:
                            raise ValueError(json.dumps(errs, ensure_ascii=False))
                        state.mark_zeroed(ean, inv)
                        state.clear_issue(ean)
                        stats["zeroed"] += 1
                    except (ValueError, AmbiguousSkuMatch) as exc:
                        log_error(f"EAN scomparso {ean}: azzeramento NON eseguito", str(exc))
                        state.record_issue(ean, str(exc))
                        stats["failed"] += 1

        except Exception:
            logger.exception("ERRORE FATALE durante sincronizzazione")
            raise
        finally:
            state.finish_run(run_id)
            logger.info("=== RIEPILOGO RUN %s ===", run_id)
            for k, v in stats.items():
                logger.info("%-15s %d", k, v)
            logger.info("State DB: %s | log: %s", STATE_DB_PATH, LOG_PATH)
        if stats["failed"] or stats["deferred"]:
            logger.warning("=== SYNC PARZIALE: %d falliti, %d rinviati ===",
                           stats["failed"], stats["deferred"])
            # EXIT NON ZERO: Task Scheduler puo' notificare/sorvegliare il guasto.
            raise RuntimeError("Sincronizzazione incompleta: consultare shopify_sync.log")
        logger.info("=== SYNC COMPLETATA SENZA ERRORI ===")
    finally:
        state.conn.close()


def try_acquire_single_instance_lock():
    """Evita due sincronizzazioni contemporanee, anche se avviate dal Task Scheduler."""
    lock_file = open(APP_DIR / "shopify_sync.lock", "a+b")
    try:
        if os.name == "nt":
            import msvcrt
            lock_file.seek(0)
            if not lock_file.read(1):
                lock_file.write(b"0")
                lock_file.flush()
            lock_file.seek(0)
            msvcrt.locking(lock_file.fileno(), msvcrt.LK_NBLCK, 1)
        else:
            import fcntl
            fcntl.flock(lock_file.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
    except OSError as e:
        lock_file.close()
        # Blocco gia' acquisito da un'altra istanza.
        if getattr(e, "winerror", None) in (32, 33, 36) or getattr(e, "errno", None) in (11, 13):
            return None
        raise
    return lock_file


if __name__ == "__main__":
    instance_lock = try_acquire_single_instance_lock()
    if instance_lock is None:
        logger.error("Sync NON avviata: un'altra istanza dello script e' ancora in esecuzione")
        sys.exit(2)

    try:
        main()
    except Exception:
        import traceback

        print("\n=== ERRORE FATALE ===")
        traceback.print_exc()

        try:
            logger.exception("Eccezione non gestita")
        except Exception:
            pass

        raise
    finally:
        instance_lock.close()  # Il sistema operativo rilascia automaticamente il lock.
