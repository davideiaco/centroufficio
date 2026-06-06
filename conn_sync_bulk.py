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
import sqlite3
import requests
import time
import logging
from decimal import Decimal, InvalidOperation, ROUND_HALF_UP
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
        headers = {
            "Content-Type": "application/json",
            "X-Shopify-Access-Token": current_token
        }

        try:
            resp = requests.post(
                endpoint,
                headers=headers,
                json={"query": query, "variables": variables},
                timeout=180
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
                        "status": getattr(getattr(e, "response", None), "status_code", None)
                    }
                )
                raise

            sleep_s = min(2 ** attempt, 30)
            time.sleep(sleep_s)

    raise RuntimeError("Unreachable")


# =============================================================================
# Shopify standard mutation (inventory batch)
# =============================================================================

MUTATION_INVENTORY_SET_QUANTITIES = """
mutation InventorySet($input: InventorySetQuantitiesInput!) {
  inventorySetQuantities(input: $input) {
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
    variables = {
        "input": {
            "name": "available",
            "reason": "correction",
            "referenceDocumentUri": reference_document_uri,
            "quantities": [
                {
                    "inventoryItemId": inv_id,
                    "locationId": SHOPIFY_LOCATION_ID,
                    "quantity": int(qty),
                    "changeFromQuantity": None
                }
                for inv_id, qty in inventory_quantities
            ],
        }
    }

    res = shopify_graphql(
        endpoint,
        SHOPIFY_ACCESS_TOKEN,
        MUTATION_INVENTORY_SET_QUANTITIES,
        variables
    )

    errs = ((res.get("data") or {}).get("inventorySetQuantities") or {}).get("userErrors") or []
    if errs:
        log_error("inventorySetQuantities userErrors (batch)", errs)

    return res


def inventory_set_available_zero_batch(endpoint: str, inventory_item_ids: List[str]) -> Dict[str, Any]:
    return inventory_set_available_batch(
        endpoint,
        [(inv_id, 0) for inv_id in inventory_item_ids],
        reference_document_uri="gid://erp-connector/SyncJob/missing-record",
    )


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
    while True:
        res = shopify_graphql(endpoint, token, QUERY_BULK_OPERATION, {"id": op_id})
        op = (res.get("data") or {}).get("bulkOperation") or {}
        status = (op.get("status") or "").upper()

        if status in ("COMPLETED", "FAILED", "CANCELED", "CANCELLED"):
            return op

        time.sleep(poll_seconds)


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

    while True:
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
    rounded = value.quantize(Decimal("0.01"), rounding=ROUND_HALF_UP)
    return f"{rounded:.2f}"


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
    # Usiamo Decimal per non perdere precisione e arrotondiamo solo alla fine a 2 decimali.
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

    if not chosen and variants:
        chosen = variants[0]

    variant_id = (chosen or {}).get("id")
    inventory_item_id = ((chosen or {}).get("inventoryItem") or {}).get("id")
    return product_id, variant_id, inventory_item_id


# =============================================================================
# SQLite state (batch upsert)
# =============================================================================

class SyncState:
    def __init__(self, path: str):
        self.path = path

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
            log_error("Record scartato: CODICE_EAN contiene spazi", {
                "raw_ean": raw_ean,
                "titolo": get_ci(row, "TITOLO", default=""),
            })
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
# Main sync
# =============================================================================

def main() -> None:
    logger.info("=== AVVIO SYNC SHOPIFY ===")
    logger.info(f"APP_DIR:    {SCRIPT_DIR}")
    logger.info(f"Config env: {file_path_str(ENV_PATH)}")
    logger.info(f"Log file:   {LOG_PATH}")
    logger.info(f"State DB:   {STATE_DB_PATH}")
    logger.info(f"WordPress cover enabled: {WORDPRESS_COVER_ENABLED}")
    logger.info(f"WordPress media URL: {WORDPRESS_MEDIA_SEARCH_URL}")

    records = read_testi_records()
    logger.info(f"Record letti con EAN: {len(records)}")

    state = SyncState(STATE_DB_PATH)
    run_id = state.start_run()
    prev = state.load_all()

    # -------------------------------------------------------------------------
    # WordPress viene letto una sola volta e solo se ci sono prodotti nuovi.
    # Questo evita una chiamata HTTP per ogni prodotto e mantiene il sync veloce
    # anche con migliaia di record.
    # -------------------------------------------------------------------------
    new_eans_preview: set[str] = set()
    for row in records:
        ean_preview = clean_str(get_ci(row, "CODICE_EAN", default=""))
        if ean_preview and ean_preview not in prev:
            new_eans_preview.add(ean_preview)

    wordpress_cover_by_ean: Dict[str, str] = {}

    if WORDPRESS_COVER_ENABLED and new_eans_preview:
        wordpress_cover_by_ean = load_wordpress_cover_urls_by_ean()

        wp_hits_for_new = sum(
            1 for ean in new_eans_preview
            if ean in wordpress_cover_by_ean
        )

        logger.info(
            f"Nuovi prodotti rilevati: {len(new_eans_preview)} | "
            f"immagini WordPress disponibili per nuovi prodotti: {wp_hits_for_new} | "
            f"fallback template: {len(new_eans_preview) - wp_hits_for_new}"
        )
    elif not new_eans_preview:
        logger.info("Nessun nuovo prodotto: non indicizzo immagini WordPress")
    else:
        logger.info("WordPress cover disabilitate: uso COVER_URL_TEMPLATE per eventuali nuovi prodotti")

    if DRY_RUN:
        endpoint = ""
        logger.info("DRY_RUN=1 (nessuna chiamata a Shopify)")
    else:
        if not SHOPIFY_SHOP or not SHOPIFY_ACCESS_TOKEN:
            raise RuntimeError(
                "Config mancante: SHOPIFY_SHOP e SHOPIFY_ACCESS_TOKEN (oppure DRY_RUN=1)."
            )
        if not SHOPIFY_LOCATION_ID:
            raise RuntimeError("Config mancante: SHOPIFY_LOCATION_ID.")

        endpoint = f"https://{SHOPIFY_SHOP}/admin/api/{SHOPIFY_API_VERSION}/graphql.json"

    seen_now: set[str] = set()
    created = 0
    updated = 0
    unchanged = 0
    zeroed = 0

    to_create: List[Dict[str, Any]] = []
    to_inventory_update: List[Dict[str, Any]] = []
    unchanged_rows: List[Tuple[int, str, str, Optional[str], Optional[str], Optional[str]]] = []

    for row in records:
        ean = clean_str(get_ci(row, "CODICE_EAN", default=""))
        if not ean:
            continue

        seen_now.add(ean)

        h = compute_row_hash(row)
        prev_item = prev.get(ean)
        is_scolastico_row = is_scolastico(row)

        is_new = prev_item is None
        is_changed = (not is_new) and (prev_item.get("row_hash") != h)

        if (not is_new) and (is_scolastico_row or not is_changed):
            unchanged += 1
            unchanged_rows.append((
                run_id,
                ean,
                h,
                prev_item.get("product_id"),
                prev_item.get("variant_id"),
                prev_item.get("inventory_item_id")
            ))
        elif is_new:
            to_create.append({
                "ean": ean,
                "row_hash": h,
                # I prodotti nuovi vengono creati con scheda completa.
                # Per i nuovi prodotti: prima immagine WordPress, poi fallback template.
                "input": build_productset_input_from_testi_row(
                    row,
                    include_files=True,
                    wordpress_cover_by_ean=wordpress_cover_by_ean,
                ),
                "is_new": True,
            })
        else:
            # Prodotto già creato: da questo punto in poi NON aggiorniamo più
            # titolo, descrizione, prezzo, autore, categoria, note, metafield
            # o immagini. Se cambia qualcosa nel DBF, lo state considera solo
            # la giacenza e su Shopify inviamo solo inventorySetQuantities.
            to_inventory_update.append({
                "ean": ean,
                "row_hash": h,
                "quantity": get_row_stock_quantity(row),
                "product_id": prev_item.get("product_id"),
                "variant_id": prev_item.get("variant_id"),
                "inventory_item_id": prev_item.get("inventory_item_id"),
            })

    try:
        for chunk in chunked(unchanged_rows, SQLITE_UPSERT_BATCH):
            state.upsert_items_many(chunk)

        if DRY_RUN:
            sim_create = [(run_id, it["ean"], it["row_hash"], None, None, None) for it in to_create]
            sim_inventory = [
                (run_id, it["ean"], it["row_hash"], it.get("product_id"), it.get("variant_id"), it.get("inventory_item_id"))
                for it in to_inventory_update
            ]
            for chunk in chunked(sim_create + sim_inventory, SQLITE_UPSERT_BATCH):
                state.upsert_items_many(chunk)
        else:
            if not BULK_ENABLED:
                raise RuntimeError("BULK_ENABLED=0 ma bulk richiesto: imposta BULK_ENABLED=1")

            # -----------------------------------------------------------------
            # 1) CREAZIONE prodotti nuovi: productSet completo.
            #    Nessun prodotto già esistente passa più da productSet.
            # -----------------------------------------------------------------
            publish_queue: List[str] = []
            logger.info(f"Bulk productSet nuovi prodotti: {len(to_create)} (chunk={BULK_CHUNK_SIZE})")
            total_chunks = (len(to_create) + BULK_CHUNK_SIZE - 1) // BULK_CHUNK_SIZE if to_create else 0

            for chunk_idx, chunk_items in enumerate(chunked(to_create, BULK_CHUNK_SIZE), start=1):
                t_chunk0 = time.perf_counter()
                chunk_log("productSet", chunk_idx, total_chunks, "inizio elaborazione")

                eans_in_order: List[str] = []
                rowhash_by_ean: Dict[str, str] = {}
                isnew_by_ean: Dict[str, bool] = {}
                lines: List[str] = []

                for it in chunk_items:
                    ean = it["ean"]
                    eans_in_order.append(ean)
                    rowhash_by_ean[ean] = it["row_hash"]
                    isnew_by_ean[ean] = bool(it["is_new"])

                    input_obj = dict(it["input"])

                    lines.append(json.dumps({
                        "identifier": {
                            "customId": {
                                "namespace": EXTERNAL_ID_NAMESPACE,
                                "key": EXTERNAL_ID_KEY,
                                "value": ean
                            }
                        },
                        "input": input_obj
                    }, ensure_ascii=False, separators=(",", ":")))

                jsonl_bytes = ("\n".join(lines) + "\n").encode("utf-8")

                staged_path = staged_upload_jsonl(
                    endpoint,
                    SHOPIFY_ACCESS_TOKEN,
                    filename=f"productset_vars_run{run_id}_chunk{chunk_idx}.jsonl",
                    jsonl_bytes=jsonl_bytes
                )

                op_id = bulk_run_mutation(
                    endpoint,
                    SHOPIFY_ACCESS_TOKEN,
                    mutation_str=BULK_MUTATION_PRODUCT_SET,
                    staged_upload_path=staged_path,
                    client_identifier=f"productset_run{run_id}_chunk{chunk_idx}"
                )

                op = poll_bulk_operation(endpoint, SHOPIFY_ACCESS_TOKEN, op_id)
                status = (op.get("status") or "").upper()
                if status != "COMPLETED":
                    log_error("Bulk productSet non COMPLETED", op)
                    raise RuntimeError("Bulk productSet fallita")

                out_url = op.get("url")
                if not out_url:
                    log_error("Bulk productSet COMPLETED ma url output mancante", op)
                    raise RuntimeError("Output url mancante")

                upsert_rows: List[Tuple[int, str, str, Optional[str], Optional[str], Optional[str]]] = []

                for i, out_line in enumerate(iter_jsonl_from_url(out_url)):
                    if i >= len(eans_in_order):
                        break

                    ean = eans_in_order[i]
                    payload = (out_line.get("data") or {}).get("productSet") or {}
                    uerrs = payload.get("userErrors") or []

                    if uerrs:
                        log_error(f"productSet userErrors (EAN={ean})", uerrs)
                        continue

                    product = payload.get("product") or {}
                    if not product:
                        log_error(f"productSet senza product (EAN={ean})", out_line)
                        continue

                    product_id = product.get("id")
                    if not product_id:
                        log_error(f"productSet product_id NULL (EAN={ean})", out_line)
                        continue

                    product_id, variant_id, inventory_item_id = pick_ids_from_product_node(product, ean)

                    upsert_rows.append((
                        run_id,
                        ean,
                        rowhash_by_ean[ean],
                        product_id,
                        variant_id,
                        inventory_item_id
                    ))

                    created += 1
                    if PUBLICATION_IDS:
                        publish_queue.append(product_id)

                for sub in chunked(upsert_rows, SQLITE_UPSERT_BATCH):
                    state.upsert_items_many(sub)

                chunk_log(
                    "productSet",
                    chunk_idx,
                    total_chunks,
                    f"COMPLETED in {time.perf_counter() - t_chunk0:.2f}s"
                )

            # -----------------------------------------------------------------
            # 2) AGGIORNAMENTO prodotti esistenti: solo giacenza.
            #    Qui non vengono inviati title, descriptionHtml, vendor, price,
            #    metafield, tags o files.
            # -----------------------------------------------------------------
            logger.info(f"Aggiornamento sole giacenze prodotti esistenti: {len(to_inventory_update)}")
            inv_updates = [
                it for it in to_inventory_update
                if it.get("inventory_item_id")
            ]
            missing_inv = [it for it in to_inventory_update if not it.get("inventory_item_id")]
            for it in missing_inv:
                log_error(
                    f"Impossibile aggiornare solo giacenza: inventory_item_id mancante (EAN={it['ean']})",
                    {"ean": it["ean"], "product_id": it.get("product_id"), "variant_id": it.get("variant_id")}
                )

            inv_total = (len(inv_updates) + INVENTORY_ZERO_BATCH - 1) // INVENTORY_ZERO_BATCH if inv_updates else 0
            for chunk_idx, batch in enumerate(chunked(inv_updates, INVENTORY_ZERO_BATCH), start=1):
                t_chunk0 = time.perf_counter()
                chunk_log("inventory", chunk_idx, inv_total, "inizio aggiornamento giacenze")

                res = inventory_set_available_batch(
                    endpoint,
                    [(it["inventory_item_id"], it["quantity"]) for it in batch],
                    reference_document_uri=f"gid://erp-connector/SyncJob/run-{run_id}-inventory",
                )
                errs = ((res.get("data") or {}).get("inventorySetQuantities") or {}).get("userErrors") or []
                if errs:
                    continue

                updated += len(batch)
                upsert_rows = [
                    (run_id, it["ean"], it["row_hash"], it.get("product_id"), it.get("variant_id"), it.get("inventory_item_id"))
                    for it in batch
                ]
                for sub in chunked(upsert_rows, SQLITE_UPSERT_BATCH):
                    state.upsert_items_many(sub)

                chunk_log(
                    "inventory",
                    chunk_idx,
                    inv_total,
                    f"COMPLETED in {time.perf_counter() - t_chunk0:.2f}s"
                )

            if PUBLICATION_IDS and publish_queue:
                publish_total = (len(publish_queue) + BULK_CHUNK_SIZE - 1) // BULK_CHUNK_SIZE
                logger.info(f"Bulk publish: {len(publish_queue)} (chunk={BULK_CHUNK_SIZE})")

                for chunk_idx, prod_ids in enumerate(chunked(publish_queue, BULK_CHUNK_SIZE), start=1):
                    t_chunk0 = time.perf_counter()
                    chunk_log("publish", chunk_idx, publish_total, "inizio elaborazione")

                    lines = []
                    for pid in prod_ids:
                        lines.append(json.dumps({
                            "id": pid,
                            "input": [{"publicationId": pub_id} for pub_id in PUBLICATION_IDS]
                        }, ensure_ascii=False, separators=(",", ":")))

                    jsonl_bytes = ("\n".join(lines) + "\n").encode("utf-8")

                    staged_path = staged_upload_jsonl(
                        endpoint,
                        SHOPIFY_ACCESS_TOKEN,
                        filename=f"publish_vars_run{run_id}_chunk{chunk_idx}.jsonl",
                        jsonl_bytes=jsonl_bytes
                    )

                    op_id = bulk_run_mutation(
                        endpoint,
                        SHOPIFY_ACCESS_TOKEN,
                        mutation_str=BULK_MUTATION_PUBLISH,
                        staged_upload_path=staged_path,
                        client_identifier=f"publish_run{run_id}_chunk{chunk_idx}"
                    )

                    op = poll_bulk_operation(endpoint, SHOPIFY_ACCESS_TOKEN, op_id)
                    status = (op.get("status") or "").upper()
                    if status != "COMPLETED":
                        log_error("Bulk publish non COMPLETED", op)
                        raise RuntimeError("Bulk publish fallita")

                    out_url = op.get("url")
                    if out_url:
                        for out_line in iter_jsonl_from_url(out_url):
                            uerrs = ((out_line.get("data") or {}).get("publishablePublish") or {}).get("userErrors") or []
                            if uerrs:
                                log_error("publishablePublish userErrors", uerrs)

                    chunk_log(
                        "publish",
                        chunk_idx,
                        publish_total,
                        f"COMPLETED in {time.perf_counter() - t_chunk0:.2f}s"
                    )

        missing = [ean for ean in prev.keys() if ean not in seen_now]
        if missing and (not DRY_RUN):
            inv_pairs: List[Tuple[str, str]] = []
            for ean in missing:
                inv_id = (prev.get(ean) or {}).get("inventory_item_id")
                if inv_id:
                    inv_pairs.append((ean, inv_id))

            for batch in chunked(inv_pairs, INVENTORY_ZERO_BATCH):
                batch_inv = [inv for (_, inv) in batch]
                res = inventory_set_available_zero_batch(endpoint, batch_inv)
                errs = ((res.get("data") or {}).get("inventorySetQuantities") or {}).get("userErrors") or []
                if not errs:
                    zeroed += len(batch)

    except Exception as e:
        log_error("ERRORE FATALE durante sync", {"error": str(e)})
        raise
    finally:
        state.finish_run(run_id)

    logger.info("=== SYNC COMPLETATA ===")
    logger.info(f"Creati:     {created}")
    logger.info(f"Aggiornati: {updated}")
    logger.info(f"Invariati:  {unchanged}")
    logger.info(f"Spariti->0: {zeroed}")
    logger.info(f"State DB:   {STATE_DB_PATH}")
    logger.info(f"Log file:   {LOG_PATH}")


if __name__ == "__main__":
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

        if is_running_as_exe():
            try:
                input("\nPremi Invio per chiudere...")
            except EOFError:
                pass
        raise
