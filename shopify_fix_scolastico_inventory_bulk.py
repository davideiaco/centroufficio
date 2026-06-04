from dotenv import load_dotenv
from pathlib import Path
import sys
import os
import re
import json
import struct
import datetime
import time
import logging
from logging.handlers import RotatingFileHandler
from typing import Any, Dict, Iterable, List, Optional, Set, Tuple

import requests

# =============================================================================
# Script BULK una tantum per correggere i prodotti gia' presenti su Shopify
# che nel gestionale risultano di tipologia "SCOLASTICO".
#
# Flusso:
# 1) Legge testi.dbf + tipologie.dbf e costruisce l'elenco SKU/EAN SCOLASTICO.
# 2) Esporta in bulk da Shopify tutte le varianti con SKU, inventoryPolicy,
#    inventoryItem.tracked e productId.
# 3) Incrocia gli SKU SCOLASTICO con le varianti Shopify esportate.
# 4) Crea un JSONL per bulk mutation productVariantsBulkUpdate:
#       inventoryPolicy = CONTINUE
#       inventoryItem.tracked = false
# 5) Carica il JSONL con staged upload e avvia bulkOperationRunMutation.
#
# Usa lo stesso config.env dello script principale.
# Consiglio: prima eseguire con DRY_RUN=1.
# =============================================================================


def get_app_dir() -> Path:
    if getattr(sys, "frozen", False):
        return Path(sys.executable).resolve().parent
    return Path(__file__).resolve().parent


APP_DIR = get_app_dir()
ENV_PATH = APP_DIR / "config.env"
load_dotenv(dotenv_path=ENV_PATH)


def _env(name: str, default: str = "") -> str:
    return os.getenv(name, default).strip()


def _env_bool(name: str, default: str = "0") -> bool:
    return _env(name, default).lower() in {"1", "true", "yes", "y", "si", "s"}


def _env_int(name: str, default: str = "0") -> int:
    try:
        return int(_env(name, default))
    except ValueError:
        return int(default)


DATA_DIR = _env("DATA_DIR", r"C:\WinVaria\data")
DBF_FILENAME = _env("DBF_FILENAME", "testi.dbf")
TIPOLOGIE_DBF_FILENAME = _env("TIPOLOGIE_DBF_FILENAME", "tipologie.dbf")
DBF_PATH = os.path.join(DATA_DIR, DBF_FILENAME)
TIPOLOGIE_DBF_PATH = os.path.join(DATA_DIR, TIPOLOGIE_DBF_FILENAME)

SHOPIFY_SHOP = _env("SHOPIFY_SHOP", "")
SHOPIFY_ACCESS_TOKEN = _env("SHOPIFY_ACCESS_TOKEN", "")
SHOPIFY_API_VERSION = _env("SHOPIFY_API_VERSION", "2026-01")

# DRY_RUN=1: non modifica Shopify. Esporta, incrocia e genera il JSONL locale.
DRY_RUN = _env_bool("DRY_RUN", "1")
LIMIT_RECORDS = _env_int("LIMIT_RECORDS", "0")
POLL_SECONDS = max(2, _env_int("SHOPIFY_BULK_POLL_SECONDS", "10"))
POLL_TIMEOUT_SECONDS = max(60, _env_int("SHOPIFY_BULK_TIMEOUT_SECONDS", "21600"))  # 6 ore
MAX_VARIANTS_PER_PRODUCT_LINE = max(1, _env_int("SCOLASTICO_BULK_VARIANTS_PER_PRODUCT_LINE", "100"))

TOKEN_ENV_PATH = ENV_PATH
SHOPIFY_REFRESH_CLIENT_ID = _env("SHOPIFY_REFRESH_CLIENT_ID", "")
SHOPIFY_REFRESH_CLIENT_SECRET = _env("SHOPIFY_REFRESH_CLIENT_SECRET", "")

RUN_TS = datetime.datetime.now().strftime("%Y%m%d_%H%M%S")
LOG_PATH = str(APP_DIR / "shopify_fix_scolastico_inventory_bulk.log")
EXPORT_JSONL_PATH = APP_DIR / f"shopify_variants_export_{RUN_TS}.jsonl"
MUTATION_JSONL_PATH = APP_DIR / f"shopify_scolastico_fix_mutation_{RUN_TS}.jsonl"
RESULT_JSONL_PATH = APP_DIR / f"shopify_scolastico_fix_result_{RUN_TS}.jsonl"
NOT_FOUND_PATH = APP_DIR / f"shopify_scolastico_not_found_{RUN_TS}.txt"

logger = logging.getLogger("shopify_fix_scolastico_inventory_bulk")
logger.setLevel(logging.INFO)
logger.propagate = False
_fmt = logging.Formatter("%(asctime)s | %(levelname)s | %(message)s")
if not logger.handlers:
    _file = RotatingFileHandler(LOG_PATH, maxBytes=10_000_000, backupCount=5, encoding="utf-8")
    _file.setFormatter(_fmt)
    logger.addHandler(_file)
    _console = logging.StreamHandler()
    _console.setFormatter(_fmt)
    logger.addHandler(_console)


def log_error(msg: str, details: Any = None) -> None:
    if details is None:
        logger.error(msg)
        return
    try:
        s = json.dumps(details, ensure_ascii=False, indent=2, default=str)
    except Exception:
        s = str(details)
    logger.error(f"{msg}\n{s[:4000]}")


# =============================================================================
# DBF minimale
# =============================================================================

FieldDef = Tuple[str, str, int, int]


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
    off = 1
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
    if not rec or rec[0] == 0x2A:
        return None

    out: Dict[str, Any] = {}
    for name, ftype, length, dec, off in field_offsets:
        raw = rec[off:off + length]
        if ftype == "C":
            out[name] = raw.decode("cp1252", errors="ignore").rstrip()
        elif ftype == "N":
            s = raw.decode("ascii", errors="ignore").strip()
            out[name] = float(s) if s else None
        elif ftype == "I":
            out[name] = struct.unpack("<i", raw)[0]
        elif ftype == "D":
            s = raw.decode("ascii", errors="ignore").strip()
            out[name] = datetime.date(int(s[0:4]), int(s[4:6]), int(s[6:8])) if s else None
        elif ftype == "L":
            c = raw[:1].decode("ascii", errors="ignore").upper()
            out[name] = c in ("Y", "T")
        elif ftype == "M":
            out[name] = struct.unpack("<I", raw)[0]
        else:
            out[name] = raw
    return out


def get_ci(row: Dict[str, Any], *keys: str, default=None):
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
        desc = clean_str(get_ci(r, "DESCRIZIONE", "Descrizione", "DESC", "Description", "DESCRIZION", default=""))
        if raw_id is None or not desc:
            continue
        try:
            m[int(raw_id)] = desc
        except (TypeError, ValueError):
            continue
    return m


def read_scolastico_eans() -> Set[str]:
    if not os.path.exists(DBF_PATH):
        raise FileNotFoundError(f"DBF testi non trovato: {DBF_PATH}")
    tipologie_map = load_tipologie_map(TIPOLOGIE_DBF_PATH)

    dbf = _read_all_bytes(DBF_PATH)
    fields, header_len, record_len, num_records = parse_dbf_fields(dbf)
    field_offsets = build_field_offsets(fields)

    eans: Set[str] = set()
    for i in range(num_records):
        row = parse_record(dbf, field_offsets, header_len, record_len, i)
        if not row:
            continue

        raw_ean = clean_str(get_ci(row, "CODICE_EAN", default=""))
        if not raw_ean or " " in raw_ean:
            continue

        id_tipo_raw = get_ci(row, "Id_tipo", "ID_TIPO", "IDTIPO", default=None)
        categoria = ""
        try:
            if id_tipo_raw is not None:
                categoria = tipologie_map.get(int(id_tipo_raw), "") or ""
        except (TypeError, ValueError):
            categoria = ""

        if categoria.strip().upper() == "SCOLASTICO":
            eans.add(raw_ean)
            if LIMIT_RECORDS and len(eans) >= LIMIT_RECORDS:
                break

    return eans


# =============================================================================
# Shopify GraphQL + token refresh
# =============================================================================


def _shop_name_only(shop_value: str) -> str:
    shop_value = (shop_value or "").strip()
    if not shop_value:
        return ""
    return shop_value.replace("https://", "").replace("http://", "").replace(".myshopify.com", "").strip("/")


def normalize_shop_domain(shop_value: str) -> str:
    shop_name = _shop_name_only(shop_value)
    if not shop_name:
        return ""
    if "." in shop_name:
        return shop_name
    return f"{shop_name}.myshopify.com"


def update_config_env_access_token(new_token: str, file_path: Path = TOKEN_ENV_PATH) -> None:
    if not new_token:
        raise ValueError("Nuovo access token vuoto")
    content = file_path.read_text(encoding="utf-8") if file_path.exists() else ""
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
        raise RuntimeError("Token Shopify non valido e credenziali refresh mancanti nel config.env")

    shop_name = _shop_name_only(SHOPIFY_SHOP)
    if not shop_name:
        raise RuntimeError("SHOPIFY_SHOP mancante o non valido")

    refresh_url = f"https://{shop_name}.myshopify.com/admin/oauth/access_token"
    payload = {
        "grant_type": "client_credentials",
        "client_id": SHOPIFY_REFRESH_CLIENT_ID,
        "client_secret": SHOPIFY_REFRESH_CLIENT_SECRET,
    }
    resp = requests.post(refresh_url, headers={"Content-Type": "application/x-www-form-urlencoded"}, data=payload, timeout=60)
    resp.raise_for_status()
    data = resp.json()
    new_token = (data.get("access_token") or "").strip()
    if not new_token:
        raise RuntimeError("Risposta refresh token senza access_token")
    update_config_env_access_token(new_token)
    SHOPIFY_ACCESS_TOKEN = new_token
    os.environ["SHOPIFY_ACCESS_TOKEN"] = new_token
    logger.info("Nuovo access token Shopify generato e salvato in config.env")
    return new_token


def _graphql_has_auth_error(data: Dict[str, Any]) -> bool:
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


def shopify_graphql(endpoint: str, token: str, query: str, variables: Optional[Dict[str, Any]] = None, *, max_retries: int = 6) -> Dict[str, Any]:
    global SHOPIFY_ACCESS_TOKEN
    current_token = token
    auth_refresh_done = False

    for attempt in range(max_retries):
        headers = {"Content-Type": "application/json", "X-Shopify-Access-Token": current_token}
        try:
            resp = requests.post(endpoint, headers=headers, json={"query": query, "variables": variables or {}}, timeout=180)
            if resp.status_code in (401, 403):
                if auth_refresh_done:
                    resp.raise_for_status()
                current_token = refresh_shopify_access_token()
                SHOPIFY_ACCESS_TOKEN = current_token
                auth_refresh_done = True
                continue
            if resp.status_code in (429, 500, 502, 503, 504):
                raise requests.HTTPError(f"HTTP {resp.status_code}: {resp.text[:500]}", response=resp)
            resp.raise_for_status()
            data = resp.json()
            if _graphql_has_auth_error(data):
                if auth_refresh_done:
                    raise RuntimeError(f"Autenticazione Shopify fallita: {data}")
                current_token = refresh_shopify_access_token()
                SHOPIFY_ACCESS_TOKEN = current_token
                auth_refresh_done = True
                continue
            if data.get("errors"):
                log_error("GraphQL top-level errors", data.get("errors"))
            return data
        except Exception as e:
            if attempt >= max_retries - 1:
                log_error("Errore chiamata Shopify GraphQL", {"error": str(e)})
                raise
            time.sleep(min(2 ** attempt, 30))
    raise RuntimeError("Unreachable")


BULK_VARIANTS_QUERY = r'''
mutation RunBulkVariantExport {
  bulkOperationRunQuery(
    query: """
    {
      productVariants {
        edges {
          node {
            id
            sku
            inventoryPolicy
            product { id title }
            inventoryItem { id tracked }
          }
        }
      }
    }
    """
  ) {
    bulkOperation { id status }
    userErrors { field message }
  }
}
'''.strip()


QUERY_BULK_OPERATION = r'''
query BulkOperationStatus($id: ID!) {
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
'''.strip()


STAGED_UPLOAD_CREATE = r'''
mutation StagedUploadsCreate($input: [StagedUploadInput!]!) {
  stagedUploadsCreate(input: $input) {
    stagedTargets {
      url
      resourceUrl
      parameters { name value }
    }
    userErrors { field message }
  }
}
'''.strip()


BULK_MUTATION_RUN = r'''
mutation RunBulkMutation($mutation: String!, $stagedUploadPath: String!) {
  bulkOperationRunMutation(mutation: $mutation, stagedUploadPath: $stagedUploadPath) {
    bulkOperation { id status }
    userErrors { field message }
  }
}
'''.strip()


PRODUCT_VARIANTS_BULK_UPDATE_MUTATION = r'''
mutation FixScolasticoInventory($productId: ID!, $variants: [ProductVariantsBulkInput!]!) {
  productVariantsBulkUpdate(productId: $productId, variants: $variants, allowPartialUpdates: true) {
    product { id title }
    productVariants {
      id
      sku
      inventoryPolicy
      inventoryItem { id tracked }
    }
    userErrors { field message code }
  }
}
'''.strip()


def ensure_no_top_errors(res: Dict[str, Any], context: str) -> None:
    if res.get("errors"):
        raise RuntimeError(f"{context}: GraphQL errors: {json.dumps(res.get('errors'), ensure_ascii=False)}")


def start_bulk_variant_export(endpoint: str) -> str:
    res = shopify_graphql(endpoint, SHOPIFY_ACCESS_TOKEN, BULK_VARIANTS_QUERY)
    ensure_no_top_errors(res, "start_bulk_variant_export")
    payload = ((res.get("data") or {}).get("bulkOperationRunQuery") or {})
    errs = payload.get("userErrors") or []
    if errs:
        raise RuntimeError(f"Errore avvio bulk export: {errs}")
    op = payload.get("bulkOperation") or {}
    op_id = op.get("id")
    if not op_id:
        raise RuntimeError(f"Bulk export senza id: {res}")
    logger.info(f"Bulk export varianti avviato: {op_id}")
    return op_id


def poll_bulk_operation(endpoint: str, op_id: str, label: str) -> Dict[str, Any]:
    started = time.time()
    last_count = None
    while True:
        res = shopify_graphql(endpoint, SHOPIFY_ACCESS_TOKEN, QUERY_BULK_OPERATION, {"id": op_id})
        ensure_no_top_errors(res, f"poll {label}")
        op = ((res.get("data") or {}).get("bulkOperation") or {})
        if not op:
            raise RuntimeError(f"Operazione bulk non trovata: {op_id}")
        status = op.get("status")
        obj_count = op.get("objectCount")
        if obj_count != last_count:
            logger.info(f"{label}: status={status} objectCount={obj_count}")
            last_count = obj_count
        else:
            logger.info(f"{label}: status={status}")

        if status == "COMPLETED":
            return op
        if status in {"FAILED", "CANCELED", "EXPIRED"}:
            raise RuntimeError(f"{label} terminata con stato {status}: {op}")
        if time.time() - started > POLL_TIMEOUT_SECONDS:
            raise TimeoutError(f"Timeout polling {label} dopo {POLL_TIMEOUT_SECONDS} secondi")
        time.sleep(POLL_SECONDS)


def download_file(url: str, path: Path) -> None:
    if not url:
        raise RuntimeError("URL download vuoto")
    with requests.get(url, stream=True, timeout=300) as r:
        r.raise_for_status()
        with open(path, "wb") as f:
            for chunk in r.iter_content(chunk_size=1024 * 1024):
                if chunk:
                    f.write(chunk)
    logger.info(f"File scaricato: {path}")


def iter_jsonl(path: Path) -> Iterable[Dict[str, Any]]:
    with open(path, "r", encoding="utf-8") as f:
        for line_no, line in enumerate(f, start=1):
            line = line.strip()
            if not line:
                continue
            try:
                yield json.loads(line)
            except Exception as e:
                log_error(f"JSONL non valido alla riga {line_no}", {"error": str(e), "line": line[:500]})


def product_id_from_variant_obj(obj: Dict[str, Any]) -> str:
    product = obj.get("product") or {}
    if isinstance(product, dict):
        return clean_str(product.get("id"))
    return clean_str(obj.get("__parentId"))


def build_mutation_jsonl(export_path: Path, scolastico_skus: Set[str], out_path: Path) -> Tuple[int, int, int, int]:
    """Returns: matched, to_update_variants, already_ok, jsonl_lines"""
    matched_skus: Set[str] = set()
    by_product: Dict[str, List[Dict[str, Any]]] = {}
    already_ok = 0

    for obj in iter_jsonl(export_path):
        sku = clean_str(obj.get("sku"))
        if not sku or sku not in scolastico_skus:
            continue

        matched_skus.add(sku)
        variant_id = clean_str(obj.get("id"))
        product_id = product_id_from_variant_obj(obj)
        inventory_policy = clean_str(obj.get("inventoryPolicy"))
        inv_item = obj.get("inventoryItem") or {}
        tracked = bool(inv_item.get("tracked")) if isinstance(inv_item, dict) else False

        if not variant_id or not product_id:
            log_error("Variante SCOLASTICO senza id o productId", obj)
            continue

        if tracked is False and inventory_policy == "CONTINUE":
            already_ok += 1
            continue

        by_product.setdefault(product_id, []).append({
            "id": variant_id,
            "inventoryPolicy": "CONTINUE",
            "inventoryItem": {"tracked": False},
        })

    not_found = sorted(scolastico_skus - matched_skus)
    with open(NOT_FOUND_PATH, "w", encoding="utf-8") as f:
        for sku in not_found:
            f.write(sku + "\n")

    lines = 0
    to_update_variants = 0
    with open(out_path, "w", encoding="utf-8") as f:
        for product_id, variants in by_product.items():
            for i in range(0, len(variants), MAX_VARIANTS_PER_PRODUCT_LINE):
                chunk = variants[i:i + MAX_VARIANTS_PER_PRODUCT_LINE]
                f.write(json.dumps({"productId": product_id, "variants": chunk}, ensure_ascii=False) + "\n")
                lines += 1
                to_update_variants += len(chunk)

    return len(matched_skus), to_update_variants, already_ok, lines


def create_staged_upload(endpoint: str, jsonl_path: Path) -> Tuple[str, str, List[Dict[str, str]]]:
    variables = {
        "input": [{
            "resource": "BULK_MUTATION_VARIABLES",
            "filename": jsonl_path.name,
            "mimeType": "text/jsonl",
            "httpMethod": "POST",
        }]
    }
    res = shopify_graphql(endpoint, SHOPIFY_ACCESS_TOKEN, STAGED_UPLOAD_CREATE, variables)
    ensure_no_top_errors(res, "stagedUploadsCreate")
    payload = ((res.get("data") or {}).get("stagedUploadsCreate") or {})
    errs = payload.get("userErrors") or []
    if errs:
        raise RuntimeError(f"Errore stagedUploadsCreate: {errs}")
    target = (payload.get("stagedTargets") or [None])[0]
    if not target:
        raise RuntimeError(f"Nessun staged target: {res}")
    url = target.get("url")
    resource_url = target.get("resourceUrl")
    params = target.get("parameters") or []
    staged_upload_path = ""
    for p in params:
        if p.get("name") == "key":
            staged_upload_path = p.get("value") or ""
            break
    if not url or not staged_upload_path:
        raise RuntimeError(f"Staged upload senza url/key: {target}")
    return url, staged_upload_path, params


def upload_jsonl_to_staged_target(url: str, params: List[Dict[str, str]], jsonl_path: Path) -> None:
    data = {p["name"]: p["value"] for p in params if "name" in p and "value" in p}
    with open(jsonl_path, "rb") as f:
        files = {"file": (jsonl_path.name, f, "text/jsonl")}
        resp = requests.post(url, data=data, files=files, timeout=300)
    if resp.status_code not in (200, 201, 204):
        raise RuntimeError(f"Errore upload staged JSONL HTTP {resp.status_code}: {resp.text[:1000]}")
    logger.info(f"JSONL caricato su staged target: {jsonl_path.name}")


def start_bulk_mutation(endpoint: str, staged_upload_path: str) -> str:
    variables = {
        "mutation": PRODUCT_VARIANTS_BULK_UPDATE_MUTATION,
        "stagedUploadPath": staged_upload_path,
    }
    res = shopify_graphql(endpoint, SHOPIFY_ACCESS_TOKEN, BULK_MUTATION_RUN, variables)
    ensure_no_top_errors(res, "bulkOperationRunMutation")
    payload = ((res.get("data") or {}).get("bulkOperationRunMutation") or {})
    errs = payload.get("userErrors") or []
    if errs:
        raise RuntimeError(f"Errore avvio bulk mutation: {errs}")
    op = payload.get("bulkOperation") or {}
    op_id = op.get("id")
    if not op_id:
        raise RuntimeError(f"Bulk mutation senza id: {res}")
    logger.info(f"Bulk mutation avviata: {op_id}")
    return op_id


def summarize_result_jsonl(path: Path) -> Tuple[int, int]:
    rows = 0
    rows_with_errors = 0
    if not path.exists():
        return 0, 0
    for obj in iter_jsonl(path):
        rows += 1
        payload = obj.get("productVariantsBulkUpdate") or obj.get("data", {}).get("productVariantsBulkUpdate") or {}
        errors = payload.get("userErrors") if isinstance(payload, dict) else None
        if errors:
            rows_with_errors += 1
            log_error("Errore in risultato bulk mutation", errors)
    return rows, rows_with_errors


def main() -> int:
    if not SHOPIFY_SHOP or not SHOPIFY_ACCESS_TOKEN:
        raise RuntimeError("SHOPIFY_SHOP e SHOPIFY_ACCESS_TOKEN devono essere configurati in config.env")

    shop_domain = normalize_shop_domain(SHOPIFY_SHOP)
    endpoint = f"https://{shop_domain}/admin/api/{SHOPIFY_API_VERSION}/graphql.json"

    logger.info("=== AVVIO FIX BULK PRODOTTI SCOLASTICO ===")
    logger.info(f"DBF testi: {DBF_PATH}")
    logger.info(f"DBF tipologie: {TIPOLOGIE_DBF_PATH}")
    logger.info(f"Shopify API version: {SHOPIFY_API_VERSION}")
    logger.info(f"DRY_RUN={int(DRY_RUN)}")

    scolastico_skus = read_scolastico_eans()
    logger.info(f"SKU/EAN SCOLASTICO trovati nel DBF: {len(scolastico_skus)}")
    if not scolastico_skus:
        logger.info("Nessuno SKU SCOLASTICO da elaborare.")
        return 0

    export_op_id = start_bulk_variant_export(endpoint)
    export_op = poll_bulk_operation(endpoint, export_op_id, "Bulk export varianti")
    download_file(export_op.get("url"), EXPORT_JSONL_PATH)

    matched, to_update, already_ok, jsonl_lines = build_mutation_jsonl(EXPORT_JSONL_PATH, scolastico_skus, MUTATION_JSONL_PATH)
    not_found_count = len(scolastico_skus) - matched

    logger.info("=== RIEPILOGO INCROCIO ===")
    logger.info(f"SCOLASTICO nel DBF: {len(scolastico_skus)}")
    logger.info(f"Trovati su Shopify: {matched}")
    logger.info(f"Gia' OK: {already_ok}")
    logger.info(f"Da aggiornare: {to_update}")
    logger.info(f"Non trovati: {not_found_count} | file: {NOT_FOUND_PATH}")
    logger.info(f"JSONL mutation: {MUTATION_JSONL_PATH} | righe: {jsonl_lines}")

    if to_update == 0:
        logger.info("Nessuna variante da aggiornare.")
        return 0

    if DRY_RUN:
        logger.info("DRY_RUN=1: bulk mutation NON avviata. Controlla il JSONL generato e poi esegui con DRY_RUN=0.")
        return 0

    upload_url, staged_upload_path, params = create_staged_upload(endpoint, MUTATION_JSONL_PATH)
    upload_jsonl_to_staged_target(upload_url, params, MUTATION_JSONL_PATH)
    mutation_op_id = start_bulk_mutation(endpoint, staged_upload_path)
    mutation_op = poll_bulk_operation(endpoint, mutation_op_id, "Bulk mutation fix SCOLASTICO")

    result_url = mutation_op.get("url") or mutation_op.get("partialDataUrl")
    if result_url:
        download_file(result_url, RESULT_JSONL_PATH)
        rows, rows_with_errors = summarize_result_jsonl(RESULT_JSONL_PATH)
        logger.info(f"Risultato bulk mutation: righe={rows}, righe_con_errori={rows_with_errors}, file={RESULT_JSONL_PATH}")
        if rows_with_errors:
            return 1
    else:
        logger.warning("Bulk mutation completata ma senza URL risultato.")

    logger.info("=== FINE FIX BULK PRODOTTI SCOLASTICO ===")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
