from dotenv import load_dotenv
from pathlib import Path
import sys
import os
import re
import html
import json
import struct
import datetime
import requests
import time
import logging
from logging.handlers import RotatingFileHandler
from typing import Any, Dict, List, Optional, Tuple, Iterable


# =============================================================================
# Aggiornamento descrizioni Shopify da DBF/FPT WinVaria
# -----------------------------------------------------------------------------
# Questo script legge tutti i record da testi.dbf + testi.fpt e aggiorna SOLO
# descriptionHtml dei prodotti Shopify gia' esistenti.
#
# Matching prodotto:
#   usa il customId gia' usato dallo script di creazione:
#   metafield custom.external_id = CODICE_EAN
#
# Non aggiorna:
#   prezzo, giacenza, immagini, varianti, vendor, tag, categoria, metafield.
#
# A capo:
#   - singolo a capo nel DBF/FPT  -> <br>
#   - riga vuota                 -> nuovo <p>...</p>
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


def _env(name: str, default: str = "") -> str:
    return os.getenv(name, default).strip()


def _env_bool(name: str, default: str = "0") -> bool:
    return _env(name, default) == "1"


def _env_int(name: str, default: str = "0") -> int:
    try:
        return int(_env(name, default))
    except ValueError:
        return int(default)


# =============================================================================
# CONFIG
# =============================================================================

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

DRY_RUN = _env_bool("DRY_RUN", "0")
LIMIT_RECORDS = _env_int("LIMIT_RECORDS", "0")

# Chunk dedicato: se non presente usa BULK_CHUNK_SIZE del vecchio script, poi 2000.
DESCRIPTION_UPDATE_CHUNK_SIZE = _env_int(
    "DESCRIPTION_UPDATE_CHUNK_SIZE",
    _env("BULK_CHUNK_SIZE", "2000")
)

# Se vuoi salvare un file JSONL locale di anteprima: DESCRIPTION_UPDATE_WRITE_PREVIEW=1
DESCRIPTION_UPDATE_WRITE_PREVIEW = _env_bool("DESCRIPTION_UPDATE_WRITE_PREVIEW", "0")

EXTERNAL_ID_NAMESPACE = "custom"
EXTERNAL_ID_KEY = "external_id"

TOKEN_ENV_PATH = ENV_PATH
SHOPIFY_REFRESH_CLIENT_ID = _env("SHOPIFY_REFRESH_CLIENT_ID", "")
SHOPIFY_REFRESH_CLIENT_SECRET = _env("SHOPIFY_REFRESH_CLIENT_SECRET", "")


# =============================================================================
# LOGGING
# =============================================================================

LOG_PATH = str(APP_DIR / "shopify_description_update.log")
logger = logging.getLogger("shopify_description_update")
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
# DBF / FPT minimal reader Visual FoxPro
# =============================================================================

FieldDef = Tuple[str, str, int, int]  # name, type, length, decimals


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

    # Mantiene gli a capo interni; elimina solo terminatori null e spazi esterni.
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
            out[name] = float(s) if s else None
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
# Helpers dati
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
# Descrizione HTML con a capo corretti
# =============================================================================


def text_to_shopify_html(value: Any) -> str:
    """
    Converte un testo DBF/FPT in HTML sicuro per descriptionHtml.

    Regole:
    - accenti e caratteri cp1252 vengono mantenuti;
    - &, <, >, virgolette vengono escapati;
    - \r\n, \r e \n vengono normalizzati;
    - singolo a capo -> <br>;
    - riga vuota -> nuovo paragrafo <p>...</p>.
    """
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


def build_description_html_from_testi_row(row: Dict[str, Any]) -> str:
    """
    Stessa struttura della creazione prodotto:
    - sottotitolo in grassetto come primo paragrafo;
    - NOTE_TEXT come descrizione;
    - se non c'e' nulla, <p></p>.
    """
    sottotitolo = clean_str(get_ci(row, "Sotto_tit", "SOTTO_TIT", "SOTTOTIT", "SOTTOTITOLO", default=""))
    note = clean_str(get_ci(row, "NOTE_TEXT", default=""))

    description_parts: List[str] = []

    if sottotitolo:
        description_parts.append(f"<p><strong>{html.escape(sottotitolo)}</strong></p>")

    if note:
        note_html = text_to_shopify_html(note)
        if note_html:
            description_parts.append(note_html)

    return "\n".join(description_parts) if description_parts else "<p></p>"


# =============================================================================
# Shopify GraphQL + Bulk
# =============================================================================


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


def file_path_str(p: Path) -> str:
    return str(p.resolve())


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

    if not SHOPIFY_REFRESH_CLIENT_ID or not SHOPIFY_REFRESH_CLIENT_SECRET:
        raise RuntimeError(
            "Token Shopify non valido/scaduto e credenziali refresh mancanti: "
            "SHOPIFY_REFRESH_CLIENT_ID / SHOPIFY_REFRESH_CLIENT_SECRET"
        )

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

BULK_MUTATION_PRODUCT_SET_DESCRIPTION = """
mutation ProductSetDescriptionBulk($identifier: ProductSetIdentifiers, $input: ProductSetInput!) {
  productSet(identifier: $identifier, synchronous: false, input: $input) {
    product {
      id
      handle
      title
    }
    userErrors { field message }
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


def chunked(items: List[Any], size: int) -> Iterable[List[Any]]:
    for i in range(0, len(items), size):
        yield items[i:i + size]


# =============================================================================
# Main
# =============================================================================


def build_bulk_line(ean: str, description_html: str) -> str:
    return json.dumps({
        "identifier": {
            "customId": {
                "namespace": EXTERNAL_ID_NAMESPACE,
                "key": EXTERNAL_ID_KEY,
                "value": ean
            }
        },
        "input": {
            # Aggiorna solo la descrizione. Non passiamo varianti, prezzi, immagini, tag.
            "descriptionHtml": description_html
        }
    }, ensure_ascii=False, separators=(",", ":"))


def main() -> None:
    logger.info("=== AVVIO UPDATE DESCRIZIONI SHOPIFY ===")
    logger.info(f"APP_DIR:    {APP_DIR}")
    logger.info(f"Config env: {file_path_str(ENV_PATH)}")
    logger.info(f"DBF:        {DBF_PATH}")
    logger.info(f"FPT:        {FPT_PATH}")
    logger.info(f"Log file:   {LOG_PATH}")
    logger.info(f"DRY_RUN:    {DRY_RUN}")

    records = read_testi_records()
    logger.info(f"Record letti con EAN: {len(records)}")

    items: List[Dict[str, str]] = []
    skipped_no_ean = 0

    for row in records:
        ean = clean_str(get_ci(row, "CODICE_EAN", default=""))
        if not ean:
            skipped_no_ean += 1
            continue

        description_html = build_description_html_from_testi_row(row)
        items.append({
            "ean": ean,
            "description_html": description_html,
        })

    logger.info(f"Descrizioni preparate: {len(items)} | record senza EAN saltati: {skipped_no_ean}")

    if DESCRIPTION_UPDATE_WRITE_PREVIEW:
        preview_path = APP_DIR / "description_update_preview.jsonl"
        preview_path.write_text(
            "\n".join(build_bulk_line(it["ean"], it["description_html"]) for it in items) + "\n",
            encoding="utf-8"
        )
        logger.info(f"Preview JSONL scritta in: {file_path_str(preview_path)}")

    if DRY_RUN:
        logger.info("DRY_RUN=1: nessuna chiamata a Shopify. Fine.")
        logger.info("=== UPDATE DESCRIZIONI COMPLETATO IN DRY RUN ===")
        return

    if not SHOPIFY_SHOP or not SHOPIFY_ACCESS_TOKEN:
        raise RuntimeError("Config mancante: SHOPIFY_SHOP e SHOPIFY_ACCESS_TOKEN, oppure usa DRY_RUN=1.")

    endpoint = f"https://{SHOPIFY_SHOP}/admin/api/{SHOPIFY_API_VERSION}/graphql.json"

    updated = 0
    failed = 0
    total_chunks = (len(items) + DESCRIPTION_UPDATE_CHUNK_SIZE - 1) // DESCRIPTION_UPDATE_CHUNK_SIZE if items else 0
    logger.info(f"Bulk update descrizioni: {len(items)} prodotti | chunk={DESCRIPTION_UPDATE_CHUNK_SIZE}")

    for chunk_idx, chunk_items in enumerate(chunked(items, DESCRIPTION_UPDATE_CHUNK_SIZE), start=1):
        t_chunk0 = time.perf_counter()
        logger.info(f"[description] Chunk {chunk_idx}/{total_chunks} | preparo JSONL")

        eans_in_order = [it["ean"] for it in chunk_items]
        lines = [build_bulk_line(it["ean"], it["description_html"]) for it in chunk_items]
        jsonl_bytes = ("\n".join(lines) + "\n").encode("utf-8")

        staged_path = staged_upload_jsonl(
            endpoint,
            SHOPIFY_ACCESS_TOKEN,
            filename=f"description_update_chunk{chunk_idx}.jsonl",
            jsonl_bytes=jsonl_bytes
        )

        op_id = bulk_run_mutation(
            endpoint,
            SHOPIFY_ACCESS_TOKEN,
            mutation_str=BULK_MUTATION_PRODUCT_SET_DESCRIPTION,
            staged_upload_path=staged_path,
            client_identifier=f"description_update_chunk{chunk_idx}"
        )

        op = poll_bulk_operation(endpoint, SHOPIFY_ACCESS_TOKEN, op_id)
        status = (op.get("status") or "").upper()
        if status != "COMPLETED":
            log_error("Bulk descrizioni non COMPLETED", op)
            raise RuntimeError("Bulk descrizioni fallita")

        out_url = op.get("url")
        if not out_url:
            log_error("Bulk descrizioni COMPLETED ma url output mancante", op)
            raise RuntimeError("Output url mancante")

        for i, out_line in enumerate(iter_jsonl_from_url(out_url)):
            ean = eans_in_order[i] if i < len(eans_in_order) else "?"
            payload = (out_line.get("data") or {}).get("productSet") or {}
            uerrs = payload.get("userErrors") or []
            product = payload.get("product") or {}

            if uerrs or not product:
                failed += 1
                log_error(f"Errore update descrizione (EAN={ean})", {
                    "userErrors": uerrs,
                    "line": out_line,
                })
                continue

            updated += 1

        logger.info(
            f"[description] Chunk {chunk_idx}/{total_chunks} | COMPLETED "
            f"in {time.perf_counter() - t_chunk0:.2f}s"
        )

    logger.info("=== UPDATE DESCRIZIONI COMPLETATO ===")
    logger.info(f"Descrizioni aggiornate: {updated}")
    logger.info(f"Errori/non trovati:      {failed}")
    logger.info(f"Log file:               {LOG_PATH}")


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

        if getattr(sys, "frozen", False):
            try:
                input("\nPremi Invio per chiudere...")
            except EOFError:
                pass
        raise
