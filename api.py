from __future__ import annotations

import re
import time
from threading import RLock
from typing import Any, Dict, List, Optional, Tuple

import requests
from fastapi import FastAPI, HTTPException, Query
from fastapi.middleware.cors import CORSMiddleware

app = FastAPI(title="API Scuole + Libri")

# =========================================================
# CORS
# =========================================================
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],  # in produzione restringi
    allow_credentials=False,
    allow_methods=["*"],
    allow_headers=["*"],
)

# =========================================================
# CONFIG
# =========================================================
MIUR_SCUOLE_ENDPOINT = "https://dati.istruzione.it/opendata/SCUANAGRAFESTAT/query"
MIUR_OPENDATA_BASE = "https://dati.istruzione.it/opendata"

HTTP_TIMEOUT = 60
SPARQL_PAGE_SIZE = 1000
USER_AGENT = "fastapi-scuole-libri/3.0"

# =========================================================
# CACHE TTL SEMPLICE
# =========================================================
class TTLCache:
    def __init__(self, ttl_seconds: int = 3600, max_items: int = 1024):
        self.ttl_seconds = ttl_seconds
        self.max_items = max_items
        self._store: Dict[str, Tuple[float, Any]] = {}
        self._lock = RLock()

    def get(self, key: str) -> Optional[Any]:
        now = time.time()
        with self._lock:
            item = self._store.get(key)
            if not item:
                return None

            expires_at, value = item
            if expires_at < now:
                self._store.pop(key, None)
                return None

            return value

    def set(self, key: str, value: Any) -> None:
        now = time.time()
        with self._lock:
            if len(self._store) >= self.max_items:
                self._evict(now)
            self._store[key] = (now + self.ttl_seconds, value)

    def _evict(self, now: float) -> None:
        expired_keys = [k for k, (exp, _) in self._store.items() if exp < now]
        for key in expired_keys[: max(1, self.max_items // 10)]:
            self._store.pop(key, None)

        if len(self._store) >= self.max_items:
            first_key = next(iter(self._store), None)
            if first_key is not None:
                self._store.pop(first_key, None)


cache_province = TTLCache(ttl_seconds=24 * 3600, max_items=128)
cache_comuni = TTLCache(ttl_seconds=24 * 3600, max_items=1024)
cache_scuole = TTLCache(ttl_seconds=24 * 3600, max_items=8192)
cache_search = TTLCache(ttl_seconds=6 * 3600, max_items=4096)
cache_libri = TTLCache(ttl_seconds=24 * 3600, max_items=8192)

# =========================================================
# COSTANTI DOMINIO
# =========================================================
REGIONI_CANONICHE = [
    "ABRUZZO",
    "BASILICATA",
    "CALABRIA",
    "CAMPANIA",
    "EMILIA-ROMAGNA",
    "FRIULI-VENEZIA GIULIA",
    "LAZIO",
    "LIGURIA",
    "LOMBARDIA",
    "MARCHE",
    "MOLISE",
    "PIEMONTE",
    "PUGLIA",
    "SARDEGNA",
    "SICILIA",
    "TOSCANA",
    "TRENTINO-ALTO ADIGE",
    "UMBRIA",
    "VALLE D'AOSTA",
    "VENETO",
]

REGION_ALIASES = {
    "ABRUZZO": "ABRUZZO",
    "BASILICATA": "BASILICATA",
    "CALABRIA": "CALABRIA",
    "CAMPANIA": "CAMPANIA",
    "EMILIA ROMAGNA": "EMILIA-ROMAGNA",
    "EMILIA-ROMAGNA": "EMILIA-ROMAGNA",
    "FRIULI VENEZIA GIULIA": "FRIULI-VENEZIA GIULIA",
    "FRIULI-VENEZIA GIULIA": "FRIULI-VENEZIA GIULIA",
    "LAZIO": "LAZIO",
    "LIGURIA": "LIGURIA",
    "LOMBARDIA": "LOMBARDIA",
    "MARCHE": "MARCHE",
    "MOLISE": "MOLISE",
    "PIEMONTE": "PIEMONTE",
    "PUGLIA": "PUGLIA",
    "SARDEGNA": "SARDEGNA",
    "SICILIA": "SICILIA",
    "TOSCANA": "TOSCANA",
    "TRENTINO ALTO ADIGE": "TRENTINO-ALTO ADIGE",
    "TRENTINO-ALTO ADIGE": "TRENTINO-ALTO ADIGE",
    "TRENTINO-ALTO ADIGE/SUDTIROL": "TRENTINO-ALTO ADIGE",
    "TRENTINO-ALTO ADIGE/SÜDTIROL": "TRENTINO-ALTO ADIGE",
    "UMBRIA": "UMBRIA",
    "VALLE D AOSTA": "VALLE D'AOSTA",
    "VALLE D'AOSTA": "VALLE D'AOSTA",
    "VALLEE D AOSTE": "VALLE D'AOSTA",
    "VALLE D'AOSTA/VALLEE D'AOSTE": "VALLE D'AOSTA",
    "VENETO": "VENETO",
}

ALT_DATASET_BY_REGION = {
    "ABRUZZO": "ALTABRUZZO",
    "BASILICATA": "ALTBASILICATA",
    "CALABRIA": "ALTCALABRIA",
    "CAMPANIA": "ALTCAMPANIA",
    "EMILIA-ROMAGNA": "ALTEMILIAROMAGNA",
    "FRIULI-VENEZIA GIULIA": "ALTFRIULIVENEZIAGIULIA",
    "LAZIO": "ALTLAZIO",
    "LIGURIA": "ALTLIGURIA",
    "LOMBARDIA": "ALTLOMBARDIA",
    "MARCHE": "ALTMARCHE",
    "MOLISE": "ALTMOLISE",
    "PIEMONTE": "ALTPIEMONTE",
    "PUGLIA": "ALTPUGLIA",
    "SARDEGNA": "ALTSARDEGNA",
    "SICILIA": "ALTSICILIA",
    "TOSCANA": "ALTTOSCANA",
    "TRENTINO-ALTO ADIGE": "ALTTRENTINOALTOADIGE",
    "UMBRIA": "ALTUMBRIA",
    "VALLE D'AOSTA": "ALTVALLEDAOSTA",
    "VENETO": "ALTVENETO",
}

# =========================================================
# UTILS
# =========================================================
http_session = requests.Session()
http_session.headers.update(
    {
        "Accept": "application/sparql-results+json, application/json;q=0.9, */*;q=0.8",
        "User-Agent": USER_AGENT,
    }
)


def norm(value: Optional[str]) -> str:
    return (value or "").strip().upper()


def normalize_spaces(value: Optional[str]) -> str:
    return re.sub(r"\s+", " ", (value or "").strip())


def normalize_regione_input(value: str) -> str:
    cleaned = normalize_spaces(value).upper().replace("’", "'")
    if cleaned in REGION_ALIASES:
        return REGION_ALIASES[cleaned]

    fallback = re.sub(r"[^A-Z0-9/' -]", " ", cleaned)
    fallback = re.sub(r"\s+", " ", fallback).strip()
    if fallback in REGION_ALIASES:
        return REGION_ALIASES[fallback]

    raise HTTPException(status_code=400, detail=f"Regione non riconosciuta: {value}")


def require_not_blank(value: str, field_name: str) -> str:
    cleaned = normalize_spaces(value)
    if not cleaned:
        raise HTTPException(status_code=400, detail=f"Il parametro '{field_name}' è obbligatorio")
    return cleaned


def sparql_escape_string(value: str) -> str:
    return (
        value.replace("\\", "\\\\")
        .replace('"', '\\"')
        .replace("\n", " ")
        .replace("\r", " ")
    )


def binding_value(item: Dict[str, Any], key: str) -> Optional[str]:
    return item.get(key, {}).get("value")


def build_cache_key(prefix: str, *parts: Any) -> str:
    return "::".join([prefix, *[str(part) for part in parts]])


def extract_bindings(payload: Dict[str, Any]) -> List[Dict[str, Any]]:
    return payload.get("results", {}).get("bindings", [])


def session_get_json(url: str, *, params: Dict[str, str]) -> Dict[str, Any]:
    try:
        response = http_session.get(
            url,
            params=params,
            timeout=HTTP_TIMEOUT,
        )
        response.raise_for_status()
    except requests.RequestException as exc:
        raise HTTPException(
            status_code=502,
            detail=f"Errore chiamando il servizio MIUR: {exc}",
        ) from exc

    try:
        return response.json()
    except ValueError as exc:
        raise HTTPException(
            status_code=502,
            detail="Risposta non valida dal servizio MIUR",
        ) from exc


def execute_sparql(endpoint: str, query: str) -> List[Dict[str, Any]]:
    payload = session_get_json(endpoint, params={"query": query})
    return extract_bindings(payload)


# =========================================================
# QUERY BUILDERS
# =========================================================
def build_province_query(regione: str) -> str:
    regione_safe = sparql_escape_string(regione.lower())
    return f"""
PREFIX miur: <http://www.miur.it/ns/miur#>

SELECT ?Provincia (COUNT(DISTINCT ?S) AS ?Totale)
WHERE {{
  GRAPH ?g {{
    ?S miur:REGIONE ?Regione .
    ?S miur:PROVINCIA ?Provincia .
    FILTER (lcase(str(?Regione)) = "{regione_safe}")
  }}
}}
GROUP BY ?Provincia
ORDER BY ?Provincia
""".strip()


def build_comuni_query(regione: str, provincia: str) -> str:
    regione_safe = sparql_escape_string(regione.lower())
    provincia_safe = sparql_escape_string(provincia.lower())
    return f"""
PREFIX miur: <http://www.miur.it/ns/miur#>

SELECT ?DescrizioneComune (COUNT(DISTINCT ?S) AS ?Totale)
WHERE {{
  GRAPH ?g {{
    ?S miur:REGIONE ?Regione .
    ?S miur:PROVINCIA ?Provincia .
    ?S miur:DESCRIZIONECOMUNE ?DescrizioneComune .
    FILTER (lcase(str(?Regione)) = "{regione_safe}")
    FILTER (lcase(str(?Provincia)) = "{provincia_safe}")
  }}
}}
GROUP BY ?DescrizioneComune
ORDER BY ?DescrizioneComune
""".strip()


def build_scuole_count_query(regione: str, provincia: str, comune: str) -> str:
    regione_safe = sparql_escape_string(regione.lower())
    provincia_safe = sparql_escape_string(provincia.lower())
    comune_safe = sparql_escape_string(comune.lower())
    return f"""
PREFIX miur: <http://www.miur.it/ns/miur#>

SELECT (COUNT(DISTINCT ?CodiceScuola) AS ?Totale)
WHERE {{
  GRAPH ?g {{
    ?S miur:REGIONE ?Regione .
    ?S miur:PROVINCIA ?Provincia .
    ?S miur:DESCRIZIONECOMUNE ?DescrizioneComune .
    ?S miur:CODICESCUOLA ?CodiceScuola .
    FILTER (lcase(str(?Regione)) = "{regione_safe}")
    FILTER (lcase(str(?Provincia)) = "{provincia_safe}")
    FILTER (lcase(str(?DescrizioneComune)) = "{comune_safe}")
  }}
}}
""".strip()


def build_scuole_query(regione: str, provincia: str, comune: str, limit: int, offset: int) -> str:
    regione_safe = sparql_escape_string(regione.lower())
    provincia_safe = sparql_escape_string(provincia.lower())
    comune_safe = sparql_escape_string(comune.lower())

    return f"""
PREFIX miur: <http://www.miur.it/ns/miur#>

SELECT DISTINCT
  ?CodiceScuola
  ?DenominazioneScuola
  ?IndirizzoScuola
  ?IndirizzoEmailScuola
  ?IndirizzoPecScuola
  ?SitoWebScuola
  ?DescrizioneTipologiaGradoIstruzioneScuola
  ?DescrizioneCaratteristicaScuola
WHERE {{
  GRAPH ?g {{
    ?S miur:REGIONE ?Regione .
    ?S miur:PROVINCIA ?Provincia .
    ?S miur:DESCRIZIONECOMUNE ?DescrizioneComune .
    ?S miur:CODICESCUOLA ?CodiceScuola .
    ?S miur:DENOMINAZIONESCUOLA ?DenominazioneScuola .

    OPTIONAL {{ ?S miur:INDIRIZZOSCUOLA ?IndirizzoScuola . }}
    OPTIONAL {{ ?S miur:INDIRIZZOEMAILSCUOLA ?IndirizzoEmailScuola . }}
    OPTIONAL {{ ?S miur:INDIRIZZOPECSCUOLA ?IndirizzoPecScuola . }}
    OPTIONAL {{ ?S miur:SITOWEBSCUOLA ?SitoWebScuola . }}
    OPTIONAL {{ ?S miur:DESCRIZIONETIPOLOGIAGRADOISTRUZIONESCUOLA ?DescrizioneTipologiaGradoIstruzioneScuola . }}
    OPTIONAL {{ ?S miur:DESCRIZIONECARATTERISTICASCUOLA ?DescrizioneCaratteristicaScuola . }}

    FILTER (lcase(str(?Regione)) = "{regione_safe}")
    FILTER (lcase(str(?Provincia)) = "{provincia_safe}")
    FILTER (lcase(str(?DescrizioneComune)) = "{comune_safe}")
  }}
}}
ORDER BY ?DenominazioneScuola ?CodiceScuola
LIMIT {limit}
OFFSET {offset}
""".strip()


def build_scuole_search_count_query(regione: str, q: str) -> str:
    regione_safe = sparql_escape_string(regione.lower())
    q_safe = sparql_escape_string(q.lower())
    return f"""
PREFIX miur: <http://www.miur.it/ns/miur#>

SELECT (COUNT(DISTINCT ?CodiceScuola) AS ?Totale)
WHERE {{
  GRAPH ?g {{
    ?S miur:REGIONE ?Regione .
    ?S miur:PROVINCIA ?Provincia .
    ?S miur:DESCRIZIONECOMUNE ?DescrizioneComune .
    ?S miur:CODICESCUOLA ?CodiceScuola .
    ?S miur:DENOMINAZIONESCUOLA ?DenominazioneScuola .

    FILTER (lcase(str(?Regione)) = "{regione_safe}")
    FILTER (
      CONTAINS(lcase(str(?DenominazioneScuola)), "{q_safe}") ||
      CONTAINS(lcase(str(?CodiceScuola)), "{q_safe}") ||
      CONTAINS(lcase(str(?DescrizioneComune)), "{q_safe}") ||
      CONTAINS(lcase(str(?Provincia)), "{q_safe}")
    )
  }}
}}
""".strip()


def build_scuole_search_query(regione: str, q: str, limit: int, offset: int) -> str:
    regione_safe = sparql_escape_string(regione.lower())
    q_safe = sparql_escape_string(q.lower())
    return f"""
PREFIX miur: <http://www.miur.it/ns/miur#>

SELECT DISTINCT
  ?Provincia
  ?DescrizioneComune
  ?CodiceScuola
  ?DenominazioneScuola
  ?IndirizzoScuola
  ?SitoWebScuola
WHERE {{
  GRAPH ?g {{
    ?S miur:REGIONE ?Regione .
    ?S miur:PROVINCIA ?Provincia .
    ?S miur:DESCRIZIONECOMUNE ?DescrizioneComune .
    ?S miur:CODICESCUOLA ?CodiceScuola .
    ?S miur:DENOMINAZIONESCUOLA ?DenominazioneScuola .

    OPTIONAL {{ ?S miur:INDIRIZZOSCUOLA ?IndirizzoScuola . }}
    OPTIONAL {{ ?S miur:SITOWEBSCUOLA ?SitoWebScuola . }}

    FILTER (lcase(str(?Regione)) = "{regione_safe}")
    FILTER (
      CONTAINS(lcase(str(?DenominazioneScuola)), "{q_safe}") ||
      CONTAINS(lcase(str(?CodiceScuola)), "{q_safe}") ||
      CONTAINS(lcase(str(?DescrizioneComune)), "{q_safe}") ||
      CONTAINS(lcase(str(?Provincia)), "{q_safe}")
    )
  }}
}}
ORDER BY ?DenominazioneScuola ?CodiceScuola
LIMIT {limit}
OFFSET {offset}
""".strip()


def build_libri_query(codicescuola: str, limit: int, offset: int) -> str:
    codice_safe = sparql_escape_string(codicescuola.strip().lower())
    return f"""
PREFIX miur: <http://www.miur.it/ns/miur#>

SELECT
  ?CodiceScuola ?AnnoCorso ?SezioneAnno ?TipoGradoScuola ?Combinazione
  ?Disciplina ?CodiceISBN ?Autori ?Titolo ?Sottotitolo ?Volume
  ?Editore ?Prezzo ?NuovaAdoz ?DaAcquist ?Consigliato
WHERE {{
  GRAPH ?g {{
    ?S miur:CODICESCUOLA ?CodiceScuola .
    ?S miur:ANNOCORSO ?AnnoCorso .
    ?S miur:SEZIONEANNO ?SezioneAnno .

    OPTIONAL {{ ?S miur:TIPOGRADOSCUOLA ?TipoGradoScuola . }}
    OPTIONAL {{ ?S miur:COMBINAZIONE ?Combinazione . }}
    OPTIONAL {{ ?S miur:DISCIPLINA ?Disciplina . }}
    OPTIONAL {{ ?S miur:CODICEISBN ?CodiceISBN . }}
    OPTIONAL {{ ?S miur:AUTORI ?Autori . }}
    OPTIONAL {{ ?S miur:TITOLO ?Titolo . }}
    OPTIONAL {{ ?S miur:SOTTOTITOLO ?Sottotitolo . }}
    OPTIONAL {{ ?S miur:VOLUME ?Volume . }}
    OPTIONAL {{ ?S miur:EDITORE ?Editore . }}
    OPTIONAL {{ ?S miur:PREZZO ?Prezzo . }}
    OPTIONAL {{ ?S miur:NUOVAADOZ ?NuovaAdoz . }}
    OPTIONAL {{ ?S miur:DAACQUIST ?DaAcquist . }}
    OPTIONAL {{ ?S miur:CONSIGLIATO ?Consigliato . }}

    FILTER (lcase(str(?CodiceScuola)) = "{codice_safe}")
  }}
}}
ORDER BY ?SezioneAnno ?AnnoCorso ?Disciplina ?Titolo ?CodiceISBN
LIMIT {limit}
OFFSET {offset}
""".strip()

# =========================================================
# PARSERS
# =========================================================
def parse_single_count(bindings: List[Dict[str, Any]]) -> int:
    if not bindings:
        return 0
    return int(float(binding_value(bindings[0], "Totale") or 0))


def parse_province(bindings: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    result: List[Dict[str, Any]] = []
    for item in bindings:
        provincia = binding_value(item, "Provincia")
        totale = binding_value(item, "Totale")
        if provincia:
            result.append(
                {
                    "Provincia": provincia,
                    "Totale": int(float(totale or 0)),
                }
            )
    return result


def parse_comuni(bindings: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    result: List[Dict[str, Any]] = []
    for item in bindings:
        comune = binding_value(item, "DescrizioneComune")
        totale = binding_value(item, "Totale")
        if comune:
            result.append(
                {
                    "DescrizioneComune": comune,
                    "Totale": int(float(totale or 0)),
                }
            )
    return result


def parse_scuole(bindings: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    return [
        {
            "CodiceScuola": binding_value(item, "CodiceScuola"),
            "DenominazioneScuola": binding_value(item, "DenominazioneScuola"),
            "IndirizzoScuola": binding_value(item, "IndirizzoScuola"),
            "IndirizzoEmailScuola": binding_value(item, "IndirizzoEmailScuola"),
            "IndirizzoPecScuola": binding_value(item, "IndirizzoPecScuola"),
            "SitoWebScuola": binding_value(item, "SitoWebScuola"),
            "DescrizioneTipologiaGradoIstruzioneScuola": binding_value(
                item, "DescrizioneTipologiaGradoIstruzioneScuola"
            ),
            "DescrizioneCaratteristicaScuola": binding_value(
                item, "DescrizioneCaratteristicaScuola"
            ),
        }
        for item in bindings
    ]


def parse_search_scuole(bindings: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    return [
        {
            "Provincia": binding_value(item, "Provincia"),
            "DescrizioneComune": binding_value(item, "DescrizioneComune"),
            "CodiceScuola": binding_value(item, "CodiceScuola"),
            "DenominazioneScuola": binding_value(item, "DenominazioneScuola"),
            "IndirizzoScuola": binding_value(item, "IndirizzoScuola"),
            "SitoWebScuola": binding_value(item, "SitoWebScuola"),
        }
        for item in bindings
    ]


def parse_libri(bindings: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    return [
        {
            "CodiceScuola": binding_value(item, "CodiceScuola"),
            "AnnoCorso": binding_value(item, "AnnoCorso"),
            "SezioneAnno": binding_value(item, "SezioneAnno"),
            "TipoGradoScuola": binding_value(item, "TipoGradoScuola"),
            "Combinazione": binding_value(item, "Combinazione"),
            "Disciplina": binding_value(item, "Disciplina"),
            "CodiceISBN": binding_value(item, "CodiceISBN"),
            "Autori": binding_value(item, "Autori"),
            "Titolo": binding_value(item, "Titolo"),
            "Sottotitolo": binding_value(item, "Sottotitolo"),
            "Volume": binding_value(item, "Volume"),
            "Editore": binding_value(item, "Editore"),
            "Prezzo": binding_value(item, "Prezzo"),
            "NuovaAdoz": binding_value(item, "NuovaAdoz"),
            "DaAcquist": binding_value(item, "DaAcquist"),
            "Consigliato": binding_value(item, "Consigliato"),
        }
        for item in bindings
    ]

# =========================================================
# FETCHERS
# =========================================================
def fetch_province(regione: str) -> List[Dict[str, Any]]:
    cache_key = build_cache_key("province", regione)
    cached = cache_province.get(cache_key)
    if cached is not None:
        return cached

    bindings = execute_sparql(MIUR_SCUOLE_ENDPOINT, build_province_query(regione))
    result = parse_province(bindings)
    cache_province.set(cache_key, result)
    return result


def fetch_comuni(regione: str, provincia: str) -> List[Dict[str, Any]]:
    cache_key = build_cache_key("comuni", regione, provincia)
    cached = cache_comuni.get(cache_key)
    if cached is not None:
        return cached

    bindings = execute_sparql(MIUR_SCUOLE_ENDPOINT, build_comuni_query(regione, provincia))
    result = parse_comuni(bindings)
    cache_comuni.set(cache_key, result)
    return result


def fetch_scuole(regione: str, provincia: str, comune: str, page: int, page_size: int) -> Dict[str, Any]:
    cache_key = build_cache_key("scuole", regione, provincia, comune, page, page_size)
    cached = cache_scuole.get(cache_key)
    if cached is not None:
        return cached

    offset = (page - 1) * page_size

    total_bindings = execute_sparql(
        MIUR_SCUOLE_ENDPOINT,
        build_scuole_count_query(regione, provincia, comune),
    )
    totale = parse_single_count(total_bindings)

    row_bindings = execute_sparql(
        MIUR_SCUOLE_ENDPOINT,
        build_scuole_query(regione, provincia, comune, page_size, offset),
    )
    scuole = parse_scuole(row_bindings)

    result = {
        "totale": totale,
        "page": page,
        "page_size": page_size,
        "has_next": offset + len(scuole) < totale,
        "scuole": scuole,
    }
    cache_scuole.set(cache_key, result)
    return result


def fetch_search_scuole(regione: str, q: str, page: int, page_size: int) -> Dict[str, Any]:
    cache_key = build_cache_key("search", regione, q.lower(), page, page_size)
    cached = cache_search.get(cache_key)
    if cached is not None:
        return cached

    offset = (page - 1) * page_size

    total_bindings = execute_sparql(
        MIUR_SCUOLE_ENDPOINT,
        build_scuole_search_count_query(regione, q),
    )
    totale = parse_single_count(total_bindings)

    row_bindings = execute_sparql(
        MIUR_SCUOLE_ENDPOINT,
        build_scuole_search_query(regione, q, page_size, offset),
    )
    scuole = parse_search_scuole(row_bindings)

    result = {
        "totale": totale,
        "page": page,
        "page_size": page_size,
        "has_next": offset + len(scuole) < totale,
        "scuole": scuole,
    }
    cache_search.set(cache_key, result)
    return result


def fetch_libri(regione: str, codicescuola: str) -> Dict[str, Any]:
    cache_key = build_cache_key("libri", regione, codicescuola.upper())
    cached = cache_libri.get(cache_key)
    if cached is not None:
        return cached

    dataset_name = ALT_DATASET_BY_REGION.get(regione)
    if not dataset_name:
        raise HTTPException(
            status_code=400,
            detail=f"Nessun dataset libri configurato per {regione}",
        )

    endpoint = f"{MIUR_OPENDATA_BASE}/{dataset_name}/query"
    all_rows: List[Dict[str, Any]] = []
    offset = 0

    while True:
        query = build_libri_query(
            codicescuola=codicescuola,
            limit=SPARQL_PAGE_SIZE,
            offset=offset,
        )

        try:
            bindings = execute_sparql(endpoint, query)
        except HTTPException as exc:
            raise HTTPException(
                status_code=502,
                detail=f"Errore nella chiamata al servizio libri MIUR (dataset {dataset_name}): {exc.detail}",
            ) from exc

        rows = parse_libri(bindings)
        if not rows:
            break

        all_rows.extend(rows)

        if len(rows) < SPARQL_PAGE_SIZE:
            break

        offset += SPARQL_PAGE_SIZE

    result = {
        "dataset": dataset_name,
        "endpoint": endpoint,
        "libri": all_rows,
    }
    cache_libri.set(cache_key, result)
    return result

# =========================================================
# API
# =========================================================
@app.get("/regioni")
def get_regioni() -> Dict[str, List[str]]:
    return {"regioni": REGIONI_CANONICHE}


@app.get("/province")
def get_province(regione: str = Query(...)) -> Dict[str, Any]:
    regione_norm = normalize_regione_input(regione)
    province = fetch_province(regione_norm)
    return {
        "regione": regione_norm,
        "totale": len(province),
        "province": province,
    }


@app.get("/comuni")
def get_comuni_api(
    regione: str = Query(...),
    provincia: str = Query(...),
) -> Dict[str, Any]:
    regione_norm = normalize_regione_input(regione)
    provincia_input = require_not_blank(provincia, "provincia")
    comuni = fetch_comuni(regione_norm, provincia_input)

    return {
        "regione": regione_norm,
        "provincia": provincia_input,
        "totale": len(comuni),
        "comuni": comuni,
    }


@app.get("/scuole")
def get_scuole_api(
    regione: str = Query(...),
    provincia: str = Query(...),
    comune: str = Query(...),
    page: int = Query(1, ge=1),
    page_size: int = Query(20, ge=1, le=100),
) -> Dict[str, Any]:
    regione_norm = normalize_regione_input(regione)
    provincia_input = require_not_blank(provincia, "provincia")
    comune_input = require_not_blank(comune, "comune")

    result = fetch_scuole(regione_norm, provincia_input, comune_input, page, page_size)
    return {
        "regione": regione_norm,
        "provincia": provincia_input,
        "comune": comune_input,
        **result,
    }


@app.get("/scuole/search")
def search_scuole_api(
    regione: str = Query(...),
    q: str = Query(..., min_length=2),
    page: int = Query(1, ge=1),
    page_size: int = Query(20, ge=1, le=100),
) -> Dict[str, Any]:
    regione_norm = normalize_regione_input(regione)
    q_input = require_not_blank(q, "q")

    if len(q_input) < 2:
        raise HTTPException(status_code=400, detail="La ricerca deve avere almeno 2 caratteri")

    result = fetch_search_scuole(regione_norm, q_input, page, page_size)
    return {
        "regione": regione_norm,
        "q": q_input,
        **result,
    }


@app.get("/libri")
def get_libri_api(
    codicescuola: str = Query(..., description="Codice scuola"),
    regione: str = Query(..., description="Nome regione"),
) -> Dict[str, Any]:
    codice_input = require_not_blank(codicescuola, "codicescuola")
    regione_norm = normalize_regione_input(regione)

    result = fetch_libri(
        regione=regione_norm,
        codicescuola=codice_input,
    )

    return {
        "regione": regione_norm,
        "dataset": result["dataset"],
        "endpoint": result["endpoint"],
        "codicescuola": norm(codice_input),
        "totale": len(result["libri"]),
        "libri": result["libri"],
    }