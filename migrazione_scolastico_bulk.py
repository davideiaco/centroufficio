#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""Migrazione massiva SCOLASTICO. Da eseguire a mano, NON da Task Scheduler.

Collocare nella cartella del conn_sync_bulk.py funzionante, insieme a config.env.
Utilizza le sue funzioni di lettura DBF, autenticazione, lock e SQLite.
API Shopify Admin GraphQL 2026-01+.

Flusso (le fasi sono SEQUENZIALI; le righe di una stessa bulk sono indipendenti):
  lettura Shopify bulk -> policy CONTINUE bulk -> tracked true bulk ->
  nuova lettura bulk -> inventoryActivate bulk (solo dove manca la sede) ->
  inventorySetQuantities in batch -> lettura/verifica Shopify bulk -> SQLite.
La scrittura sullo stato locale avviene SOLO dopo verifica effettiva.
"""

from __future__ import annotations

import argparse
import collections
import csv
import datetime as dt
import hashlib
import json
import os
from pathlib import Path
import re
import sqlite3
import sys
import time
import uuid
from typing import Any

# La versione di produzione e' importata senza eseguire main() o il suo task.
import conn_sync_bulk as sync

LOG = sync.logger
DIR = sync.APP_DIR
PENDING = DIR / "scolastico_bulk_operazione_in_corso.json"

BULK_READ = """
mutation ScolasticoBulkQuery($query: String!) {
  bulkOperationRunQuery(query: $query) {
    bulkOperation { id status }
    userErrors { field message code }
  }
}
""".strip()

BULK_SCAN = """{
  productVariants {
    edges {
      node {
        id
        sku
        inventoryPolicy
        product { id }
        inventoryItem {
          id
          tracked
          inventoryLevel(locationId: LOCATION_GID) {
            id
            quantities(names: ["available"]) { name quantity }
          }
        }
      }
    }
  }
}"""

MUT_POLICY = """
mutation PolicyBulk($productId: ID!, $variants: [ProductVariantsBulkInput!]!) {
  productVariantsBulkUpdate(productId: $productId, variants: $variants) {
    productVariants { id inventoryPolicy }
    userErrors { field message }
  }
}
""".strip()

MUT_TRACK = """
mutation TrackedBulk($id: ID!, $input: InventoryItemInput!) {
  inventoryItemUpdate(id: $id, input: $input) {
    inventoryItem { id tracked }
    userErrors { field message }
  }
}
""".strip()

MUT_ACTIVATE = """
mutation ActivateBulk($inventoryItemId: ID!, $locationId: ID!, $key: String!) {
  inventoryActivate(inventoryItemId: $inventoryItemId, locationId: $locationId)
    @idempotent(key: $key) {
    inventoryLevel { id }
    userErrors { field message }
  }
}
""".strip()


def gql(query: str, variables: dict[str, Any]) -> dict[str, Any]:
    return sync.shopify_graphql(ENDPOINT, sync.SHOPIFY_ACCESS_TOKEN, query, variables)


def write_json_atomic(path: Path, data: dict) -> None:
    tmp = path.with_name(path.name + ".tmp")
    tmp.write_text(json.dumps(data, ensure_ascii=False, indent=2), encoding="utf-8")
    os.replace(tmp, path)


def track_pending(stage: str, operation_id: str) -> None:
    write_json_atomic(PENDING, {
        "stage": stage, "operation_id": operation_id,
        "created_utc": dt.datetime.now(dt.UTC).isoformat(),
    })


def wait_operation(stage: str, op_id: str) -> dict[str, Any]:
    LOG.info("[bulk] %s | op=%s | attesa completamento Shopify", stage, op_id)
    op = sync.poll_bulk_operation(ENDPOINT, sync.SHOPIFY_ACCESS_TOKEN, op_id, poll_seconds=10)
    if (op.get("status") or "").upper() != "COMPLETED" or not op.get("url"):
        raise RuntimeError(
            f"Bulk {stage} non COMPLETED (stato={op.get('status')}, "
            f"errore={op.get('errorCode')}, partialDataUrl={bool(op.get('partialDataUrl'))}). "
            "Rieseguire per ricostruire lo stato da Shopify; non usare dati parziali."
        )
    return op


def recover_pending() -> None:
    """Impedisce di lanciare una nuova scrittura mentre una vecchia bulk lavora."""
    if not PENDING.exists():
        return
    old = json.loads(PENDING.read_text(encoding="utf-8"))
    op_id = old.get("operation_id")
    if not op_id:
        raise RuntimeError(f"File operazione incompleto: {PENDING}")
    LOG.warning("Operazione precedente rilevata (%s). Attendo il suo termine prima di procedere.", op_id)
    op = sync.poll_bulk_operation(ENDPOINT, sync.SHOPIFY_ACCESS_TOKEN, op_id, poll_seconds=10)
    if (op.get("status") or "").upper() != "COMPLETED":
        raise RuntimeError(
            f"Operazione precedente {op_id} terminata con stato {op.get('status')}; "
            f"consultare log, poi eliminare manualmente {PENDING.name} "
            "SOLTANTO dopo aver confermato che nessuna bulk e' in corso."
        )
    PENDING.unlink()
    LOG.info("Operazione precedente conclusa. Riparto da una nuova lettura Shopify.")


def submit_bulk_query() -> str:
    loc = sync.SHOPIFY_LOCATION_ID
    if not re.fullmatch(r"gid://shopify/Location/\d+", loc):
        raise ValueError(f"SHOPIFY_LOCATION_ID non valido: {loc!r}")
    query = BULK_SCAN.replace("LOCATION_GID", json.dumps(loc))
    payload = (gql(BULK_READ, {"query": query}).get("data") or {}).get("bulkOperationRunQuery") or {}
    if payload.get("userErrors"):
        raise RuntimeError(f"Bulk query: {json.dumps(payload['userErrors'], ensure_ascii=False)}")
    op_id = (payload.get("bulkOperation") or {}).get("id")
    if not op_id:
        raise RuntimeError("bulkOperationRunQuery non ha restituito l'ID dell'operazione")
    return op_id


def scan_shopify() -> dict[str, list[dict[str, Any]]]:
    op = wait_operation("lettura catalogo", submit_bulk_query())
    candidates: dict[str, list[dict[str, Any]]] = collections.defaultdict(list)
    count = 0
    for data in sync.iter_jsonl_from_url(op["url"]):
        if data.get("__parentId"):
            # Non sono previste connessioni annidate nel risultato di questa query.
            raise RuntimeError("Risultato Shopify con parentId inatteso: schema non riconosciuto")
        sku = str(data.get("sku") or "").strip()
        if not sku or sku not in ROWS:
            continue
        if not data.get("id") or not (data.get("product") or {}).get("id") or not (data.get("inventoryItem") or {}).get("id"):
            raise RuntimeError(f"Dati Shopify incompleti per SKU {sku}: {data}")
        candidates[sku].append(data)
        count += 1
    LOG.info("[bulk] Catalogo letto: %s varianti riferite agli EAN selezionati", count)
    return candidates


def same_identity(old: dict[str, Any], new: dict[str, Any]) -> bool:
    """Mai cambiare oggetto Shopify a parita' di SKU durante una migrazione."""
    return (old.get("id") == new.get("id") and
            (old.get("product") or {}).get("id") == (new.get("product") or {}).get("id") and
            (old.get("inventoryItem") or {}).get("id") == (new.get("inventoryItem") or {}).get("id"))


def stock(variant: dict[str, Any]) -> int | None:
    level = ((variant.get("inventoryItem") or {}).get("inventoryLevel"))
    if level is None:
        return None
    for item in level.get("quantities") or []:
        if item.get("name") == "available":
            return int(item["quantity"])
    return None


def load_rows(ean: str | None, limit: int | None) -> dict[str, dict]:
    all_rows = sync.read_testi_records()
    latest: dict[str, dict] = {}
    dup: set[str] = set()
    for row in all_rows:
        key = sync.clean_str(sync.get_ci(row, "CODICE_EAN", default=""))
        if key in latest:
            dup.add(key)
        latest[key] = row
    rows = {key: row for key, row in latest.items() if sync.is_scolastico(row)}
    if ean:
        rows = {key: row for key, row in rows.items() if key == ean}
        if not rows:
            raise RuntimeError(f"EAN non presente in SCOLASTICO: {ean}\n" + sync.diagnose_ean_dbf(ean))
    if dup.intersection(rows):
        raise RuntimeError("EAN duplicati nel DBF, migrazione bloccata: " + ", ".join(sorted(dup.intersection(rows))[:20]))
    rows = dict(sorted(rows.items()))
    if limit:
        rows = dict(list(rows.items())[:limit])
    return rows


def build_plan(shop: dict[str, list[dict]], rows: dict[str, dict]) -> tuple[dict[str, dict], dict[str, str]]:
    good: dict[str, dict] = {}
    errors: dict[str, str] = {}
    used_items: dict[str, str] = {}
    for ean, row in rows.items():
        found = shop.get(ean) or []
        if not found:
            errors[ean] = "SKU mancante su Shopify (non creato)"
            continue
        if len(found) != 1:
            errors[ean] = f"SKU ambiguo: {len(found)} varianti Shopify"
            continue
        v = found[0]
        inv_id = v["inventoryItem"]["id"]
        if inv_id in used_items and used_items[inv_id] != ean:
            errors[ean] = f"InventoryItem ID condiviso con EAN {used_items[inv_id]}"
            errors[used_items[inv_id]] = f"InventoryItem ID condiviso con EAN {ean}"
            good.pop(used_items[inv_id], None)
            continue
        used_items[inv_id] = ean
        good[ean] = v
    return good, errors


def stage_input_policy(variants: dict[str, dict]) -> list[tuple[dict, list[str]]]:
    grouped: dict[str, list[tuple[str, str]]] = collections.defaultdict(list)
    for ean, v in variants.items():
        if v.get("inventoryPolicy") != "CONTINUE":
            grouped[v["product"]["id"]].append((ean, v["id"]))
    return [({"productId": pid, "variants": [{"id": vid, "inventoryPolicy": "CONTINUE"} for _, vid in vv]},
             [ean for ean, _ in vv]) for pid, vv in sorted(grouped.items())]


def stage_input_track(variants: dict[str, dict]) -> list[tuple[dict, list[str]]]:
    return [({"id": v["inventoryItem"]["id"], "input": {"tracked": True}}, [ean])
            for ean, v in variants.items() if v["inventoryItem"].get("tracked") is not True]


def stage_input_activate(variants: dict[str, dict], run_key: str) -> list[tuple[dict, list[str]]]:
    return [({"inventoryItemId": v["inventoryItem"]["id"],
              "locationId": sync.SHOPIFY_LOCATION_ID,
              "key": str(uuid.uuid5(uuid.NAMESPACE_URL, f"scolastico-bulk-activate|{run_key}|{ean}|{sync.SHOPIFY_LOCATION_ID}"))}, [ean])
            for ean, v in variants.items() if (v["inventoryItem"].get("inventoryLevel") is None)]


def parse_bulk_result(data: dict[str, Any], *, root: str, keys: list[str], variables: dict) -> str | None:
    if data.get("errors"):
        return "GraphQL errors: " + json.dumps(data["errors"], ensure_ascii=False)[:500]
    value = (data.get("data") or {}).get(root)
    if not isinstance(value, dict):
        return "Payload mancante: " + root
    if value.get("userErrors"):
        return "userErrors: " + json.dumps(value["userErrors"], ensure_ascii=False)[:500]
    if root == "productVariantsBulkUpdate":
        expected = {item["id"] for item in variables["variants"]}
        actual = {v.get("id") for v in (value.get("productVariants") or []) if v.get("inventoryPolicy") == "CONTINUE"}
        if not expected.issubset(actual):
            return f"policy CONTINUE non confermata per {sorted(expected - actual)}"
    if root == "inventoryItemUpdate":
        item = value.get("inventoryItem") or {}
        if item.get("id") != variables["id"] or item.get("tracked") is not True:
            return "tracked=True non confermato dalla mutazione"
    if root == "inventoryActivate":
        # Controlliamo il risultato effettivo prima delle quantita', via scan bulk.
        if not (value.get("inventoryLevel") or {}).get("id"):
            return "inventoryLevel non confermato dalla mutazione"
    return None


def exec_stage(stage: str, mutation: str, root: str, rows: list[tuple[dict, list[str]]],
               blocked: dict[str, str], chunk_size: int) -> None:
    rows = [(params, keys) for params, keys in rows if not any(k in blocked for k in keys)]
    LOG.info("[bulk] Fase %s: %s righe, chunk=%s", stage, len(rows), chunk_size)
    for start in range(0, len(rows), chunk_size):
        batch = rows[start:start + chunk_size]
        jsonl = b"".join((json.dumps(item[0], separators=(",", ":"), ensure_ascii=False) + "\n").encode("utf-8") for item in batch)
        staged = sync.staged_upload_jsonl(ENDPOINT, sync.SHOPIFY_ACCESS_TOKEN,
                                          filename=f"scolastico_{stage}_{start}.jsonl", jsonl_bytes=jsonl)
        job_id = sync.bulk_run_mutation(ENDPOINT, sync.SHOPIFY_ACCESS_TOKEN,
                                        mutation_str=mutation, staged_upload_path=staged,
                                        client_identifier=f"scolastico-{stage}-{start}-{uuid.uuid4().hex[:8]}")
        track_pending(stage, job_id)
        op = wait_operation(stage, job_id)
        result_by_line: dict[int, dict] = {}
        for result in sync.iter_jsonl_from_url(op["url"]):
            index = result.get("__lineNumber")
            if type(index) is not int or index < 0 or index >= len(batch) or index in result_by_line:
                raise RuntimeError(f"Fase {stage}: numero riga bulk non valido/duplicato: {index!r}")
            result_by_line[index] = result
        for index, (variables, keys) in enumerate(batch):
            data = result_by_line.get(index)
            reason = ("Risposta bulk per riga assente" if data is None else
                      parse_bulk_result(data, root=root, keys=keys, variables=variables))
            if reason:
                for key in keys:
                    blocked[key] = f"{stage}: {reason}"
                LOG.error("[bulk] Fase %s riga %d EAN %s: %s", stage, start + index, keys, reason)
        PENDING.unlink(missing_ok=True)
        LOG.info("[bulk] Fase %s completata chunk %d-%d/%d | bloccati=%d",
                 stage, start + 1, start + len(batch), len(rows), len(blocked))


def set_qty_batches(variants: dict[str, dict], blocked: dict[str, str],
                    qty_batch: int, run_key: str) -> None:
    entries: list[tuple[str, str, int]] = []
    for ean, v in variants.items():
        if ean in blocked:
            continue
        need = sync.get_row_stock_quantity(ROWS[ean])
        if stock(v) != need:
            entries.append((ean, v["inventoryItem"]["id"], need))
    LOG.info("[bulk] Giacenze da correggere: %d (batch=%d)", len(entries), qty_batch)

    def submit(items: list[tuple[str, str, int]]) -> None:
        if not items:
            return
        ref = f"gid://erp-connector/ScolasticoBulk/{run_key}/" + hashlib.sha256(
            ("|".join(f"{ean}:{qty}" for ean, _, qty in items)).encode()).hexdigest()[:20]
        # Su errori di rete/autenticazione si interrompe invece di scatenare
        # migliaia di retry; la quantita' e' assoluta e il run si puo' ripetere.
        result = sync.inventory_set_available_batch(ENDPOINT, [(inv, qty) for _, inv, qty in items],
                                                    reference_document_uri=ref)
        errs = sync.inventory_errors(result)
        if errs:
            if len(items) > 1:
                mid = len(items) // 2
                submit(items[:mid])
                submit(items[mid:])
            else:
                ean = items[0][0]
                blocked[ean] = "inventorySetQuantities: " + json.dumps(errs, ensure_ascii=False)[:500]
                LOG.error("[bulk] Errore giacenza EAN=%s: %s", ean, errs)

    for i in range(0, len(entries), qty_batch):
        submit(entries[i:i + qty_batch])
        LOG.info("[bulk] Giacenze %d/%d trattate", min(i + qty_batch, len(entries)), len(entries))


def verify_and_update_state(variants: dict[str, dict], shop: dict[str, list[dict]],
                            blocked: dict[str, str], run_id: int, state: Any,
                            report: Path) -> tuple[int, int]:
    verified = []
    output = []
    for ean, row in ROWS.items():
        target = variants.get(ean)
        after = shop.get(ean) or []
        qty = sync.get_row_stock_quantity(row)
        reason = blocked.get(ean, "")
        if not reason:
            if target is None or len(after) != 1:
                reason = "Variante non presente oppure SKU ambiguo nella verifica finale"
            else:
                v = after[0]
                inv = v.get("inventoryItem") or {}
                diff = {}
                for key, actual, expected in [
                    ("variant_id", v.get("id"), target["id"]),
                    ("product_id", (v.get("product") or {}).get("id"), target["product"]["id"]),
                    ("inventory_item_id", inv.get("id"), target["inventoryItem"]["id"]),
                    ("policy", v.get("inventoryPolicy"), "CONTINUE"),
                    ("tracked", inv.get("tracked"), True),
                    ("qty", stock(v), qty),
                ]:
                    if actual != expected:
                        diff[key] = {"atteso": expected, "shopify": actual}
                if diff:
                    reason = json.dumps(diff, ensure_ascii=False)
        if reason:
            state.record_issue(ean, "migrazione bulk SCOLASTICO: " + reason)
        else:
            v = after[0]
            verified.append((run_id, ean, sync.compute_row_hash(row),
                             v["product"]["id"], v["id"], v["inventoryItem"]["id"]))
        current = after[0] if len(after) == 1 else {}
        output.append({"ean": ean, "stato": "OK" if not reason else "ERRORE",
                       "giacenza_dbf": qty, "giacenza_shopify": stock(current),
                       "tracked": (current.get("inventoryItem") or {}).get("tracked"),
                       "inventory_policy": current.get("inventoryPolicy"),
                       "motivo": reason})
    # L'hash non viene avanzato per gli articoli non verificati.
    for chunk in sync.chunked(verified, 1000):
        state.upsert_items_many(chunk)
    for _, ean, _, _, _, item_id in verified:
        state.mark_link_verified(ean, item_id)
        state.clear_issue(ean)
    with report.open("w", encoding="utf-8-sig", newline="") as output_file:
        writer = csv.DictWriter(output_file, fieldnames=["ean", "stato", "giacenza_dbf", "giacenza_shopify", "tracked", "inventory_policy", "motivo"], delimiter=";")
        writer.writeheader()
        writer.writerows(output)
    return len(verified), len(output) - len(verified)


def main() -> None:
    args = parse_args()
    if not sync.SHOPIFY_SHOP or not sync.SHOPIFY_ACCESS_TOKEN or not sync.SHOPIFY_LOCATION_ID:
        raise RuntimeError("Shopify: SHOPIFY_SHOP, SHOPIFY_ACCESS_TOKEN, SHOPIFY_LOCATION_ID obbligatori in config.env")
    if sync.DRY_RUN and args.apply:
        raise RuntimeError("DRY_RUN=1 in config.env: --apply non permesso")
    if sync.LIMIT_RECORDS:
        raise RuntimeError("LIMIT_RECORDS deve essere 0 per la migrazione")
    if not Path(sync.STATE_DB_PATH).exists():
        raise RuntimeError("Database SQLite del sync mancante: " + sync.STATE_DB_PATH)
    global ENDPOINT, ROWS
    ENDPOINT = f"https://{sync.SHOPIFY_SHOP}/admin/api/{sync.SHOPIFY_API_VERSION}/graphql.json"
    ROWS = load_rows(args.ean, args.limit)
    LOG.info("[bulk] Inizio migrazione SCOLASTICO. Modalita=%s | EAN DBF=%d | location=%s",
             "APPLY" if args.apply else "ANTEPRIMA", len(ROWS), sync.SHOPIFY_LOCATION_ID)
    recover_pending()
    initial = scan_shopify()
    found, blocked = build_plan(initial, ROWS)
    n_policy = sum(v.get("inventoryPolicy") != "CONTINUE" for v in found.values())
    n_track = sum(v["inventoryItem"].get("tracked") is not True for v in found.values())
    n_activate = sum(v["inventoryItem"].get("inventoryLevel") is None for v in found.values())
    n_qty = sum(stock(v) != sync.get_row_stock_quantity(ROWS[e]) for e, v in found.items())
    LOG.info("[bulk] Piano: esistenti=%d | esclusi=%d | policy da aggiornare=%d | "
             "tracking da attivare=%d | sedi mancanti=%d | giacenze divergenti=%d",
             len(found), len(blocked), n_policy, n_track, n_activate, n_qty)
    for ean, reason in list(blocked.items())[:20]:
        LOG.warning("[bulk] Escluso EAN %s: %s", ean, reason)
    if not args.apply:
        LOG.info("[bulk] ANTEPRIMA terminata. Nessuna modifica a Shopify/SQLite. Usa --apply per procedere.")
        return

    state = sync.SyncState(sync.STATE_DB_PATH)
    report = DIR / ("scolastico_bulk_esito_" + dt.datetime.now().strftime("%Y%m%d_%H%M%S") + ".csv")
    try:
        backup = Path(sync.STATE_DB_PATH).with_name("shopify_sync_state_pre_bulk_scolastico_" +
                 dt.datetime.now().strftime("%Y%m%d_%H%M%S") + ".sqlite")
        with sqlite3.connect(str(backup)) as dest:
            state.conn.backup(dest)
        LOG.info("[bulk] Backup SQLite: %s", backup)
        run_id = state.start_run()
        run_key = f"{run_id}-{uuid.uuid4().hex[:8]}"
        try:
            exec_stage("continue", MUT_POLICY, "productVariantsBulkUpdate", stage_input_policy(found), blocked, args.chunk_size)
            exec_stage("tracked", MUT_TRACK, "inventoryItemUpdate",
                       stage_input_track({k: v for k, v in found.items() if k not in blocked}), blocked, args.chunk_size)
            # Dopo il tracking Shopify puo' creare gia' l'inventoryLevel: rileggere.
            middle = scan_shopify()
            ready, mid_errors = build_plan(middle, ROWS)
            for ean, reason in mid_errors.items():
                blocked[ean] = reason
            for ean, v in list(ready.items()):
                if ean in blocked:
                    ready.pop(ean)
                elif (not same_identity(found[ean], v) or
                      v.get("inventoryPolicy") != "CONTINUE" or v["inventoryItem"].get("tracked") is not True):
                    blocked[ean] = "Dopo le bulk: identita'/policy/tracking non conformi"
                    ready.pop(ean)
            exec_stage("activation", MUT_ACTIVATE, "inventoryActivate",
                       stage_input_activate(ready, run_key), blocked, args.chunk_size)
            # Evita di scrivere su inventoryLevel che ancora non esiste:
            before_qty = scan_shopify()
            ready_qty, qty_errors = build_plan(before_qty, ROWS)
            for ean, reason in qty_errors.items():
                blocked[ean] = reason
            for ean, v in list(ready_qty.items()):
                if ean in blocked:
                    ready_qty.pop(ean)
                elif (not same_identity(found[ean], v) or
                      v.get("inventoryPolicy") != "CONTINUE" or
                      v["inventoryItem"].get("tracked") is not True or
                      v["inventoryItem"].get("inventoryLevel") is None):
                    blocked[ean] = "Prima delle giacenze policy/tracked/sede non conformi"
                    ready_qty.pop(ean)
            set_qty_batches(ready_qty, blocked, args.qty_batch, run_key)
            after = scan_shopify()
            ok, fail = verify_and_update_state(found, after, blocked, run_id, state, report)
            LOG.info("[bulk] FINE: verificati=%d | errori/esclusi=%d | report=%s", ok, fail, report)
            if fail:
                raise RuntimeError(f"Migrazione incompleta: {fail} EAN non verificati; vedere {report}")
        finally:
            state.finish_run(run_id)
    finally:
        state.conn.close()


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Migrazione bulk SCOLASTICO (usa lo script di sync esistente come libreria)")
    parser.add_argument("--apply", action="store_true", help="Abilita le scritture: senza il flag esegue solo anteprima")
    parser.add_argument("--ean", help="Limita a un EAN per collaudo")
    parser.add_argument("--limit", type=int, help="Prime N righe SCOLASTICO in ordine EAN")
    parser.add_argument("--chunk-size", type=int, default=1500, help="Righe per bulk mutation Shopify (default 1500)")
    parser.add_argument("--qty-batch", type=int, default=100, help="Quantita' per singola inventorySetQuantities (default 100)")
    args = parser.parse_args()
    if args.limit is not None and args.limit < 1:
        parser.error("--limit deve essere positivo")
    if not 1 <= args.chunk_size <= 5000:
        parser.error("--chunk-size deve essere tra 1 e 5000")
    if not 1 <= args.qty_batch <= 200:
        parser.error("--qty-batch deve essere tra 1 e 200")
    return args


if __name__ == "__main__":
    lock = sync.try_acquire_single_instance_lock()
    if lock is None:
        raise SystemExit("Un'altra istanza Shopify sync/migrazione è ancora attiva. Disabilitare prima il Task Scheduler e fermare la migrazione sequenziale.")
    try:
        main()
    except Exception:
        LOG.exception("[bulk] ERRORE FATALE")
        raise
    finally:
        lock.close()
