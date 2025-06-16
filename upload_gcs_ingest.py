#!/usr/bin/env python3
"""
upload_gcs_ingest.py — v4.0
──────────────────────────
• Carica su GCS tutti i file che corrispondono ai pattern forniti.
• Crea/aggiorna 'ingest/metadata.jsonl' (JSON Lines).
• Esegue un backup timestamped del metadata.jsonl esistente prima dell'aggiornamento.
"""
from __future__ import annotations
import argparse, hashlib, json, sys, tempfile, pathlib, datetime as _dt
from typing import Dict, List, Set
from google.cloud import storage
from google.api_core import exceptions
from tqdm import tqdm


CREDENTIALS_FILE = "GOOGLE_CREDENTIALS.json" # <--- NUOVA RIGA

# ...funzioni sha1, get_relative_path...
def sha1(path: pathlib.Path, buf_size: int = 65_536) -> str:
    h = hashlib.sha1()
    with open(path, "rb") as f:
        while chunk := f.read(buf_size): h.update(chunk)
    return h.hexdigest()
def get_relative_path(path: pathlib.Path, base: pathlib.Path) -> str:
    return str(path.relative_to(base)).replace("\\", "/")


def upload_directory(src: pathlib.Path, bucket_name: str, prefix: str, patterns: List[str], refresh: bool):
    try:
        # ---- MODIFICA QUI ----
        client = storage.Client.from_service_account_json(CREDENTIALS_FILE)
        # ----------------------
        bucket = client.bucket(bucket_name)
    except Exception as e:
        sys.exit(f"❌ Impossibile connettersi a GCS (file: {CREDENTIALS_FILE}). Errore: {e}")
    
    # (Il resto del file rimane identico)
    if not src.is_dir():
        print(f"ℹ️ La cartella sorgente '{src}' non esiste o è vuota. Processo terminato per questa fonte.")
        return
    if refresh:
        print(f"🗑️ Cancellando il prefisso 'gs://{bucket_name}/{prefix}/'...")
        blobs_to_delete = list(client.list_blobs(bucket_name, prefix=prefix))
        if blobs_to_delete:
            for blob in tqdm(blobs_to_delete, desc="Pulizia GCS", unit="blob"): blob.delete()
        else:
            print("  - Prefisso già vuoto.")
    all_files: Set[pathlib.Path] = set()
    for pattern in patterns:
        all_files.update(src.rglob(pattern))
    if not all_files:
        print(f"ℹ️ Nessun file corrispondente ai pattern {patterns} in '{src}'. Processo terminato.")
        return
    print(f"Trovati {len(all_files)} file da processare.")
    jsonl_records: List[Dict] = []
    data_files = [f for f in all_files if f.suffix.lower() != ".json"]
    for data_file in tqdm(data_files, desc="1/2 - Upload e metadati", unit="file"):
        relative_path = get_relative_path(data_file, src)
        gcs_path = f"{prefix}/{relative_path}".lstrip("/")
        blob = bucket.blob(gcs_path)
        blob.upload_from_filename(str(data_file))
        sidecar_path = data_file.with_suffix(".json")
        metadata: Dict = {}
        if sidecar_path.exists():
            try:
                with open(sidecar_path, "r", encoding="utf-8") as f: metadata = json.load(f)
            except json.JSONDecodeError: print(f"⚠️ JSON corrotto, lo salto: {sidecar_path}")
        record = { "source_file_gcs_uri": f"gs://{bucket_name}/{gcs_path}", "relative_path": relative_path, "sha1_hash": sha1(data_file), "upload_timestamp_utc": _dt.datetime.now(_dt.timezone.utc).isoformat(), **metadata, }
        jsonl_records.append(record)
    if not jsonl_records:
        print("✅ Nessun nuovo file di dati trovato, metadata.jsonl non modificato.")
        return
    print("\n2/2 - Aggiornamento di metadata.jsonl...")
    metadata_blob_name = f"{prefix}/ingest/metadata.jsonl".lstrip("/")
    metadata_blob = bucket.blob(metadata_blob_name)
    try:
        if metadata_blob.exists():
            timestamp = _dt.datetime.now(_dt.timezone.utc).strftime("%Y%m%dT%H%M%S")
            backup_blob_name = f"{metadata_blob_name}.{timestamp}.bak"
            bucket.copy_blob(metadata_blob, bucket, new_name=backup_blob_name)
            print(f"  - Backup di metadata.jsonl creato: '{backup_blob_name}'")
    except exceptions.NotFound:
        print("  - Nessun metadata.jsonl esistente. Ne verrà creato uno nuovo.")
    with tempfile.NamedTemporaryFile("w+", delete=False, encoding="utf-8", suffix=".jsonl") as tmp:
        for record in jsonl_records: tmp.write(json.dumps(record, ensure_ascii=False) + "\n")
        tmp_path = pathlib.Path(tmp.name)
    metadata_blob.upload_from_filename(str(tmp_path), content_type="application/json")
    tmp_path.unlink()
    print(f"✅ {len(data_files)} file caricati.")
    print(f"✅ metadata.jsonl aggiornato in gs://{bucket_name}/{metadata_blob_name}")
def main(argv=None):
    parser = argparse.ArgumentParser()
    parser.add_argument("--src", required=True, type=pathlib.Path)
    parser.add_argument("--bucket", required=True)
    parser.add_argument("--prefix", default="")
    parser.add_argument("--patterns", required=True, help="Pattern dei file, separati da virgola")
    parser.add_argument("--refresh", action="store_true")
    args = parser.parse_args(argv)
    patterns_list = [p.strip() for p in args.patterns.split(',')]
    upload_directory(args.src, args.bucket, args.prefix, patterns_list, args.refresh)
if __name__ == "__main__":
    main()