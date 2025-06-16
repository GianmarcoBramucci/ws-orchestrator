#!/usr/bin/env python3
"""
upload_gcs_structured.py — v6.0 Compatible Format
===================================================
Upload che crea JSONL nel formato compatibile con il tuo sistema esistente.
"""
from __future__ import annotations
import argparse
import hashlib
import json
import sys
import tempfile
import pathlib
import datetime as dt
from typing import Dict, List, Set

try:
    from google.cloud import storage
    from google.api_core import exceptions
    from tqdm import tqdm
except ImportError as e:
    print(f"ERRORE: Dipendenza mancante: {e}")
    sys.exit(1)

CREDENTIALS_FILE = "GOOGLE_CREDENTIALS.json"

def safe_print(msg: str):
    """Print sicuro per Windows"""
    try:
        print(msg)
    except UnicodeEncodeError:
        ascii_msg = msg.encode('ascii', 'replace').decode('ascii')
        print(ascii_msg)

def create_structured_record(file_path: pathlib.Path, gcs_uri: str, metadata: Dict) -> Dict:
    """Crea un record nel formato strutturato richiesto"""
    
    # Estrae info dal filename o metadata
    filename = file_path.stem
    
    # Determina ID univoco
    if 'legislatura' in metadata and 'seduta' in metadata:
        record_id = f"{metadata.get('source', 'doc')}_leg{metadata['legislatura']}_sed{metadata.get('seduta', '0000')}"
        if 'date' in metadata:
            record_id += f"_{metadata['date']}"
    else:
        # Fallback: usa parte del filename
        record_id = filename.replace('.', '_').replace(' ', '_').lower()
    
    # Determina mimeType
    mime_types = {
        '.pdf': 'application/pdf',
        '.txt': 'text/plain',
        '.xml': 'application/xml',
        '.json': 'application/json',
        '.html': 'text/html'
    }
    mime_type = mime_types.get(file_path.suffix.lower(), 'application/octet-stream')
    
    # Determina sourceType
    source_types = {
        'camera': 'parliamentary_records_camera',
        'senato': 'parliamentary_records_senato'
    }
    source_type = source_types.get(metadata.get('source', 'unknown'), 'parliamentary_records')
    
    # Costruisce title intelligente
    title_parts = []
    if metadata.get('source') == 'camera':
        title_parts.append('Camera dei Deputati')
    elif metadata.get('source') == 'senato':
        title_parts.append('Senato della Repubblica')
    
    if metadata.get('document_type'):
        title_parts.append(metadata['document_type'].replace('_', ' ').title())
    
    if metadata.get('legislatura'):
        title_parts.append(f"Legislatura {metadata['legislatura']}")
        
    if metadata.get('seduta'):
        title_parts.append(f"Seduta {metadata['seduta']}")
        
    if metadata.get('date'):
        title_parts.append(metadata['date'])
    
    title = ' - '.join(title_parts) if title_parts else filename
    
    # Struttura finale
    record = {
        "id": record_id,
        "content": {
            "uri": gcs_uri,
            "mimeType": mime_type
        },
        "structData": {
            "sourceType": source_type,
            "title": title,
            "language": metadata.get('language', 'it'),
            **{k: v for k, v in metadata.items() if k not in ['source', 'document_type', 'language']}
        }
    }
    
    return record

def upload_directory(src: pathlib.Path, bucket_name: str, prefix: str, patterns: List[str], refresh: bool):
    """Upload con formato strutturato"""
    
    safe_print("Inizializzazione client GCS...")
    try:
        if pathlib.Path(CREDENTIALS_FILE).exists():
            client = storage.Client.from_service_account_json(CREDENTIALS_FILE)
        else:
            client = storage.Client()
        bucket = client.bucket(bucket_name)
    except Exception as e:
        safe_print(f"ERRORE GCS: {e}")
        sys.exit(1)
    
    if not src.is_dir():
        safe_print(f"Cartella '{src}' non esiste")
        return
    
    # Refresh se richiesto
    if refresh:
        safe_print("PULIZIA bucket...")
        blobs_to_delete = list(client.list_blobs(bucket_name, prefix=prefix))
        for blob in tqdm(blobs_to_delete, desc="Pulizia"):
            blob.delete()
    
    # Trova tutti i file
    all_files: Set[pathlib.Path] = set()
    for pattern in patterns:
        all_files.update(src.rglob(pattern))
    
    data_files = [f for f in all_files if f.suffix.lower() != ".json"]
    
    if not data_files:
        safe_print("Nessun file trovato")
        return
    
    safe_print(f"Upload di {len(data_files)} file...")
    
    jsonl_records: List[Dict] = []
    
    for data_file in tqdm(data_files, desc="Upload"):
        relative_path = str(data_file.relative_to(src)).replace("\\", "/")
        gcs_path = f"{prefix}/{relative_path}".lstrip("/")
        gcs_uri = f"gs://{bucket_name}/{gcs_path}"
        
        # Upload file
        blob = bucket.blob(gcs_path)
        try:
            blob.upload_from_filename(str(data_file))
        except Exception as e:
            safe_print(f"ERRORE upload {data_file.name}: {e}")
            continue
        
        # Leggi metadata sidecar
        sidecar_path = data_file.with_suffix(".json")
        metadata: Dict = {}
        if sidecar_path.exists():
            try:
                with open(sidecar_path, "r", encoding="utf-8") as f:
                    metadata = json.load(f)
            except:
                pass
        
        # Crea record strutturato
        record = create_structured_record(data_file, gcs_uri, metadata)
        jsonl_records.append(record)
    
    # Salva metadata.jsonl
    safe_print("Creazione metadata.jsonl...")
    metadata_blob_name = f"{prefix}/ingest/metadata.jsonl".lstrip("/")
    metadata_blob = bucket.blob(metadata_blob_name)
    
    # Backup se esiste
    if metadata_blob.exists():
        timestamp = dt.datetime.now(dt.timezone.utc).strftime("%Y%m%dT%H%M%S")
        backup_name = f"{metadata_blob_name}.{timestamp}.bak"
        bucket.copy_blob(metadata_blob, bucket, new_name=backup_name)
        safe_print(f"Backup: {backup_name}")
    
    # Scrivi nuovo file
    with tempfile.NamedTemporaryFile("w+", delete=False, encoding="utf-8") as tmp:
        for record in jsonl_records:
            tmp.write(json.dumps(record, ensure_ascii=False) + "\n")
        tmp_path = pathlib.Path(tmp.name)
    
    try:
        metadata_blob.upload_from_filename(str(tmp_path), content_type="application/json")
        safe_print(f"SUCCESS: metadata.jsonl in gs://{bucket_name}/{metadata_blob_name}")
    finally:
        tmp_path.unlink()
    
    safe_print(f"COMPLETATO: {len(data_files)} file caricati")

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--src", required=True, type=pathlib.Path)
    parser.add_argument("--bucket", required=True)
    parser.add_argument("--prefix", default="")
    parser.add_argument("--patterns", required=True)
    parser.add_argument("--refresh", action="store_true")
    
    args = parser.parse_args()
    patterns_list = [p.strip() for p in args.patterns.split(',')]
    
    upload_directory(args.src, args.bucket, args.prefix, patterns_list, args.refresh)

if __name__ == "__main__":
    main()