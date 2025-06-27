"""
rename_gcs_vertex_flash.py — v8.2 (Fix ADC credentials + Batch JSONL Update)
============================================================================
Rinomina file in GCS sfruttando Gemini Flash via Vertex AI (GA).

CHANGELOG v8.2 – 2025‑06‑23
---------------------------
* **NUOVO**: Aggiornamento automatico del batch.jsonl dopo rename
* Dopo aver rinominato i file, aggiorna il batch.jsonl con:
  - URI corretti dei file rinominati
  - Date corrette estratte dai nuovi nomi file
* I JSON metadata individuali sono rinominati ma non modificati (inutili dopo batch.jsonl)
* Backup automatico del batch.jsonl prima dell'aggiornamento
* Mantiene tutte le funzionalità precedenti
"""

from __future__ import annotations
import os, re, csv, json, time, argparse, traceback, sys, datetime as dt
from pathlib import Path
from concurrent.futures import ThreadPoolExecutor, as_completed
from typing import Tuple, Final, Optional
from io import BytesIO
from urllib.parse import urlparse

# ───────────────────────────── CONFIG
LOG_FILE: str              = "rename_gcs_log.csv"
WORKERS: int               = 16
MAX_PAGES_PDF: int         = 3              # Max pagine PDF da leggere
MAX_CHARS_CONTENT: int     = 4000           # Max caratteri da inviare a Gemini

PROJECT_ID: str            = "progetto-analisi-sentiment"  # <‑- CAMBIA SE NECESSARIO
REGION: str                = "global"       # o "europe-west8" se serve residenza dati
SERVICE_ACCOUNT_FILE: str  = "GOOGLE_CREDENTIALS.json"

MODEL_ID: Final[str]       = "gemini-2.0-flash-lite-001"
SYSTEM_PROMPT: Final[str] = (
    "Sei un archivista esperto. Analizza i primi caratteri del documento fornito (PDF, XML o TXT) e il suo nome originale. "
    "Il documento è un atto ufficiale del Parlamento italiano (Camera o Senato) o un altro documento istituzionale. "
    "Genera un nuovo nome di file nel formato ESATTO: \n\n"
    "<organo>_<tipo_atto>_<data_iso>_<descrizione_o_presidenza>\n\n"
    "- <organo>: 'camera' o 'senato' (o 'doc' se incerto)\n"
    "- <tipo_atto>: es. 'resoconto_stenografico', 'ddl', 'audizione' (o 'atto')\n"
    "- <data_iso>: data AAAA-MM-GG\n"
    "- <descrizione_o_presidenza>: cognome presidente o breve descrizione (≤3 parole)\n\n"
    "RISPONDI SOLO COL NOME, senza testo extra."
)
# ───────────────────────────── END CONFIG

# Import lazy
try:
    import pdfplumber
    from google.oauth2 import service_account
    from google.auth.transport.requests import Request as GoogleRequest
    from google.cloud import storage
    from google.cloud.storage.blob import Blob
    import requests
    from tqdm import tqdm
except ImportError as e:
    sys.exit(f"❌ Libreria mancante: {e}. Installa con: pip install google-cloud-storage pdfplumber google-auth requests tqdm")

# ───────────────────────────── Helpers

def fetch_access_token() -> str:
    """Recupera un access token usando il file di servizio."""
    try:
        creds = service_account.Credentials.from_service_account_file(
            SERVICE_ACCOUNT_FILE,
            scopes=["https://www.googleapis.com/auth/cloud-platform"],
        )
        creds.refresh(GoogleRequest())
        return creds.token
    except Exception as e:
        raise RuntimeError(
            f"Token non ottenibile da '{SERVICE_ACCOUNT_FILE}'. Controlla il file o esegui 'gcloud auth application-default login'. Errore: {e}"
        )


def extract_text_from_gcs_blob(blob: Blob) -> str:
    """Estrae testo (prime pagine o primi KB) dal blob per dare contesto al modello."""
    try:
        data = blob.download_as_bytes()
        name_low = blob.name.lower()

        if name_low.endswith(".pdf"):
            with BytesIO(data) as stream, pdfplumber.open(stream) as pdf:
                pages = [p.extract_text() or "" for p in pdf.pages[:MAX_PAGES_PDF]]
            content = "\n".join(pages)
        elif name_low.endswith((".txt", ".xml", ".html", ".md")):
            content = data.decode("utf-8", errors="ignore")
        else:
            return ""

        full = f"NOME FILE ORIGINALE: {Path(blob.name).name}\n\nCONTENUTO:\n{content}"
        return full[:MAX_CHARS_CONTENT]
    except Exception:
        return ""


def call_model_api(content: str, token: str, retries: int = 3) -> str:
    """Invoca Vertex AI (GA) con Gemini Flash."""
    url = (
        f"https://aiplatform.googleapis.com/v1/projects/{PROJECT_ID}/locations/{REGION}/"
        f"publishers/google/models/{MODEL_ID}:generateContent"
    )
    headers = {"Authorization": f"Bearer {token}", "Content-Type": "application/json"}
    body = {
        "systemInstruction": {"parts": [{"text": SYSTEM_PROMPT}]},
        "contents": [{"role": "user", "parts": [{"text": content}]}],
        "generationConfig": {"temperature": 0.0, "maxOutputTokens": 256},
    }

    for i in range(retries):
        try:
            r = requests.post(url, headers=headers, json=body, timeout=60)
            r.raise_for_status()
            parts = r.json()["candidates"][0]["content"]["parts"]
            return parts[0]["text"].strip()
        except Exception:
            if i == retries - 1:
                raise
            time.sleep(2 ** i)
    return ""

# ───────────────────────────── Date Extraction Utils

def extract_date_from_filename(filename: str) -> Optional[str]:
    """Estrae data ISO (YYYY-MM-DD) da un nome file rinominato"""
    # Pattern per data ISO nel formato del rename
    patterns = [
        r'_(\d{4}-\d{2}-\d{2})_',      # _2024-03-15_
        r'_(\d{4}-\d{2}-\d{2})\.',     # _2024-03-15.pdf
        r'(\d{4}-\d{2}-\d{2})_',       # 2024-03-15_
        r'(\d{4}-\d{2}-\d{2})\.'       # 2024-03-15.pdf
    ]
    
    for pattern in patterns:
        match = re.search(pattern, filename)
        if match:
            date_str = match.group(1)
            # Valida che sia una data ISO corretta
            try:
                dt.datetime.strptime(date_str, "%Y-%m-%d")
                return date_str
            except ValueError:
                continue
    
    return None

# ───────────────────────────── Utils

def sanitize(stem: str) -> str:
    s = re.sub(r"\s+", "_", stem.lower())
    s = re.sub(r"[^a-z0-9._-]", "", s)
    return s.strip("_.") or "documento_rinominato"


def split_blob_path(name: str) -> Tuple[str, str, str]:
    parts = name.split("/")
    prefix = "/".join(parts[:-1]) + "/" if len(parts) > 1 else ""
    stem, _, ext = parts[-1].rpartition(".")
    return prefix, stem, f".{ext}" if ext else ""


def list_target_blobs(client: storage.Client, bucket: str, prefix: str):
    return [b for b in client.list_blobs(bucket, prefix=prefix) if not b.name.lower().endswith((".json", ".jsonl"))]


def process_blob(blob: Blob, client: storage.Client, bucket: str, token: str):
    """Processa un blob: rinomina file e JSON associato (senza modificare contenuto JSON)"""
    origin_uri = f"gs://{bucket}/{blob.name}"
    text = extract_text_from_gcs_blob(blob)
    if not text:
        return origin_uri, "NO_TEXT"

    try:
        new_stem = sanitize(call_model_api(text, token))
        if not new_stem:
            return origin_uri, "EMPTY_RESPONSE"

        prefix, _, ext = split_blob_path(blob.name)
        new_key = f"{prefix}{new_stem}{ext}"
        if new_key == blob.name:
            return origin_uri, "UNCHANGED"

        bucket_ref = client.bucket(bucket)
        
        # 1. Copia il file principale rinominato
        bucket_ref.copy_blob(blob, bucket_ref, new_key)
        
        # 2. Rinomina anche il JSON associato se esiste (senza modificarlo)
        original_json_path = f"{prefix}{Path(blob.name).stem}.json"
        new_json_path = f"{prefix}{new_stem}.json"
        
        try:
            original_json_blob = bucket_ref.blob(original_json_path)
            if original_json_blob.exists():
                bucket_ref.copy_blob(original_json_blob, bucket_ref, new_json_path)
                original_json_blob.delete()  # Rimuovi JSON originale
        except Exception as e:
            print(f"    ⚠️  Errore rinomina JSON: {e}")
        
        # 3. Rimuovi il file originale
        blob.delete()
        
        return origin_uri, f"gs://{bucket}/{new_key}"
        
    except Exception as e:
        return origin_uri, f"ERROR:{type(e).__name__}:{e}"

# ───────────────────────────── Batch JSONL Update

def update_batch_jsonl(client: storage.Client, bucket_name: str, prefix: str, changes_map: dict) -> bool:
    """Aggiorna il batch.jsonl con i nuovi URI e date dopo il rename"""
    if not changes_map:
        print("📝 Nessun cambiamento da applicare al batch.jsonl")
        return True
    
    batch_blob_path = f"{prefix}ingest/batch.jsonl".lstrip("/")
    print(f"\n📋 Aggiornamento batch.jsonl: gs://{bucket_name}/{batch_blob_path}")
    
    try:
        bucket = client.bucket(bucket_name)
        batch_blob = bucket.blob(batch_blob_path)
        
        if not batch_blob.exists():
            print("⚠️  batch.jsonl non trovato - skip aggiornamento")
            return True
        
        # Scarica il batch.jsonl attuale
        print("  ⬇️  Scaricando batch.jsonl...")
        jsonl_content = batch_blob.download_as_text()
        
        # Processa ogni linea del JSONL
        updated_lines = []
        updates_count = 0
        
        for line_num, line in enumerate(jsonl_content.strip().split('\n'), 1):
            if not line.strip():
                continue
                
            try:
                record = json.loads(line)
                original_uri = record.get("content", {}).get("uri", "")
                
                # Controlla se questo URI è stato rinominato
                if original_uri in changes_map:
                    new_uri, new_date = changes_map[original_uri]
                    
                    # Aggiorna l'URI
                    record["content"]["uri"] = new_uri
                    
                    # Aggiorna la data se presente
                    if new_date and "structData" in record:
                        old_date = record["structData"].get("date")
                        record["structData"]["date"] = new_date
                        record["structData"]["date_corrected_by_rename"] = True
                        record["structData"]["date_correction_timestamp"] = dt.datetime.now(dt.timezone.utc).isoformat()
                        
                        if old_date != new_date:
                            print(f"    📅 Record {line_num}: {old_date} → {new_date}")
                    
                    updates_count += 1
                    print(f"    🔄 Record {line_num}: URI aggiornato")
                
                updated_lines.append(json.dumps(record, ensure_ascii=False))
                
            except (json.JSONDecodeError, KeyError) as e:
                print(f"    ⚠️  Errore parsing record {line_num}: {e}")
                updated_lines.append(line)  # Mantieni la linea originale
        
        # Ricarica il batch.jsonl aggiornato
        if updates_count > 0:
            print(f"  ⬆️  Caricando batch.jsonl aggiornato ({updates_count} record modificati)...")
            updated_jsonl = '\n'.join(updated_lines) + '\n'
            
            # Backup del vecchio batch.jsonl
            timestamp = dt.datetime.now(dt.timezone.utc).strftime("%Y%m%dT%H%M%S")
            backup_path = f"{batch_blob_path}.{timestamp}.bak"
            bucket.copy_blob(batch_blob, bucket, backup_path)
            print(f"    💾 Backup creato: {backup_path}")
            
            # Upload del nuovo batch.jsonl
            batch_blob.upload_from_string(updated_jsonl, content_type="application/json")
            print(f"  ✅ batch.jsonl aggiornato con {updates_count} modifiche")
        else:
            print("  ℹ️  Nessun record nel batch.jsonl richiede aggiornamenti")
        
        return True
        
    except Exception as e:
        print(f"  ❌ Errore aggiornamento batch.jsonl: {e}")
        return False

# ───────────────────────────── Main

def main(gcs_uri: str):
    parsed = urlparse(gcs_uri)
    if parsed.scheme != "gs":
        sys.exit("❌ URI deve iniziare con 'gs://'")

    bucket_name = parsed.netloc
    prefix = parsed.path.lstrip("/")
    if prefix and not prefix.endswith("/"):
        prefix += "/"

    # ➜ Init Storage client: usa service‑account se presente
    if Path(SERVICE_ACCOUNT_FILE).exists():
        storage_client = storage.Client.from_service_account_json(SERVICE_ACCOUNT_FILE)
        print(f"🔑 Storage client con credenziali '{SERVICE_ACCOUNT_FILE}'")
    else:
        storage_client = storage.Client(project=PROJECT_ID)
        print("🔑 Storage client con ADC di sistema")

    print(f"📂 Bucket: {bucket_name}  |  Prefisso: '{prefix or '(root)'}'")

    token = fetch_access_token()
    blobs = list_target_blobs(storage_client, bucket_name, prefix)
    if not blobs:
        sys.exit("✅ Nessun file da rinominare")
    print(f"🔍 File da elaborare: {len(blobs)}")

    # 🆕 Mappa per tracciare i cambiamenti: {vecchio_uri: (nuovo_uri, nuova_data)}
    uri_changes_map = {}
    
    log_path = Path(LOG_FILE)
    with open(log_path, "w", newline="", encoding="utf-8") as fh, ThreadPoolExecutor(max_workers=WORKERS) as pool:
        writer = csv.writer(fh)
        writer.writerow(["vecchio_uri", "nuovo_uri_o_stato"])
        futures = {pool.submit(process_blob, b, storage_client, bucket_name, token): b for b in blobs}
        for fut in tqdm(as_completed(futures), total=len(blobs), desc="Rinomina + Correzione"):
            old_uri, result = fut.result()
            writer.writerow([old_uri, result])
            fh.flush()
            
            # 🆕 Traccia i cambiamenti per aggiornare il batch.jsonl
            if result.startswith("gs://") and result != old_uri:
                # Estrai la data dal nuovo nome file
                new_filename = result.split("/")[-1]
                extracted_date = extract_date_from_filename(new_filename)
                uri_changes_map[old_uri] = (result, extracted_date)

    print(f"\n✅ Log salvato in {log_path.resolve()}")
    
    # 🎯 AGGIORNA SOLO IL BATCH.JSONL (i JSON individuali non ci interessano)
    if uri_changes_map:
        print(f"🔄 Trovati {len(uri_changes_map)} file rinominati")
        success = update_batch_jsonl(storage_client, bucket_name, prefix, uri_changes_map)
        if success:
            print("✅ batch.jsonl aggiornato con successo!")
        else:
            print("⚠️  Errore nell'aggiornamento del batch.jsonl")
    else:
        print("ℹ️  Nessun file rinominato - batch.jsonl non modificato")


if __name__ == "__main__":
    p = argparse.ArgumentParser(description="Rinomina file in GCS con Vertex AI + Correzione Date Metadata")
    p.add_argument("--gcs-input", "-i", required=True, help="URI GCS (es. gs://bucket/prefisso)")
    args = p.parse_args()

    try:
        main(args.gcs_input)
    except Exception as e:
        print(f"\n❌ ERRORE GLOBALE: {e}")
        traceback.print_exc()