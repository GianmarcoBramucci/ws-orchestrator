#!/usr/bin/env python3
"""
rename_gcs_vertex_flash.py — v7.1 (Fixed Import Error)
===========================================================
Rinomina file (PDF, TXT, XML, etc.) in GCS basandosi sul loro contenuto,
utilizzando un modello Gemini in Vertex AI.

È progettato per essere "agnostico" al tipo di file. Aggiungere il
supporto per nuovi formati richiede solo di estendere la funzione
`extract_text_from_gcs_blob`.
"""
from __future__ import annotations
import os, re, csv, json, time, argparse, traceback, sys
from pathlib import Path
from concurrent.futures import ThreadPoolExecutor, as_completed
from typing import List, Dict, Set, Tuple, Optional, Final
from io import BytesIO
from urllib.parse import urlparse

# ───────────────────────────────────────────────────────────── CONFIG
LOG_FILE: str               = "rename_gcs_log.csv"
WORKERS: int                = 16
MAX_PAGES_PDF: int          = 3                 # Max pagine PDF da leggere
MAX_CHARS_CONTENT: int      = 4000              # Max caratteri da inviare a Gemini

PROJECT_ID: str             = "progetto-analisi-sentiment" # <--- CAMBIARE
REGION: str                 = "global"
SERVICE_ACCOUNT_FILE: str   = "GOOGLE_CREDENTIALS.json"

MODEL_ID: Final[str]        = "publishers/google/models/gemini-2.0-flash-lite"
SYSTEM_PROMPT: Final[str] = (
    "Sei un archivista esperto. Analizza i primi caratteri del documento fornito (che può essere PDF, XML o TXT) "
    "e il suo nome originale. Il documento è un atto ufficiale del Parlamento italiano (Camera o Senato) o un altro documento istituzionale."
    "Il tuo unico compito è generare un nuovo nome per il file, seguendo questo formato ESATTO:\n\n"
    "<organo>_<tipo_atto>_<data_iso>_<descrizione_o_presidenza>\n\n"
    "- <organo>: 'camera' o 'senato'. Se non capisci, usa 'doc'.\n"
    "- <tipo_atto>: Tipo di atto (es. 'resoconto_stenografico', 'ddl', 'audizione'). Se non chiaro, usa 'atto'.\n"
    "- <data_iso>: Data di riferimento del documento, in formato AAAA-MM-GG.\n"
    "- <descrizione_o_presidenza>: Il cognome del presidente (es. 'fontana') o una brevissima descrizione (max 3 parole, unite da underscore). Se non trovi nulla, usa il nome del file originale senza estensione.\n\n"
    "RISPONDI SOLO ED ESCLUSIVAMENTE CON LA STRINGA DEL NUOVO NOME. NON AGGIUNGERE ALTRO TESTO."
)
# ───────────────────────────────────────────────────────────── FINE CONFIG

# Import lazy per evitare errori se le librerie non sono installate globalmente
try:
    import pdfplumber
    from google.oauth2 import service_account
    from google.auth.transport.requests import Request as GoogleRequest
    from google.cloud import storage
    from google.cloud.storage.blob import Blob
    import requests
    from tqdm import tqdm
except ImportError as e:
    sys.exit(f"❌ Errore di importazione: {e}. Esegui 'pip install google-cloud-storage pdfplumber google-auth requests tqdm'")

# --- Funzioni di supporto ---

def fetch_access_token() -> str:
    """Ottiene un token di accesso per l'API Vertex AI."""
    try:
        creds = service_account.Credentials.from_service_account_file(
            SERVICE_ACCOUNT_FILE, scopes=["https://www.googleapis.com/auth/cloud-platform"]
        )
        creds.refresh(GoogleRequest())
        return creds.token
    except Exception as e:
        raise RuntimeError(f"Impossibile ottenere il token da '{SERVICE_ACCOUNT_FILE}'. Verifica il file e le autorizzazioni (gcloud auth application-default login). Errore: {e}")

def extract_text_from_gcs_blob(blob: Blob) -> str:
    """Estrae il testo da un blob GCS usando una strategia basata sull'estensione."""
    blob_name = blob.name.lower()
    content = ""
    try:
        pdf_bytes = blob.download_as_bytes()
        
        if blob_name.endswith('.pdf'):
            with BytesIO(pdf_bytes) as pdf_stream, pdfplumber.open(pdf_stream) as pdf:
                pages_text = [p.extract_text() or "" for p in pdf.pages[:MAX_PAGES_PDF]]
            content = "\n".join(pages_text)
        
        elif blob_name.endswith(('.txt', '.xml', '.html', '.md')):
            content = pdf_bytes.decode('utf-8', errors='ignore')
            
        else:
            return "" # Tipo file non supportato

        # Unisce il nome del file originale al contenuto per dare più contesto al modello
        full_context = f"NOME FILE ORIGINALE: {Path(blob.name).name}\n\nCONTENUTO:\n{content}"
        return full_context[:MAX_CHARS_CONTENT]
        
    except Exception:
        return ""

def call_model_api(content: str, access_token: str, retries: int = 3) -> str:
    """Chiama l'API di Gemini su Vertex AI."""
    headers = {"Authorization": f"Bearer {access_token}", "Content-Type": "application/json"}
    api_url = f"https://{REGION}-aiplatform.googleapis.com/v1beta1/projects/{PROJECT_ID}/locations/{REGION}/publishers/google/models/{MODEL_ID.split('/')[-1]}:generateContent"
    
    body = {
        "system_instruction": {"parts": [{"text": SYSTEM_PROMPT}]},
        "contents": [{"role": "user", "parts": [{"text": content}]}],
        "generation_config": {"temperature": 0.0, "max_output_tokens": 128}
    }
    for attempt in range(retries):
        try:
            res = requests.post(api_url, headers=headers, json=body, timeout=60)
            res.raise_for_status()
            response_json = res.json()
            if (candidates := response_json.get("candidates")) and (parts := candidates[0].get("content", {}).get("parts", [])):
                return parts[0].get("text", "").strip()
            raise ValueError("Risposta API vuota o malformata")
        except requests.exceptions.RequestException as e:
            if attempt == retries - 1: raise
            time.sleep(2 ** attempt)
    return ""

def sanitize_filename_stem(stem: str) -> str:
    """Pulisce una stringa per renderla un nome di file valido."""
    stem = stem.lower()
    stem = re.sub(r'\s+', '_', stem)
    stem = re.sub(r'[^a-z0-9\-_.]+', '', stem)
    return stem.strip('_.') or "documento_rinominato"

def get_gcs_components(blob_name: str) -> Tuple[str, str, str]:
    """Divide un nome di blob in prefisso (cartella), nome base ed estensione."""
    parts = blob_name.split('/')
    prefix = "/".join(parts[:-1]) + "/" if len(parts) > 1 else ""
    stem, _, ext = parts[-1].rpartition('.')
    return prefix, stem, f".{ext}" if ext else ""

# --- Funzioni principali ---

def find_files_to_process(storage_client: storage.Client, bucket_name: str, prefix: str) -> List[Blob]:
    """Trova tutti i file in un percorso GCS, escludendo i JSON."""
    all_blobs = storage_client.list_blobs(bucket_name, prefix=prefix)
    return [b for b in all_blobs if not b.name.lower().endswith(('.json', '.jsonl')) and not b.name.endswith('/')]

def process_blob(blob: Blob, storage_client: storage.Client, bucket_name: str, access_token: str) -> Tuple[str, str]:
    """Elabora un singolo blob: estrae testo, chiama l'IA, rinomina."""
    original_uri = f"gs://{bucket_name}/{blob.name}"
    
    text_content = extract_text_from_gcs_blob(blob)
    if not text_content:
        return original_uri, "NO_TEXT_EXTRACTED_OR_UNSUPPORTED_TYPE"

    try:
        new_stem = call_model_api(text_content, access_token)
        if not new_stem:
            return original_uri, "MODEL_EMPTY_RESPONSE"
        
        sanitized_stem = sanitize_filename_stem(new_stem)
        
        prefix, _, extension = get_gcs_components(blob.name)
        new_blob_name = f"{prefix}{sanitized_stem}{extension}"

        if blob.name == new_blob_name:
            return original_uri, "SAME_NAME_SKIPPED"
        
        bucket = storage_client.bucket(bucket_name)
        new_blob = bucket.copy_blob(blob, bucket, new_blob_name)
        if new_blob:
            blob.delete()
            return original_uri, f"gs://{bucket_name}/{new_blob_name}"
        else:
            return original_uri, "ERROR:COPY_FAILED"
            
    except Exception as e:
        tb_str = traceback.format_exc()
        return original_uri, f"ERROR:{type(e).__name__} - {e} - {tb_str}"

def main(gcs_input_path: str):
    """Funzione principale che orchestra la rinomina."""
    parsed_uri = urlparse(gcs_input_path)
    if parsed_uri.scheme != "gs":
        sys.exit(f"❌ URI non valido: '{gcs_input_path}'. Deve iniziare con 'gs://'")
    
    bucket_name = parsed_uri.netloc
    prefix = parsed_uri.path.lstrip('/')
    if prefix and not prefix.endswith('/'): prefix += '/'

    print(f"Collegamento a GCS: bucket='{bucket_name}', prefisso='{prefix or '(root)'}'")
    storage_client = storage.Client(project=PROJECT_ID)

    print("🔑 Ottenimento token per Vertex AI...")
    access_token = fetch_access_token()
    
    print("🔍 Ricerca dei file da rinominare...")
    blobs_to_process = find_files_to_process(storage_client, bucket_name, prefix)
    if not blobs_to_process:
        sys.exit("✅ Nessun file da processare trovato nel percorso specificato.")
    print(f"📄 Trovati {len(blobs_to_process)} file da elaborare.")

    log_path = Path(LOG_FILE)
    with open(log_path, "w", newline="", encoding="utf-8") as fh, ThreadPoolExecutor(max_workers=WORKERS) as pool:
        writer = csv.writer(fh)
        writer.writerow(["vecchio_uri", "nuovo_uri_o_stato"])
        
        futures = {pool.submit(process_blob, blob, storage_client, bucket_name, access_token): blob for blob in blobs_to_process}
        
        for future in tqdm(as_completed(futures), total=len(blobs_to_process), desc="Rinomina file"):
            result = future.result()
            writer.writerow(result)
            fh.flush()
            
    print(f"\n✅ Processo completato. Log dettagliato salvato in: {log_path.resolve()}")

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Rinomina file in GCS con Vertex AI.")
    parser.add_argument("--gcs-input", "-i", required=True, help="URI del percorso GCS (es. gs://bucket/cartella/).")
    args = parser.parse_args()

    try:
        main(args.gcs_input)
    except Exception as e:
        print(f"\n❌ ERRORE GLOBALE: {e}")
        traceback.print_exc()