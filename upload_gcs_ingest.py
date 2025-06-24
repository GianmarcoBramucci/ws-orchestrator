#!/usr/bin/env python3
"""
upload_gcs_ingest.py — v6.3 SHA-256 HASH UPGRADE
================================================
Upload che crea batch.jsonl nel formato compatibile con il tuo sistema esistente.
Versione migliorata con better error handling, logging e SHA-256 hash per sicurezza.
"""
from __future__ import annotations
import argparse
import hashlib
import json
import sys
import tempfile
import pathlib
import datetime as dt
from typing import Dict, List, Set, Optional

try:
    from google.cloud import storage
    from google.api_core import exceptions
    from tqdm import tqdm
except ImportError as e:
    print(f"❌ ERRORE: Dipendenza mancante: {e}")
    print("🔧 Installa con: pip install -r requirements.txt")
    sys.exit(1)

# ───────────────────────────── CONFIG
CREDENTIALS_FILE: str = "GOOGLE_CREDENTIALS.json"
CONFIG_FILE: str = "config.json"
MAX_WORKERS: int = 16
HASH_ALGORITHM: str = "SHA-256"

# Base MIME types mapping (fallback universale)
MIME_TYPE_MAPPING = {
    "pdf": "application/pdf",
    "txt": "text/plain", 
    "xml": "application/xml",
    "json": "application/json",
    "html": "text/html",
    "md": "text/markdown"
}
# ───────────────────────────── END CONFIG

def load_config() -> Dict:
    """Carica configurazione da config.json"""
    try:
        with open(CONFIG_FILE, 'r', encoding='utf-8') as f:
            return json.load(f)
    except Exception as e:
        print(f"❌ Errore caricamento config da {CONFIG_FILE}: {e}")
        sys.exit(1)

def safe_print(msg: str):
    """Print sicuro per Windows e Unicode"""
    try:
        print(msg)
    except UnicodeEncodeError:
        ascii_msg = msg.encode('ascii', 'replace').decode('ascii')
        print(ascii_msg)

def calculate_file_hash(file_path: pathlib.Path) -> str:
    """Calcola hash SHA-256 del file per maggiore sicurezza"""
    try:
        hash_sha256 = hashlib.sha256()
        with open(file_path, "rb") as f:
            for chunk in iter(lambda: f.read(4096), b""):
                hash_sha256.update(chunk)
        return hash_sha256.hexdigest()
    except Exception:
        return "unknown"

def get_mime_type_for_source(file_path: pathlib.Path, source_name: str, config: Dict) -> str:
    """Determina MIME type basandosi sui file_patterns della source nel config"""
    
    # Trova la source nel config
    sources = config.get("sources", [])
    source_config = None
    for source in sources:
        if source.get("name") == source_name:
            source_config = source
            break
    
    if not source_config:
        # Fallback se source non trovata
        extension = file_path.suffix.lower().lstrip('.')
        return MIME_TYPE_MAPPING.get(extension, 'application/octet-stream')
    
    # Prendi file_patterns dal config della source
    file_patterns = source_config.get("file_patterns", [])
    file_extension = file_path.suffix.lower()  # es: ".pdf"
    
    # Controlla se l'estensione è supportata dai patterns
    for pattern in file_patterns:
        # pattern è tipo "*.pdf", "*.json"
        pattern_ext = pattern.replace("*", "")  # ".pdf", ".json"
        if file_extension == pattern_ext:
            # Converti estensione in MIME type
            clean_ext = pattern_ext.lstrip('.')  # "pdf", "json"
            return MIME_TYPE_MAPPING.get(clean_ext, 'application/octet-stream')
    
    # Fallback se estensione non matchata
    extension = file_path.suffix.lower().lstrip('.')
    return MIME_TYPE_MAPPING.get(extension, 'application/octet-stream')

def create_structured_record(file_path: pathlib.Path, gcs_uri: str, metadata: Dict, config: Dict) -> Dict:
    """Crea un record nel formato strutturato richiesto"""
    
    # ===== FIX CRITICO: Assicura che file_path sia Path object =====
    file_path = pathlib.Path(file_path)
    
    # Estrae info dal filename o metadata
    filename = file_path.stem
    
    # Determina ID univoco migliorato - ORA CON SHA-256
    if 'legislatura' in metadata and 'seduta' in metadata:
        record_id = f"{metadata.get('source', 'doc')}_leg{metadata['legislatura']}_sed{metadata.get('seduta', '0000')}"
        if 'date' in metadata:
            record_id += f"_{metadata['date']}"
    elif 'legislatura' in metadata and 'date' in metadata:
        # Fallback per documenti senza seduta ma con data
        record_id = f"{metadata.get('source', 'doc')}_leg{metadata['legislatura']}_{metadata['date']}"
    else:
        # Fallback: usa hash SHA-256 del filename per unicità (primi 12 caratteri)
        filename_hash = hashlib.sha256(filename.encode('utf-8')).hexdigest()[:12]
        record_id = f"{metadata.get('source', 'doc')}_{filename_hash}"
    
    # Determina mimeType DAI FILE_PATTERNS DEL CONFIG!
    source_name = metadata.get('source', 'unknown')
    mime_type = get_mime_type_for_source(file_path, source_name, config)
    
    # Inferisci source type e title dal campo 'source' che viene dal config name!
    source_key = metadata.get('source', 'unknown')
    
    # Source type inference dal name del config
    if source_key == 'camera':
        source_type = 'parliamentary_records_camera'
        title_prefix = 'Camera dei Deputati'
    elif source_key == 'senato':
        source_type = 'parliamentary_records_senato'
        title_prefix = 'Senato della Repubblica'
    elif 'youtube' in source_key:
        source_type = 'video_transcripts'
        title_prefix = 'YouTube'
    else:
        source_type = 'parliamentary_records'
        title_prefix = None
    
    # Costruisce title intelligente
    title_parts = []
    if title_prefix:
        title_parts.append(title_prefix)
    
    if metadata.get('document_type'):
        doc_type = metadata['document_type'].replace('_', ' ').title()
        title_parts.append(doc_type)
    
    if metadata.get('legislatura'):
        title_parts.append(f"Legislatura {metadata['legislatura']}")
        
    if metadata.get('seduta'):
        title_parts.append(f"Seduta {metadata['seduta']}")
        
    if metadata.get('date'):
        try:
            # Formatta la data in modo leggibile
            date_obj = dt.datetime.fromisoformat(metadata['date']).date()
            formatted_date = date_obj.strftime("%d/%m/%Y")
            title_parts.append(formatted_date)
        except:
            title_parts.append(metadata['date'])
    
    title = ' - '.join(title_parts) if title_parts else filename
    
    # Calcola file size e hash SHA-256
    file_size = file_path.stat().st_size if file_path.exists() else 0
    file_hash = calculate_file_hash(file_path) if file_path.exists() else "unknown"
    
    # Struttura finale migliorata con SHA-256
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
            "fileSize": file_size,
            "fileHash": file_hash,  # 🔒 Ora usa SHA-256
            "hashAlgorithm": HASH_ALGORITHM,  # 🆕 Dal CONFIG
            "uploadedAt": dt.datetime.now(dt.timezone.utc).isoformat(),
            **{k: v for k, v in metadata.items() if k not in ['source', 'document_type', 'language']}
        }
    }
    
    return record

def backup_existing_batch(bucket: storage.Bucket, batch_blob_name: str) -> Optional[str]:
    """Crea backup del batch esistente"""
    try:
        batch_blob = bucket.blob(batch_blob_name)
        if batch_blob.exists():
            timestamp = dt.datetime.now(dt.timezone.utc).strftime("%Y%m%dT%H%M%S")
            backup_name = f"{batch_blob_name}.{timestamp}.bak"
            bucket.copy_blob(batch_blob, bucket, new_name=backup_name)
            safe_print(f"📋 Backup creato: {backup_name}")
            return backup_name
    except Exception as e:
        safe_print(f"⚠️  Errore durante backup: {e}")
    return None

def upload_directory(src: pathlib.Path, bucket_name: str, prefix: str, patterns: List[str], refresh: bool):
    """Upload con formato strutturato e gestione errori migliorata"""
    
    # Carica config SOLO per credenziali
    config = load_config()
    credentials_file = config.get("global_settings", {}).get("credentials_file", CREDENTIALS_FILE)
    
    # ===== FIX CRITICO: Normalizza src path =====
    src = pathlib.Path(src).resolve()
    safe_print(f"📁 Source normalizzata: {src} (tipo: {type(src)})")
    safe_print(f"🔧 Config caricato da: {CONFIG_FILE}")
    safe_print(f"🔑 Credenziali da: {credentials_file}")
    safe_print(f"👥 Max workers: {MAX_WORKERS}")
    
    # Controllo sicurezza path
    src_str = str(src)
    problematic_patterns = [
        "downloadscamera", "downloadsenato", "camera2025", "senato2025"
    ]
    
    for pattern in problematic_patterns:
        if pattern in src_str.lower():
            error_msg = f"❌ SOURCE PATH MALFORMATO: '{src}' contiene '{pattern}'"
            safe_print(error_msg)
            safe_print("   💡 Questo indica problemi di concatenazione path!")
            sys.exit(1)
    
    safe_print("🔧 Inizializzazione client GCS...")
    safe_print(f"🔒 Utilizzando hash {HASH_ALGORITHM} per integrità file")
    
    try:
        if pathlib.Path(credentials_file).exists():
            client = storage.Client.from_service_account_json(credentials_file)
            safe_print(f"🔑 Usando credenziali da: {credentials_file}")
        else:
            client = storage.Client()
            safe_print("🔑 Usando credenziali di default")
        
        bucket = client.bucket(bucket_name)
        # Test connessione
        bucket.reload()
        safe_print(f"✅ Connesso al bucket: {bucket_name}")
        
    except Exception as e:
        safe_print(f"❌ ERRORE GCS: {e}")
        sys.exit(1)
    
    if not src.is_dir():
        safe_print(f"❌ Cartella '{src}' non esiste")
        return
    
    # Refresh se richiesto
    if refresh:
        safe_print("🧹 PULIZIA bucket...")
        try:
            blobs_to_delete = list(client.list_blobs(bucket_name, prefix=prefix))
            if blobs_to_delete:
                for blob in tqdm(blobs_to_delete, desc="Pulizia"):
                    try:
                        blob.delete()
                    except Exception as e:
                        safe_print(f"⚠️  Errore eliminazione {blob.name}: {e}")
                safe_print(f"🧹 Eliminati {len(blobs_to_delete)} file esistenti")
            else:
                safe_print("🧹 Nessun file da eliminare")
        except Exception as e:
            safe_print(f"⚠️  Errore durante pulizia: {e}")
    
    # Trova tutti i file
    safe_print(f"🔍 Ricerca file con pattern: {', '.join(patterns)}")
    all_files: Set[pathlib.Path] = set()
    for pattern in patterns:
        found_files = list(src.rglob(pattern))
        all_files.update(found_files)
        safe_print(f"  📄 {pattern}: {len(found_files)} file")
    
    data_files = [f for f in all_files if f.suffix.lower() != ".json"]
    
    if not data_files:
        safe_print("❌ Nessun file da caricare trovato")
        return
    
    safe_print(f"📤 Upload di {len(data_files)} file con hash SHA-256...")
    
    jsonl_records: List[Dict] = []
    upload_errors = []
    
    for data_file in tqdm(data_files, desc="Upload"):
        try:
            # ===== FIX CRITICO: Path handling sicuro =====
            data_file = pathlib.Path(data_file)  # Assicura Path object
            
            relative_path = str(data_file.relative_to(src)).replace("\\", "/")
            gcs_path = f"{prefix}/{relative_path}".lstrip("/")
            gcs_uri = f"gs://{bucket_name}/{gcs_path}"
            
            # Upload file
            blob = bucket.blob(gcs_path)
            blob.upload_from_filename(str(data_file))
            
            # Leggi metadata sidecar
            sidecar_path = data_file.with_suffix(".json")
            metadata: Dict = {}
            if sidecar_path.exists():
                try:
                    with open(sidecar_path, "r", encoding="utf-8") as f:
                        metadata = json.load(f)
                except Exception as e:
                    safe_print(f"⚠️  Errore lettura metadata {sidecar_path.name}: {e}")
            
            # Crea record strutturato leggendo MIME type dai file_patterns del config
            record = create_structured_record(data_file, gcs_uri, metadata, config)
            jsonl_records.append(record)
            
        except Exception as e:
            error_msg = f"Errore upload {data_file.name}: {e}"
            safe_print(f"❌ {error_msg}")
            upload_errors.append(error_msg)
    
    # Report errori
    if upload_errors:
        safe_print(f"\n⚠️  ERRORI DI UPLOAD ({len(upload_errors)}):")
        for error in upload_errors[:5]:  # Mostra solo i primi 5
            safe_print(f"   • {error}")
        if len(upload_errors) > 5:
            safe_print(f"   ... e altri {len(upload_errors) - 5} errori")
    
    # Salva batch.jsonl
    safe_print("\n📝 Creazione batch.jsonl con hash SHA-256...")
    batch_blob_name = f"{prefix}/ingest/batch.jsonl".lstrip("/")
    
    # Backup se esiste
    backup_existing_batch(bucket, batch_blob_name)
    
    # Scrivi nuovo file
    try:
        with tempfile.NamedTemporaryFile("w+", delete=False, encoding="utf-8") as tmp:
            for record in jsonl_records:
                tmp.write(json.dumps(record, ensure_ascii=False) + "\n")
            tmp_path = pathlib.Path(tmp.name)
        
        batch_blob = bucket.blob(batch_blob_name)
        batch_blob.upload_from_filename(str(tmp_path), content_type="application/json")
        
        safe_print(f"✅ SUCCESS: batch.jsonl caricato in gs://{bucket_name}/{batch_blob_name}")
        safe_print(f"🔒 Tutti i file ora hanno hash {HASH_ALGORITHM} per maggiore sicurezza")
        
    except Exception as e:
        safe_print(f"❌ ERRORE creazione batch.jsonl: {e}")
    finally:
        if 'tmp_path' in locals():
            tmp_path.unlink(missing_ok=True)
    
    # Summary finale
    successful_uploads = len(data_files) - len(upload_errors)
    safe_print(f"\n🎯 SUMMARY:")
    safe_print(f"   ✅ File caricati: {successful_uploads}")
    safe_print(f"   ❌ Errori: {len(upload_errors)}")
    safe_print(f"   📊 Record batch: {len(jsonl_records)}")
    safe_print(f"   🔒 Hash algorithm: {HASH_ALGORITHM}")

def main():
    parser = argparse.ArgumentParser(
        description="Upload strutturato per sistema di ingestione - SHA-256 UPGRADE (batch.jsonl)",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Esempi:
  # Upload base con SHA-256
  python upload_gcs_ingest.py --src ./downloads --bucket my-bucket --prefix docs --patterns "*.pdf,*.json"
  
  # Upload con refresh completo
  python upload_gcs_ingest.py --src ./downloads --bucket my-bucket --prefix docs --patterns "*.pdf" --refresh
        """
    )
    
    parser.add_argument("--src", required=True, type=pathlib.Path,
                       help="Cartella sorgente con i file da caricare")
    parser.add_argument("--bucket", required=True,
                       help="Nome del bucket GCS")
    parser.add_argument("--prefix", default="",
                       help="Prefisso/path nel bucket (default: root)")
    parser.add_argument("--patterns", required=True,
                       help="Pattern file separati da virgola (es: *.pdf,*.json)")
    parser.add_argument("--refresh", action="store_true",
                       help="Elimina tutto il contenuto esistente prima di caricare")
    
    args = parser.parse_args()
    
    # ===== FIX CRITICO: Normalizza args.src =====
    args.src = pathlib.Path(args.src).resolve()
    safe_print(f"📁 Source path normalizzata: {args.src}")
    
    patterns_list = [p.strip() for p in args.patterns.split(',')]
    
    try:
        upload_directory(args.src, args.bucket, args.prefix, patterns_list, args.refresh)
        safe_print("\n🎉 Upload completato con hash SHA-256!")
    except KeyboardInterrupt:
        safe_print("\n🛑 Upload interrotto dall'utente")
        sys.exit(130)
    except Exception as e:
        safe_print(f"\n💥 Errore fatale: {e}")
        import traceback
        traceback.print_exc()
        sys.exit(1)

if __name__ == "__main__":
    main()