#!/usr/bin/env python3
"""
upload_gcs_ingest.py — v6.2 Improved Format FIXED
=================================================
Upload che crea batch.jsonl nel formato compatibile con il tuo sistema esistente.
Versione migliorata con better error handling, logging e FIX per path management.
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

CREDENTIALS_FILE = "GOOGLE_CREDENTIALS.json"

def safe_print(msg: str):
    """Print sicuro per Windows e Unicode"""
    try:
        print(msg)
    except UnicodeEncodeError:
        ascii_msg = msg.encode('ascii', 'replace').decode('ascii')
        print(ascii_msg)

def calculate_file_hash(file_path: pathlib.Path) -> str:
    """Calcola hash MD5 del file"""
    try:
        hash_md5 = hashlib.md5()
        with open(file_path, "rb") as f:
            for chunk in iter(lambda: f.read(4096), b""):
                hash_md5.update(chunk)
        return hash_md5.hexdigest()
    except Exception:
        return "unknown"

def create_structured_record(file_path: pathlib.Path, gcs_uri: str, metadata: Dict) -> Dict:
    """Crea un record nel formato strutturato richiesto"""
    
    # ===== FIX CRITICO: Assicura che file_path sia Path object =====
    file_path = pathlib.Path(file_path)
    
    # Estrae info dal filename o metadata
    filename = file_path.stem
    
    # Determina ID univoco migliorato
    if 'legislatura' in metadata and 'seduta' in metadata:
        record_id = f"{metadata.get('source', 'doc')}_leg{metadata['legislatura']}_sed{metadata.get('seduta', '0000')}"
        if 'date' in metadata:
            record_id += f"_{metadata['date']}"
    elif 'legislatura' in metadata and 'date' in metadata:
        # Fallback per documenti senza seduta ma con data
        record_id = f"{metadata.get('source', 'doc')}_leg{metadata['legislatura']}_{metadata['date']}"
    else:
        # Fallback: usa hash del filename per unicità
        filename_hash = hashlib.md5(filename.encode()).hexdigest()[:8]
        record_id = f"{metadata.get('source', 'doc')}_{filename_hash}"
    
    # Determina mimeType
    mime_types = {
        '.pdf': 'application/pdf',
        '.txt': 'text/plain',
        '.xml': 'application/xml',
        '.json': 'application/json',
        '.html': 'text/html',
        '.md': 'text/markdown'
    }
    mime_type = mime_types.get(file_path.suffix.lower(), 'application/octet-stream')
    
    # Determina sourceType migliorato
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
    
    # Calcola file size e hash
    file_size = file_path.stat().st_size if file_path.exists() else 0
    file_hash = calculate_file_hash(file_path) if file_path.exists() else "unknown"
    
    # Struttura finale migliorata
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
            "fileHash": file_hash,
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
    
    # ===== FIX CRITICO: Normalizza src path =====
    src = pathlib.Path(src).resolve()
    safe_print(f"📁 Source normalizzata: {src} (tipo: {type(src)})")
    
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
    try:
        if pathlib.Path(CREDENTIALS_FILE).exists():
            client = storage.Client.from_service_account_json(CREDENTIALS_FILE)
            safe_print(f"🔑 Usando credenziali da: {CREDENTIALS_FILE}")
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
    
    safe_print(f"📤 Upload di {len(data_files)} file...")
    
    jsonl_records: List[Dict] = []
    upload_errors = []
    
    for data_file in tqdm(data_files, desc="Upload"):
        try:
            # ===== FIX CRITICO: Path handling sicuro =====
            data_file = pathlib.Path(data_file)  # Assicura Path object
            
            relative_path = str(data_file.relative_to(src)).replace("\\", "/")
            gcs_path = f"{prefix}/{relative_path}".lstrip("/")
            gcs_uri = f"gs://{bucket_name}/{gcs_path}"
            
            # Debug path creation
            safe_print(f"  📁 Path debug per {data_file.name}:")
            safe_print(f"    data_file: {data_file}")
            safe_print(f"    relative_path: {relative_path}")
            safe_print(f"    gcs_path: {gcs_path}")
            safe_print(f"    gcs_uri: {gcs_uri}")
            
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
            
            # Crea record strutturato
            record = create_structured_record(data_file, gcs_uri, metadata)
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
    safe_print("\n📝 Creazione batch.jsonl...")
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

def main():
    parser = argparse.ArgumentParser(
        description="Upload strutturato per sistema di ingestione - FIXED (batch.jsonl)",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Esempi:
  # Upload base
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
        safe_print("\n🎉 Upload completato!")
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