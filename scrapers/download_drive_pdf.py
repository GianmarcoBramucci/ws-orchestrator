#!/usr/bin/env python3
"""
download_drive_pdf_fixed.py - CRASH-FIXED VERSION + FILENAME SANITIZATION v1.2
==============================================================================

Downloader Google Drive stabilizzato con:
- Download diretto su file (niente accumulo in RAM)
- Threading ridotto e sicuro
- Error handling robusto
- Retry logic con back-off
- SANITIZZAZIONE NOMI FILE per Windows (FIXED!)
- Versione 1.2: Sanitizzazione più aggressiva per risolvere definitivamente il problema "/"
"""

from __future__ import annotations

import argparse
import json
import sys
import time
import datetime as dt
import traceback
from pathlib import Path
from typing import Optional, Dict, List, Tuple
from concurrent.futures import ThreadPoolExecutor, as_completed
from threading import Lock, Semaphore
import gc

try:
    from google.oauth2 import service_account
    from googleapiclient.discovery import build
    from googleapiclient.errors import HttpError
    from googleapiclient.http import MediaIoBaseDownload
    from tqdm import tqdm
    import io
except ImportError as e:
    sys.exit(
        f"❌ Libreria mancante: {e}. Installa con: "
        "pip install google-api-python-client google-auth tqdm"
    )

# ═══════════════════════════════════════════════════════════════════════════════
# CONFIGURAZIONE ANTI-CRASH + SANITIZZAZIONE
# ═══════════════════════════════════════════════════════════════════════════════

CONFIG = {
    # Threading ridotto per stabilità
    "max_workers": 1,  
    "chunk_download_size": 5*1024*1024,  

    # Rate-limit più conservativo
    "api_delay": 0.1,
    "download_delay": 0.2,
    "page_size": 100,

    # Retry / timeout
    "max_retries": 5,
    "base_timeout": 30,
    "max_timeout": 120,
    "backoff_factor": 2,

    # Memory management
    "gc_frequency": 50,
    "max_file_size_mb": 100,

    # Error handling
    "max_consecutive_errors": 10,
    "abort_on_memory_error": True,
    
    # Sanitizzazione file
    "preserve_readability": True,
    "max_filename_length": 200,
}

SUPPORTED_EXTENSIONS = {
    ".pdf",
    ".docx",
    ".doc",
    ".xlsx",
    ".xls",
    ".txt",
    ".md",
    ".json",
}
CREDENTIALS_FILE = "GOOGLE_CREDENTIALS.json"

# Locks globali per thread-safety
PRINT_LOCK = Lock()
STATS_LOCK = Lock()
ERROR_LOCK = Lock()


def safe_print(msg: str, prefix: str = ""):
    """Print thread-safe con lock."""
    with PRINT_LOCK:
        try:
            print(f"[{prefix}] {msg}" if prefix else msg)
        except UnicodeEncodeError:
            ascii_msg = msg.encode("ascii", "replace").decode("ascii")
            print(f"[{prefix}] {ascii_msg}" if prefix else ascii_msg)


# ═══════════════════════════════════════════════════════════════════════════════
# SANITIZZAZIONE NOMI FILE - VERSIONE MIGLIORATA v1.2
# ═══════════════════════════════════════════════════════════════════════════════

def sanitize_google_drive_filename(filename: str) -> str:
    """
    Sanitizza i nomi file da Google Drive per compatibilità Windows.
    Versione 1.2: Più aggressiva, garantisce rimozione di TUTTI i caratteri problematici.
    """
    if not filename or not filename.strip():
        return "file_senza_nome"
    
    # Prima passa: sostituzioni per preservare leggibilità
    readable_replacements = {
        '/': '_',           # SEMPRE underscore per evitare problemi directory
        '\\': '_',          # Backslash → underscore
        ':': '_',           # Due punti → underscore (era "alle" prima)
        '?': '',            # Rimozione punti interrogativi
        '*': '_',           # Asterisco → underscore
        '<': '_',           # Minore → underscore
        '>': '_',           # Maggiore → underscore  
        '|': '_',           # Pipe → underscore
        '"': '',            # Rimuovi virgolette
        '\n': '_',          # Newline → underscore
        '\r': '_',          # Carriage return → underscore
        '\t': '_',          # Tab → underscore
    }
    
    sanitized = filename
    for old_char, new_char in readable_replacements.items():
        sanitized = sanitized.replace(old_char, new_char)
    
    # Seconda passa: rimuovi caratteri di controllo (ASCII 0-31 e 127)
    sanitized = ''.join(char for char in sanitized if 32 <= ord(char) < 127 or ord(char) > 127)
    
    # Terza passa: doppio controllo caratteri Windows proibiti
    # Questo è ridondante ma garantisce sicurezza al 100%
    forbidden_chars = '<>:"/\\|?*'
    for char in forbidden_chars:
        sanitized = sanitized.replace(char, '_')
    
    # Pulizia spazi e underscore multipli
    sanitized = ' '.join(sanitized.split())  # Normalizza spazi multipli
    while '__' in sanitized:
        sanitized = sanitized.replace('__', '_')
    while '_ ' in sanitized:
        sanitized = sanitized.replace('_ ', ' ')
    while ' _' in sanitized:
        sanitized = sanitized.replace(' _', ' ')
    
    # Rimuovi underscore iniziali e finali
    sanitized = sanitized.strip('_. ')
    
    # Gestione nomi riservati Windows
    reserved_names = {
        'CON', 'PRN', 'AUX', 'NUL', 'COM1', 'COM2', 'COM3', 'COM4',
        'COM5', 'COM6', 'COM7', 'COM8', 'COM9', 'LPT1', 'LPT2',
        'LPT3', 'LPT4', 'LPT5', 'LPT6', 'LPT7', 'LPT8', 'LPT9'
    }
    
    # Controlla se il nome (senza estensione) è riservato
    name_without_ext = sanitized.rsplit('.', 1)[0] if '.' in sanitized else sanitized
    if name_without_ext.upper() in reserved_names:
        sanitized = f"file_{sanitized}"
    
    # Limita lunghezza
    if len(sanitized) > CONFIG["max_filename_length"]:
        parts = sanitized.rsplit('.', 1)
        if len(parts) == 2:
            name, ext = parts
            max_name_len = CONFIG["max_filename_length"] - len(ext) - 1
            sanitized = f"{name[:max_name_len]}.{ext}"
        else:
            sanitized = sanitized[:CONFIG["max_filename_length"]]
    
    # Verifica finale: assicurati che non ci siano slash
    if '/' in sanitized or '\\' in sanitized:
        safe_print(f"ATTENZIONE: Slash ancora presente dopo sanitizzazione! '{sanitized}'", "SANITIZE_ERROR")
        sanitized = sanitized.replace('/', '_').replace('\\', '_')
    
    # Fallback se il nome è diventato vuoto
    return sanitized if sanitized else "file_sanitizzato"


def test_sanitization():
    """Test della sanitizzazione per verificare che funzioni."""
    test_cases = [
        "ELENCO DOCUMENTI UFFICIO STUDI XIX LEGISLATURA - AGGIORNATO AL 21/06/2025.xlsx",
        "Meeting Notes 14:30 - Project/Status.pdf", 
        "Report Q1/Q2 <DRAFT>.docx",
        "Config file*.json",
        "User Guide v2.0?.txt",
        "Test<>chars:invalid/name.pdf",
        "CON.txt",  # Nome riservato Windows
        "Multiple///Slashes\\\\Test.doc",
        "Tab\tand\nNewline\rTest.pdf"
    ]
    
    safe_print("🧪 Test sanitizzazione nomi file:", "TEST")
    safe_print("=" * 70, "TEST")
    
    for original in test_cases:
        sanitized = sanitize_google_drive_filename(original)
        safe_print(f"Originale:   '{original}'", "TEST")
        safe_print(f"Sanitizzato: '{sanitized}'", "TEST")
        
        # Verifica che non contenga caratteri proibiti Windows
        forbidden = '<>:"/\\|?*'
        has_forbidden = any(char in sanitized for char in forbidden)
        if has_forbidden:
            found_chars = [char for char in forbidden if char in sanitized]
            safe_print(f"❌ ERRORE: contiene ancora caratteri proibiti: {found_chars}", "TEST")
        else:
            safe_print(f"✅ OK: nessun carattere proibito", "TEST")
        
        safe_print("-" * 70, "TEST")
    
    safe_print("🧪 Test sanitizzazione completato", "TEST")


# ═══════════════════════════════════════════════════════════════════════════════
# RESTO DEL CODICE (con sanitizzazione integrata)
# ═══════════════════════════════════════════════════════════════════════════════

class SafeGoogleDriveDownloader:
    """Downloader Google Drive stabilizzato anti-crash con sanitizzazione nomi."""

    def __init__(self, credentials_file: str = CREDENTIALS_FILE):
        self.credentials_file = credentials_file
        self.service = None
        self.stats = {
            "total_files": 0,
            "downloaded": 0,
            "skipped": 0,
            "errors": 0,
            "consecutive_errors": 0,
            "total_size": 0,
            "start_time": time.time(),
            "sanitized_files": 0,
        }
        self.rate_limiter = Semaphore(CONFIG["max_workers"])
        self.error_files = []

        self._init_service()

    def _init_service(self):
        try:
            if Path(self.credentials_file).exists():
                creds = service_account.Credentials.from_service_account_file(
                    self.credentials_file,
                    scopes=["https://www.googleapis.com/auth/drive.readonly"],
                )
                self.service = build("drive", "v3", credentials=creds)
                safe_print(
                    f"Drive API inizializzato con service account: {self.credentials_file}",
                    "OK",
                )
            else:
                safe_print(f"File credenziali non trovato: {self.credentials_file}", "ERROR")
                sys.exit(1)
        except Exception as e:
            safe_print(f"Errore inizializzazione Drive API: {e}", "ERROR")
            sys.exit(1)

    def _safe_api_call(self, api_call, *args, **kwargs):
        for attempt in range(CONFIG["max_retries"]):
            try:
                time.sleep(CONFIG["api_delay"] * (attempt + 1))
                return api_call(*args, **kwargs).execute()
            except HttpError as e:
                if e.resp.status == 403 and "quota" in str(e).lower():
                    safe_print("Quota API esaurita!", "QUOTA")
                    sys.exit(1)
                if e.resp.status == 404:
                    safe_print(f"Risorsa non trovata: {e}", "WARNING")
                    return None
                if attempt == CONFIG["max_retries"] - 1:
                    safe_print(f"API call fallita definitivamente: {e}", "ERROR")
                    raise
                wait_time = CONFIG["backoff_factor"] ** attempt
                safe_print(f"Retry API call in {wait_time}s (tentativo {attempt+1})", "RETRY")
                time.sleep(wait_time)
            except Exception as e:
                if attempt == CONFIG["max_retries"] - 1:
                    safe_print(f"Errore API generico: {e}", "ERROR")
                    raise
                time.sleep(CONFIG["backoff_factor"] ** attempt)
        return None

    def scan_folder_recursive(
        self,
        folder_id: str,
        max_depth: int = 2,
        from_date: Optional[dt.date] = None,
    ) -> List[Dict]:
        safe_print(f"Scansione ricorsiva cartella Drive ID: {folder_id}", "SCAN")
        all_files: List[Dict] = []
        folders_to_scan: List[Tuple[str, int]] = [(folder_id, 0)]

        while folders_to_scan:
            current_folder, depth = folders_to_scan.pop(0)
            if depth > max_depth:
                continue

            safe_print(f"File trovati finora: {len(all_files)} (profondità: {depth})", "SCAN")

            page_token = None
            while True:
                try:
                    query = f"'{current_folder}' in parents and trashed=false"
                    request_body = {
                        "q": query,
                        "pageSize": CONFIG["page_size"],
                        "fields": "nextPageToken, files(id, name, mimeType, size, createdTime, modifiedTime)",
                        "pageToken": page_token,
                    }
                    results = self._safe_api_call(self.service.files().list, **request_body)
                    if not results:
                        break

                    items = results.get("files", [])
                    safe_print(f"Trovati {len(items)} elementi (profondità {depth})", "PROGRESS")

                    for item in items:
                        if item["mimeType"] == "application/vnd.google-apps.folder":
                            if depth < max_depth:
                                folders_to_scan.append((item["id"], depth + 1))
                        else:
                            file_name = item["name"]
                            if Path(file_name).suffix.lower() in SUPPORTED_EXTENSIONS:
                                if from_date:
                                    try:
                                        created_time = (
                                            dt.datetime.fromisoformat(
                                                item["createdTime"].replace("Z", "+00:00")
                                            ).date()
                                        )
                                        if created_time < from_date:
                                            continue
                                    except Exception:
                                        pass
                                all_files.append(item)
                                if len(all_files) % 25 == 0:
                                    safe_print(f"File #{len(all_files)}: {file_name[:50]}...", "FOUND")

                    page_token = results.get("nextPageToken")
                    if not page_token:
                        break

                    if len(all_files) % CONFIG["gc_frequency"] == 0:
                        gc.collect()

                except Exception as e:
                    safe_print(f"Errore durante scansione: {e}", "ERROR")
                    break

        total_size_mb = sum(int(f.get("size", 0)) for f in all_files) / (1024 * 1024)
        safe_print(f"Trovati {len(all_files)} file supportati", "STATS")
        safe_print(f"File da scaricare: {len(all_files)}", "STATS")
        safe_print(f"Dimensione totale: {total_size_mb:.1f} MB", "SIZE")
        return all_files

    def download_single_file(self, file_info: Dict, output_dir: Path) -> bool:
        file_id = file_info["id"]
        original_name = file_info["name"]
        file_size = int(file_info.get("size", 0))

        # ═══ SANITIZZAZIONE NOME FILE v1.2 ═══
        sanitized_name = sanitize_google_drive_filename(original_name)
        
        # Log dettagliato per debug
        if sanitized_name != original_name:
            safe_print(f"Nome originale:  '{original_name}'", "SANITIZE")
            safe_print(f"Nome sanitizzato: '{sanitized_name}'", "SANITIZE")
            with STATS_LOCK:
                self.stats["sanitized_files"] += 1

        if file_size > CONFIG["max_file_size_mb"] * 1024 * 1024:
            safe_print(
                f"File troppo grande saltato: {sanitized_name} ({file_size / (1024*1024):.1f} MB)",
                "SKIP",
            )
            with STATS_LOCK:
                self.stats["skipped"] += 1
            return True

        # Usa il nome sanitizzato per il percorso
        dest_path = output_dir / sanitized_name
        
        # Verifica finale che il percorso sia valido
        try:
            dest_path.parent.mkdir(parents=True, exist_ok=True)
        except Exception as e:
            safe_print(f"Errore creazione directory per {sanitized_name}: {e}", "ERROR")
            with STATS_LOCK:
                self.stats["errors"] += 1
            return False
        
        if dest_path.exists():
            safe_print(f"File già esistente: {sanitized_name}", "EXISTS")
            with STATS_LOCK:
                self.stats["skipped"] += 1
            return True

        with self.rate_limiter:
            for attempt in range(CONFIG["max_retries"]):
                try:
                    safe_print(f"Scaricando: {sanitized_name} (tentativo {attempt + 1})", "DOWNLOAD")

                    request = self.service.files().get_media(fileId=file_id)

                    # ── DOWNLOAD STREAMING DIRETTO ──
                    temp_path = dest_path.with_suffix(".tmp")
                    with open(temp_path, "wb") as fh:
                        downloader = MediaIoBaseDownload(
                            fh, request, chunksize=CONFIG["chunk_download_size"]
                        )
                        done = False
                        while not done:
                            status, done = downloader.next_chunk()
                    temp_path.rename(dest_path)

                    # Crea metadata
                    self._create_metadata(dest_path, file_info, original_name, sanitized_name)

                    with STATS_LOCK:
                        self.stats["downloaded"] += 1
                        self.stats["total_size"] += file_size
                        self.stats["consecutive_errors"] = 0

                    time.sleep(CONFIG["download_delay"])
                    return True

                except Exception as e:
                    safe_print(
                        f"Errore download {sanitized_name} (tentativo {attempt + 1}): {e}",
                        "ERROR",
                    )
                    temp_path = dest_path.with_suffix(".tmp")
                    if temp_path.exists():
                        temp_path.unlink()

                    if attempt == CONFIG["max_retries"] - 1:
                        with ERROR_LOCK:
                            self.error_files.append((sanitized_name, str(e)))
                        with STATS_LOCK:
                            self.stats["errors"] += 1
                            self.stats["consecutive_errors"] += 1
                        if (
                            self.stats["consecutive_errors"]
                            >= CONFIG["max_consecutive_errors"]
                        ):
                            safe_print(
                                f"Troppi errori consecutivi ({CONFIG['max_consecutive_errors']}). Abort!",
                                "ABORT",
                            )
                            return False
                        return False
                    wait_time = CONFIG["backoff_factor"] ** attempt
                    time.sleep(wait_time)
        return False

    def _create_metadata(self, file_path: Path, file_info: Dict, original_name: str, sanitized_name: str):
        try:
            date_created = None
            if "createdTime" in file_info:
                try:
                    date_created = (
                        dt.datetime.fromisoformat(file_info["createdTime"].replace("Z", "+00:00"))
                        .date()
                        .isoformat()
                    )
                except Exception:
                    pass

            metadata = {
                "drive_file_id": file_info["id"],
                "original_name": original_name,
                "sanitized_name": sanitized_name,
                "name_was_sanitized": original_name != sanitized_name,
                "mime_type": file_info["mimeType"],
                "size_bytes": int(file_info.get("size", 0)),
                "created_time": file_info.get("createdTime"),
                "modified_time": file_info.get("modifiedTime"),
                "date": date_created,
                "source": "google_drive",
                "document_type": "office_document",
                "language": "it",
                "downloaded_at": dt.datetime.now(dt.timezone.utc).isoformat(),
                "downloader_version": "1.2_sanitized",
                "sanitization_info": {
                    "version": "1.2",
                    "preserve_readability": CONFIG["preserve_readability"],
                    "max_filename_length": CONFIG["max_filename_length"]
                }
            }

            json_path = file_path.with_suffix(".json")
            with open(json_path, "w", encoding="utf-8") as f:
                json.dump(metadata, f, ensure_ascii=False, indent=2)

        except Exception as e:
            safe_print(f"Errore creazione metadata per {file_path.name}: {e}", "WARNING")

    def download_all_files(self, files: List[Dict], output_dir: Path) -> bool:
        if not files:
            safe_print("Nessun file da scaricare", "INFO")
            return True

        output_dir.mkdir(parents=True, exist_ok=True)
        self.stats["total_files"] = len(files)
        safe_print(
            f"Inizio download di {len(files)} file con {CONFIG['max_workers']} thread...",
            "START",
        )

        success = True
        with ThreadPoolExecutor(max_workers=CONFIG["max_workers"]) as executor:
            try:
                future_to_file = {
                    executor.submit(self.download_single_file, file_info, output_dir): file_info
                    for file_info in files
                }

                with tqdm(total=len(files), desc="Download", unit="file") as pbar:
                    for future in as_completed(future_to_file):
                        file_info = future_to_file[future]
                        try:
                            result = future.result(timeout=CONFIG["max_timeout"])
                            if not result:
                                success = False
                                if (
                                    self.stats["consecutive_errors"]
                                    >= CONFIG["max_consecutive_errors"]
                                ):
                                    safe_print("Interrompendo download per troppi errori", "ABORT")
                                    break
                            pbar.update(1)
                            if pbar.n % CONFIG["gc_frequency"] == 0:
                                gc.collect()
                        except Exception as e:
                            safe_print(f"Errore task download {file_info['name']}: {e}", "ERROR")
                            success = False
                            with STATS_LOCK:
                                self.stats["errors"] += 1
                            pbar.update(1)
            except KeyboardInterrupt:
                safe_print("Download interrotto dall'utente", "INTERRUPT")
                return False

        self._print_final_stats()
        return success and self.stats["errors"] == 0

    def _print_final_stats(self):
        elapsed = time.time() - self.stats["start_time"]
        safe_print("")
        safe_print("=" * 60)
        safe_print("STATISTICHE FINALI", "STATS")
        safe_print("=" * 60)
        safe_print(f"Tempo totale: {elapsed:.1f}s", "TIME")
        safe_print(f"File processati: {self.stats['total_files']}", "FILES")
        safe_print(f"Download riusciti: {self.stats['downloaded']}", "SUCCESS")
        safe_print(f"File saltati: {self.stats['skipped']}", "SKIPPED")
        safe_print(f"Errori: {self.stats['errors']}", "ERRORS")
        safe_print(f"File con nomi sanitizzati: {self.stats['sanitized_files']}", "SANITIZE")
        safe_print(f"Dimensione scaricata: {self.stats['total_size'] / (1024*1024):.1f} MB", "SIZE")
        
        if self.error_files:
            safe_print(f"File con errori ({len(self.error_files)}):", "ERROR_LIST")
            for file_name, error in self.error_files[:10]:
                safe_print(f"  • {file_name}: {error}")
            if len(self.error_files) > 10:
                safe_print(f"  ... e altri {len(self.error_files) - 10} errori")


# ───────────────────────────────────────────────────────────────────────────────
# MAIN
# ───────────────────────────────────────────────────────────────────────────────
def main():
    parser = argparse.ArgumentParser(
        description="Google Drive Downloader v1.2 - CRASH-FIXED + SANITIZATION",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Esempi:
  python download_drive_pdf_fixed.py --folder-id FOLDER_ID --out ./downloads
  python download_drive_pdf_fixed.py --folder-id FOLDER_ID --from 2024-01-01 --out ./downloads
  python download_drive_pdf_fixed.py --folder-id FOLDER_ID --safe-mode --out ./downloads
  python download_drive_pdf_fixed.py --test-sanitization  # Test sanitizzazione
""",
    )

    parser.add_argument("--folder-id", help="ID della cartella Drive")
    parser.add_argument("--out", type=Path, help="Cartella di output")
    parser.add_argument(
        "--from",
        dest="from_date",
        type=lambda s: dt.datetime.strptime(s, "%Y-%m-%d").date(),
        help="Data iniziale (YYYY-MM-DD)",
    )
    parser.add_argument(
        "--max-depth", type=int, default=2, help="Profondità massima scansione (default: 2)"
    )
    parser.add_argument(
        "--safe-mode",
        action="store_true",
        help="Modalità ultra-sicura (1 thread, più lenta)",
    )
    parser.add_argument(
        "--test-sanitization",
        action="store_true",
        help="Esegue solo test della sanitizzazione dei nomi file"
    )

    args = parser.parse_args()

    # Test sanitizzazione se richiesto
    if args.test_sanitization:
        test_sanitization()
        sys.exit(0)

    # Verifica argomenti richiesti
    if not args.folder_id or not args.out:
        parser.error("--folder-id e --out sono richiesti (usa --test-sanitization per test)")

    if args.safe_mode:
        CONFIG["max_workers"] = 1
        CONFIG["api_delay"] = 0.5
        CONFIG["download_delay"] = 0.5
        safe_print("Modalità ultra-sicura attivata", "SAFE")

    try:
        downloader = SafeGoogleDriveDownloader()
        safe_print("GOOGLE DRIVE DOWNLOADER v1.2 - CRASH-FIXED + SANITIZATION", "FOLDER")
        safe_print(f"Cartella Drive ID: {args.folder_id}", "FOLDER")
        safe_print(
            f"Range date: {args.from_date.isoformat() if args.from_date else 'inizio'} - oggi",
            "DATE",
        )
        safe_print(f"Output directory: {args.out.resolve()}", "FOLDER")
        safe_print(f"Profondità max: {args.max_depth}, Safe mode: {args.safe_mode}", "CONFIG")
        safe_print(
            f"Thread: {CONFIG['max_workers']}, Delay API: {CONFIG['api_delay']}s, Page size: {CONFIG['page_size']}",
            "CONFIG",
        )
        safe_print(f"Sanitizzazione: v1.2, Preserve readability: {CONFIG['preserve_readability']}", "SANITIZE")

        files = downloader.scan_folder_recursive(args.folder_id, args.max_depth, args.from_date)
        if not files:
            safe_print("Nessun file trovato", "INFO")
            sys.exit(0)

        success = downloader.download_all_files(files, args.out)
        if success:
            safe_print("Download completato con successo!", "SUCCESS")
            sys.exit(0)
        safe_print("Download completato con errori", "WARNING")
        sys.exit(1)

    except KeyboardInterrupt:
        safe_print("Download interrotto dall'utente", "INTERRUPT")
        sys.exit(130)
    except Exception as e:
        safe_print(f"Errore fatale: {e}", "FATAL")
        traceback.print_exc()
        sys.exit(1)


if __name__ == "__main__":
    main()