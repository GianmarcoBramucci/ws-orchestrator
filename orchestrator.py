#!/usr/bin/env python3
"""
orchestrator_universal.py - Sistema di Orchestrazione Universale
==================================================================
Sistema generico per scaricare, processare e caricare documenti da qualsiasi fonte.
Progettato per essere futuro-proof e completamente configurabile.
"""
from __future__ import annotations
import argparse
import subprocess
import shlex
import sys
import datetime as dt
import pathlib
import json
import io
import traceback
from typing import Dict, List, Optional, Any
from dataclasses import dataclass

try:
    from google.cloud import storage
    from google.api_core import exceptions
except ImportError:
    print("❌ ERRORE: Installa le dipendenze con: pip install -r requirements.txt")
    sys.exit(1)


@dataclass
class ProcessResult:
    """Risultato di un processo"""
    success: bool
    message: str
    data: Optional[Dict] = None


class UniversalOrchestrator:
    """Orchestratore universale per l'ingestione di documenti"""
    
    def __init__(self, config_path: pathlib.Path, credentials_file: str = "GOOGLE_CREDENTIALS.json"):
        self.config_path = config_path
        self.credentials_file = credentials_file
        self.config = self._load_config()
        self.storage_client = None
        
    def _load_config(self) -> Dict:
        """Carica la configurazione"""
        if not self.config_path.exists():
            raise FileNotFoundError(f"File di configurazione non trovato: {self.config_path}")
        
        with open(self.config_path, "r", encoding="utf-8") as f:
            config = json.load(f)
        
        # Validazione configurazione base
        required_keys = ["sources", "upload", "rename"]
        for key in required_keys:
            if key not in config:
                raise ValueError(f"Chiave mancante nella configurazione: {key}")
        
        return config
    
    def _init_storage_client(self):
        """Inizializza il client Google Cloud Storage"""
        if self.storage_client is None:
            try:
                if pathlib.Path(self.credentials_file).exists():
                    self.storage_client = storage.Client.from_service_account_json(self.credentials_file)
                else:
                    # Fallback su default credentials
                    self.storage_client = storage.Client()
            except Exception as e:
                raise RuntimeError(f"Impossibile inizializzare GCS client: {e}")
    
    def get_latest_date_from_gcs(self, bucket_name: str, prefix: str) -> Optional[dt.date]:
        """Ottiene l'ultima data processata da GCS"""
        self._init_storage_client()
        
        print(f"  🔍 Controllo ultima data in gs://{bucket_name}/{prefix}/ingest/metadata.jsonl...")
        
        try:
            bucket = self.storage_client.bucket(bucket_name)
            metadata_blob_path = f"{prefix}/ingest/metadata.jsonl".lstrip("/")
            blob = bucket.blob(metadata_blob_path)

            if not blob.exists():
                print("  📝 metadata.jsonl non trovato - primo avvio")
                return None

            content = blob.download_as_text()
            latest_date = None
            
            with io.StringIO(content) as f:
                for line in f:
                    if not line.strip():
                        continue
                    try:
                        record = json.loads(line)
                        if 'date' in record:
                            current_date = dt.date.fromisoformat(record['date'])
                            if latest_date is None or current_date > latest_date:
                                latest_date = current_date
                    except (json.JSONDecodeError, TypeError, ValueError) as e:
                        print(f"  ⚠️  Record malformato ignorato: {e}")
                        continue
            
            if latest_date:
                print(f"  📅 Ultima data trovata: {latest_date.isoformat()}")
            else:
                print("  📝 Nessuna data valida nel metadata.jsonl")
            
            return latest_date
            
        except Exception as e:
            print(f"  ❌ Errore nel leggere metadata: {type(e).__name__}: {e}")
            return None
    
    def run_command(self, cmd: str, source_name: str = "") -> ProcessResult:
        """Esegue un comando e ritorna il risultato"""
        display_name = f" [{source_name}]" if source_name else ""
        
        print(f"\n┏━━━ COMANDO{display_name} ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━")
        print(f"┃ ▶ {cmd}")
        print(f"┗━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━")
        
        try:
            process = subprocess.Popen(
                shlex.split(cmd),
                stdout=subprocess.PIPE,
                stderr=subprocess.STDOUT,
                text=True,
                encoding='utf-8',
                errors='replace'
            )
            
            output_lines = []
            if process.stdout:
                for line in iter(process.stdout.readline, ''):
                    print(line, end='')
                    output_lines.append(line.strip())
            
            return_code = process.wait()
            
            if return_code == 0:
                return ProcessResult(True, "Comando completato con successo", 
                                   {"output": output_lines})
            else:
                return ProcessResult(False, f"Comando fallito con codice {return_code}")
                
        except Exception as e:
            error_msg = f"Errore nell'esecuzione del comando: {e}"
            print(f"\n❌ {error_msg}")
            return ProcessResult(False, error_msg)
    
    def determine_start_date(self, source: Dict, explicit_date: Optional[dt.date]) -> Optional[dt.date]:
        """Determina la data di partenza per una fonte"""
        if explicit_date:
            print(f"  📅 Data esplicita fornita: {explicit_date.isoformat()}")
            return explicit_date
        
        # Controlla ultima data su GCS
        gcs_bucket = source["bucket"]
        gcs_prefix = source.get("gcs_prefix", "")
        
        latest_gcs_date = self.get_latest_date_from_gcs(gcs_bucket, gcs_prefix)
        
        if latest_gcs_date:
            start_date = latest_gcs_date + dt.timedelta(days=1)
            print(f"  📈 Data di partenza (da GCS + 1): {start_date.isoformat()}")
            return start_date
        
        # Fallback su configurazione o data minima
        fallback_date = source.get("default_start_date")
        if fallback_date:
            fallback = dt.date.fromisoformat(fallback_date)
            print(f"  📋 Data di partenza (fallback config): {fallback.isoformat()}")
            return fallback
        
        print("  ⚠️  Nessuna data di partenza determinata")
        return None
    
    def build_command_args(self, base_args: Dict, extra_args: Dict) -> str:
        """Costruisce gli argomenti per un comando"""
        all_args = {**base_args, **extra_args}
        return " ".join([f"--{key} {value}" for key, value in all_args.items()])
    
    def process_source(self, source: Dict, args) -> ProcessResult:
        """Processa una singola fonte"""
        name = source["name"]
        print(f"\n{'='*60}")
        print(f"🚀 PROCESSANDO FONTE: {name.upper()}")
        print(f"{'='*60}")
        
        # Preparazione directory locale
        local_source_path = args.out / source["local_output_subdir"]
        local_source_path.mkdir(parents=True, exist_ok=True)
        
        # Determinazione data di partenza
        start_date = self.determine_start_date(source, args.from_date)
        
        # FASE 1: DOWNLOAD
        if not args.skip_download:
            print(f"\n📥 FASE 1: DOWNLOAD")
            
            downloader_script = source["downloader_script"]
            base_args = source.get("downloader_args", {})
            extra_args = {"out": str(local_source_path)}
            
            if start_date:
                extra_args["from"] = start_date.isoformat()
            
            args_str = self.build_command_args(base_args, extra_args)
            cmd = f"python {downloader_script} {args_str}"
            
            result = self.run_command(cmd, name)
            if not result.success:
                return ProcessResult(False, f"Download fallito per {name}: {result.message}")
        
        # FASE 2: UPLOAD
        if not args.skip_upload:
            print(f"\n📤 FASE 2: UPLOAD")
            
            upload_script = self.config["upload"]["script"]
            patterns = ",".join(source.get("file_patterns", ["*.pdf", "*.json"]))
            
            upload_args = {
                "src": f'"{local_source_path}"',
                "bucket": source["bucket"],
                "prefix": f'"{source.get("gcs_prefix", "")}"',
                "patterns": f'"{patterns}"'
            }
            
            if args.refresh_gcs:
                upload_args["refresh"] = ""
            
            args_str = self.build_command_args(upload_args, {})
            cmd = f"python {upload_script} {args_str}"
            
            result = self.run_command(cmd, name)
            if not result.success:
                return ProcessResult(False, f"Upload fallito per {name}: {result.message}")
        
        # FASE 3: RINOMINA
        if not args.skip_rename:
            print(f"\n🏷️  FASE 3: RINOMINA")
            
            rename_script = self.config["rename"]["script"]
            gcs_input_uri = f"gs://{source['bucket']}/{source.get('gcs_prefix', '')}"
            
            cmd = f"python {rename_script} --gcs-input {gcs_input_uri}"
            
            result = self.run_command(cmd, name)
            if not result.success:
                return ProcessResult(False, f"Rinomina fallita per {name}: {result.message}")
        
        print(f"\n✅ FONTE {name.upper()} COMPLETATA CON SUCCESSO")
        return ProcessResult(True, f"Fonte {name} processata con successo")
    
    def run(self, args) -> bool:
        """Esegue l'orchestrazione completa"""
        print("🎯 AVVIO ORCHESTRATORE UNIVERSALE")
        print(f"📋 Configurazione: {self.config_path}")
        print(f"📁 Output locale: {args.out}")
        
        # Filtra le fonti abilitate
        sources_to_run = [s for s in self.config["sources"] if s.get("enabled", False)]
        
        # Filtra per fonte specifica se richiesto
        if args.source_name:
            sources_to_run = [s for s in sources_to_run if s["name"] == args.source_name]
            if not sources_to_run:
                print(f"❌ Fonte '{args.source_name}' non trovata o non abilitata")
                return False
        
        if not sources_to_run:
            print("❌ Nessuna fonte abilitata trovata")
            return False
        
        print(f"📊 Fonti da processare: {', '.join(s['name'] for s in sources_to_run)}")
        
        # Processa ogni fonte
        results = []
        for source in sources_to_run:
            try:
                result = self.process_source(source, args)
                results.append((source["name"], result))
                
                if not result.success:
                    print(f"\n❌ ERRORE nella fonte {source['name']}: {result.message}")
                    if not args.continue_on_error:
                        return False
                        
            except Exception as e:
                error_msg = f"Errore inaspettato nella fonte {source['name']}: {e}"
                print(f"\n💥 {error_msg}")
                traceback.print_exc()
                results.append((source["name"], ProcessResult(False, error_msg)))
                
                if not args.continue_on_error:
                    return False
        
        # Riassunto finale
        print(f"\n{'='*60}")
        print("📊 RIASSUNTO FINALE")
        print(f"{'='*60}")
        
        successful = [name for name, result in results if result.success]
        failed = [name for name, result in results if not result.success]
        
        if successful:
            print(f"✅ Fonti completate: {', '.join(successful)}")
        
        if failed:
            print(f"❌ Fonti fallite: {', '.join(failed)}")
            return False
        
        print("\n🎉 ORCHESTRAZIONE COMPLETATA CON SUCCESSO!")
        return True


def main():
    parser = argparse.ArgumentParser(
        description="Orchestratore universale per ingestione documenti",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Esempi:
  # Processa tutte le fonti abilitate dalla data 2024-01-01
  python orchestrator_universal.py --from 2024-01-01
  
  # Processa solo la Camera saltando il download
  python orchestrator_universal.py --source camera --skip-download
  
  # Refresh completo di una fonte specifica
  python orchestrator_universal.py --source senato --refresh-gcs
        """
    )
    
    parser.add_argument("--config", default="config.json", type=pathlib.Path,
                       help="File di configurazione JSON (default: config.json)")
    
    parser.add_argument("--from", dest="from_date", 
                       type=lambda s: dt.datetime.strptime(s, "%Y-%m-%d").date(),
                       help="Data di partenza (YYYY-MM-DD)")
    
    parser.add_argument("--out", default="downloads", type=pathlib.Path,
                       help="Cartella locale di output (default: downloads)")
    
    parser.add_argument("--source", dest="source_name",
                       help="Processa solo questa fonte specifica")
    
    parser.add_argument("--skip-download", action="store_true",
                       help="Salta la fase di download")
    
    parser.add_argument("--skip-upload", action="store_true",
                       help="Salta la fase di upload")
    
    parser.add_argument("--skip-rename", action="store_true",
                       help="Salta la fase di rinomina")
    
    parser.add_argument("--refresh-gcs", action="store_true",
                       help="Svuota completamente GCS prima di caricare")
    
    parser.add_argument("--continue-on-error", action="store_true",
                       help="Continua anche se una fonte fallisce")
    
    parser.add_argument("--credentials", default="GOOGLE_CREDENTIALS.json",
                       help="File credenziali GCS (default: GOOGLE_CREDENTIALS.json)")
    
    args = parser.parse_args()
    
    try:
        orchestrator = UniversalOrchestrator(args.config, args.credentials)
        success = orchestrator.run(args)
        sys.exit(0 if success else 1)
        
    except Exception as e:
        print(f"\n💥 ERRORE FATALE: {e}")
        traceback.print_exc()
        sys.exit(1)


if __name__ == "__main__":
    main()