#!/usr/bin/env python3
"""
orchestrator.py - Sistema di Orchestrazione Universale v3.0 - Multi-Legislature Edition
=======================================================================================
Sistema generico per scaricare, processare e caricare documenti da qualsiasi fonte.
Supporta multi-legislature e rename configurabile per fonte.
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
import time
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
        self.session_start = dt.datetime.now()
        
    def _load_config(self) -> Dict:
        """Carica e valida la configurazione"""
        if not self.config_path.exists():
            raise FileNotFoundError(f"File di configurazione non trovato: {self.config_path}")
        
        try:
            with open(self.config_path, "r", encoding="utf-8") as f:
                config = json.load(f)
        except json.JSONDecodeError as e:
            raise ValueError(f"File di configurazione malformato: {e}")
        
        # Validazione configurazione base
        required_keys = ["sources", "upload", "rename"]
        for key in required_keys:
            if key not in config:
                raise ValueError(f"Chiave mancante nella configurazione: {key}")
        
        # Validazione fonti
        for source in config["sources"]:
            required_source_keys = ["name", "downloader_script", "bucket"]
            for key in required_source_keys:
                if key not in source:
                    raise ValueError(f"Chiave mancante nella fonte '{source.get('name', 'unknown')}': {key}")
        
        return config
    
    def _init_storage_client(self):
        """Inizializza il client Google Cloud Storage"""
        if self.storage_client is None:
            try:
                if pathlib.Path(self.credentials_file).exists():
                    self.storage_client = storage.Client.from_service_account_json(self.credentials_file)
                    print(f"🔑 Usando credenziali da: {self.credentials_file}")
                else:
                    # Fallback su default credentials
                    self.storage_client = storage.Client()
                    print("🔑 Usando credenziali di default di Google Cloud")
            except Exception as e:
                raise RuntimeError(f"Impossibile inizializzare GCS client: {e}")
    
    def get_latest_date_from_gcs(self, bucket_name: str, prefix: str) -> Optional[dt.date]:
        """Ottiene l'ultima data processata da GCS con migliore error handling"""
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
            valid_records = 0
            
            with io.StringIO(content) as f:
                for line_num, line in enumerate(f, 1):
                    if not line.strip():
                        continue
                    try:
                        record = json.loads(line)
                        valid_records += 1
                        
                        # Cerca data in diverse posizioni
                        date_str = None
                        if 'date' in record:
                            date_str = record['date']
                        elif 'structData' in record and 'date' in record['structData']:
                            date_str = record['structData']['date']
                        
                        if date_str:
                            try:
                                current_date = dt.date.fromisoformat(date_str)
                                if latest_date is None or current_date > latest_date:
                                    latest_date = current_date
                            except ValueError:
                                print(f"  ⚠️  Data malformata alla riga {line_num}: {date_str}")
                                
                    except (json.JSONDecodeError, TypeError, ValueError) as e:
                        print(f"  ⚠️  Record malformato alla riga {line_num}: {e}")
                        continue
            
            print(f"  📊 Elaborati {valid_records} record validi")
            if latest_date:
                print(f"  📅 Ultima data trovata: {latest_date.isoformat()}")
            else:
                print("  📝 Nessuna data valida nel metadata.jsonl")
            
            return latest_date
            
        except Exception as e:
            print(f"  ❌ Errore nel leggere metadata: {type(e).__name__}: {e}")
            return None
    
    def run_command(self, cmd: str, source_name: str = "", timeout: int = 3600) -> ProcessResult:
        """Esegue un comando con timeout e logging migliorato"""
        display_name = f" [{source_name}]" if source_name else ""
        
        print(f"\n┏━━━ COMANDO{display_name} ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━")
        print(f"┃ ▶ {cmd}")
        print(f"┗━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━")
        
        start_time = time.time()
        
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
            
            return_code = process.wait(timeout=timeout)
            execution_time = time.time() - start_time
            
            if return_code == 0:
                print(f"\n✅ Comando completato in {execution_time:.1f}s")
                return ProcessResult(True, "Comando completato con successo", 
                                   {"output": output_lines, "execution_time": execution_time})
            else:
                print(f"\n❌ Comando fallito con codice {return_code} dopo {execution_time:.1f}s")
                return ProcessResult(False, f"Comando fallito con codice {return_code}")
                
        except subprocess.TimeoutExpired:
            process.kill()
            error_msg = f"Comando interrotto per timeout ({timeout}s)"
            print(f"\n⏰ {error_msg}")
            return ProcessResult(False, error_msg)
            
        except Exception as e:
            execution_time = time.time() - start_time
            error_msg = f"Errore nell'esecuzione del comando dopo {execution_time:.1f}s: {e}"
            print(f"\n💥 {error_msg}")
            return ProcessResult(False, error_msg)
    
    def determine_start_date(self, source: Dict, explicit_date: Optional[dt.date]) -> Optional[dt.date]:
        """Determina la data di partenza per una fonte con logging migliorato"""
        if explicit_date:
            print(f"  📅 Data esplicita fornita: {explicit_date.isoformat()}")
            return explicit_date
        
        # Controlla ultima data su GCS
        gcs_bucket = source["bucket"]
        gcs_prefix = source.get("gcs_prefix", "")
        
        latest_gcs_date = self.get_latest_date_from_gcs(gcs_bucket, gcs_prefix)
        
        if latest_gcs_date:
            start_date = latest_gcs_date + dt.timedelta(days=1)
            print(f"  📈 Data di partenza (da GCS + 1 giorno): {start_date.isoformat()}")
            return start_date
        
        # Fallback su configurazione o data minima
        fallback_date = source.get("default_start_date")
        if fallback_date:
            try:
                fallback = dt.date.fromisoformat(fallback_date)
                print(f"  📋 Data di partenza (fallback config): {fallback.isoformat()}")
                return fallback
            except ValueError:
                print(f"  ⚠️  Data fallback malformata: {fallback_date}")
        
        print("  ⚠️  Nessuna data di partenza determinata")
        return None
    
    def build_command_args(self, base_args: Dict, extra_args: Dict) -> str:
        """Costruisce gli argomenti per un comando"""
        all_args = {**base_args, **extra_args}
        args_list = []
        
        for key, value in all_args.items():
            if value == "":  # Flag senza valore
                args_list.append(f"--{key}")
            else:
                args_list.append(f"--{key} {value}")
        
        return " ".join(args_list)
    
    def process_source(self, source: Dict, args) -> ProcessResult:
        """Processa una singola fonte con miglior gestione errori"""
        name = source["name"]
        print(f"\n{'='*60}")
        print(f"🚀 PROCESSANDO FONTE: {name.upper()}")
        print(f"{'='*60}")
        
        # Validazione preliminare
        script_path = pathlib.Path(source["downloader_script"])
        if not script_path.exists():
            return ProcessResult(False, f"Script downloader non trovato: {script_path}")
        
        # Preparazione directory locale
        base_dir = pathlib.Path(args.out)
        subdir = source.get("local_output_subdir", name)
        
        # Path joining sicuro usando l'operatore /
        local_source_path = base_dir / subdir
        
        # Crea directory con path sicuro
        try:
            local_source_path.mkdir(parents=True, exist_ok=True)
            print(f"📁 Directory confermata: {local_source_path.resolve()}")
        except Exception as e:
            return ProcessResult(False, f"Impossibile creare directory {local_source_path}: {e}")
        
        # Determinazione data di partenza
        start_date = self.determine_start_date(source, args.from_date)
        
        # FASE 1: DOWNLOAD
        if not args.skip_download:
            print(f"\n📥 FASE 1: DOWNLOAD")
            
            downloader_script = source["downloader_script"]
            base_args = source.get("downloader_args", {})
            
            # Passa path assoluto come stringa
            path_for_cmd = str(local_source_path.resolve()).replace("\\", "/")
            extra_args = {"out": f'"{path_for_cmd}"'}
            
            if start_date:
                extra_args["from"] = start_date.isoformat()
            
            if args.to_date:
                extra_args["to"] = args.to_date.isoformat()
            
            args_str = self.build_command_args(base_args, extra_args)
            cmd = f"python {downloader_script} {args_str}"
            
            # Timeout personalizzato per download
            timeout = self.config.get("global_settings", {}).get("default_timeout_seconds", 3600)
            
            result = self.run_command(cmd, name, timeout)
            if not result.success:
                return ProcessResult(False, f"Download fallito per {name}: {result.message}")
        else:
            print(f"\n⏭️  FASE 1: DOWNLOAD SALTATO")
        
        # FASE 2: UPLOAD
        if not args.skip_upload:
            print(f"\n📤 FASE 2: UPLOAD")
            
            upload_script = self.config["upload"]["script"]
            patterns = ",".join(source.get("file_patterns", ["*.pdf", "*.json"]))
            
            # Usa path assoluto come stringa
            path_for_cmd = str(local_source_path.resolve()).replace("\\", "/")
            upload_args = {
                "src": f'"{path_for_cmd}"',
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
        else:
            print(f"\n⏭️  FASE 2: UPLOAD SALTATO")
        
        # FASE 3: RINOMINA
        if not args.skip_rename and source.get("enable_rename", False):
            print(f"\n🏷️  FASE 3: RINOMINA")
            
            rename_script = self.config["rename"]["script"]
            gcs_input_uri = f"gs://{source['bucket']}/{source.get('gcs_prefix', '')}"
            
            cmd = f"python {rename_script} --gcs-input {gcs_input_uri}"
            
            result = self.run_command(cmd, name)
            if not result.success:
                return ProcessResult(False, f"Rinomina fallita per {name}: {result.message}")
        else:
            if args.skip_rename:
                print(f"\n⏭️  FASE 3: RINOMINA SALTATA (--skip-rename)")
            elif not source.get("enable_rename", False):
                print(f"\n⏭️  FASE 3: RINOMINA DISABILITATA NEL CONFIG")
        
        print(f"\n✅ FONTE {name.upper()} COMPLETATA CON SUCCESSO")
        return ProcessResult(True, f"Fonte {name} processata con successo")
    
    def run(self, args) -> bool:
        """Esegue l'orchestrazione completa con summary dettagliato"""
        session_start = time.time()
        
        print("🎯 AVVIO ORCHESTRATORE UNIVERSALE v3.0 - MULTI-LEGISLATURE EDITION")
        print(f"📋 Configurazione: {self.config_path}")
        print(f"📁 Output locale: {args.out}")
        print(f"🕒 Avvio: {self.session_start.strftime('%Y-%m-%d %H:%M:%S')}")
        
        # Filtra le fonti abilitate
        sources_to_run = [s for s in self.config["sources"] if s.get("enabled", False)]
        
        # Filtra per fonte specifica se richiesto
        if args.source_name:
            sources_to_run = [s for s in sources_to_run if s["name"] == args.source_name]
            if not sources_to_run:
                print(f"❌ Fonte '{args.source_name}' non trovata o non abilitata")
                available_sources = [s["name"] for s in self.config["sources"]]
                print(f"💡 Fonti disponibili: {', '.join(available_sources)}")
                return False
        
        if not sources_to_run:
            print("❌ Nessuna fonte abilitata trovata")
            return False
        
        print(f"📊 Fonti da processare: {', '.join(s['name'] for s in sources_to_run)}")
        
        # Processa ogni fonte
        results = []
        for i, source in enumerate(sources_to_run, 1):
            try:
                print(f"\n🔄 FONTE {i}/{len(sources_to_run)}")
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
        
        # Summary finale dettagliato
        execution_time = time.time() - session_start
        
        print(f"\n{'='*60}")
        print("📊 RIASSUNTO FINALE")
        print(f"{'='*60}")
        
        successful = [name for name, result in results if result.success]
        failed = [name for name, result in results if not result.success]
        
        print(f"⏱️  Tempo totale: {execution_time:.1f}s")
        print(f"📈 Fonti processate: {len(results)}")
        
        if successful:
            print(f"✅ Fonti completate ({len(successful)}): {', '.join(successful)}")
        
        if failed:
            print(f"❌ Fonti fallite ({len(failed)}): {', '.join(failed)}")
            print(f"\n💡 Usa --continue-on-error per continuare anche in caso di errori")
            return False
        
        print(f"\n🎉 ORCHESTRAZIONE COMPLETATA CON SUCCESSO!")
        print(f"🕒 Completata: {dt.datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
        return True


def main():
    parser = argparse.ArgumentParser(
        description="Orchestratore universale per ingestione documenti v3.0",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Esempi:
  # Processa tutte le fonti abilitate dalla data 2024-01-01
  python orchestrator.py --from 2024-01-01
  
  # Processa solo la Camera saltando il download
  python orchestrator.py --source camera --skip-download
  
  # Refresh completo di una fonte specifica
  python orchestrator.py --source senato --refresh-gcs
  
  # Processa tutto continuando anche in caso di errori
  python orchestrator.py --continue-on-error
  
  # Range di date specifico
  python orchestrator.py --from 2020-01-01 --to 2023-12-31
        """
    )
    
    parser.add_argument("--config", default="config.json", type=pathlib.Path,
                       help="File di configurazione JSON (default: config.json)")
    
    parser.add_argument("--from", dest="from_date", 
                       type=lambda s: dt.datetime.strptime(s, "%Y-%m-%d").date(),
                       help="Data di partenza (YYYY-MM-DD)")
    
    parser.add_argument("--to", dest="to_date",
                       type=lambda s: dt.datetime.strptime(s, "%Y-%m-%d").date(),
                       help="Data di fine (YYYY-MM-DD)")
    
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
    
    # Forza args.out come Path object
    args.out = pathlib.Path(args.out).resolve()
    print(f"📁 Directory output normalizzata: {args.out}")
    
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