# orchestrator.py (CORRETTO)
#!/usr/bin/env python3
from __future__ import annotations
import argparse, subprocess, shlex, sys, datetime as _dt, pathlib, json, io

try:
    from google.cloud import storage
    from google.api_core import exceptions
except ImportError:
    sys.exit("ERRORE: Libreria 'google-cloud-storage' non trovata. Esegui 'pip install google-cloud-storage'")

CREDENTIALS_FILE = "GOOGLE_CREDENTIALS.json" # <--- NUOVA RIGA

def get_latest_date_from_gcs(bucket_name: str, prefix: str) -> _dt.date | None:
    print(f"  > Controllo ultima data in gs://{bucket_name}/{prefix}/ingest/metadata.jsonl...")
    try:
        # ---- MODIFICA QUI ----
        storage_client = storage.Client.from_service_account_json(CREDENTIALS_FILE)
        # ----------------------
        bucket = storage_client.bucket(bucket_name)
        metadata_blob_path = f"{prefix}/ingest/metadata.jsonl".lstrip("/")
        blob = bucket.blob(metadata_blob_path)

        if not blob.exists():
            print("  - metadata.jsonl non trovato.")
            return None

        content = blob.download_as_text()
        latest_date = None
        
        with io.StringIO(content) as f:
            for line in f:
                if not line.strip(): continue
                try:
                    record = json.loads(line)
                    if 'date' in record:
                        current_date = _dt.date.fromisoformat(record['date'])
                        if latest_date is None or current_date > latest_date:
                            latest_date = current_date
                except (json.JSONDecodeError, TypeError, ValueError):
                    continue
        
        if latest_date: print(f"  - Ultima data trovata su GCS: {latest_date.isoformat()}")
        else: print("  - Nessuna data valida trovata nel metadata.jsonl.")
        return latest_date
    except Exception as e:
        print(f"  - Errore nel leggere l'ultima data ({type(e).__name__}). Si userà la data da --from.")
        return None

# (Il resto del file rimane identico, run_command, main, etc.)
def run_command(cmd: str):
    print(f"\n┏━━━ ESEGUENDO ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━")
    print(f"┃ ▶ {cmd}")
    print(f"┗━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━")
    process = subprocess.Popen(shlex.split(cmd), stdout=subprocess.PIPE, stderr=subprocess.STDOUT, text=True, encoding='utf-8', errors='replace')
    if process.stdout:
        for line in iter(process.stdout.readline, ''): print(line, end='')
    return_code = process.wait()
    if return_code != 0:
        print(f"\n❌ ERRORE: Comando fallito con codice {return_code}. Orchestrazione interrotta.")
        sys.exit(return_code)

def main():
    parser = argparse.ArgumentParser(description="Orchestratore intelligente per ingestione dati.")
    parser.add_argument("--config", default="config.json", type=pathlib.Path, help="File di configurazione JSON.")
    parser.add_argument("--from", dest="from_date", type=lambda s: _dt.datetime.strptime(s, "%Y-%m-%d").date(), default=None, help="Data di partenza (YYYY-MM-DD). Usata solo se non trova una data valida su GCS.")
    parser.add_argument("--out", default="downloads", type=pathlib.Path, help="Cartella locale di base per tutti gli output.")
    parser.add_argument("--source", dest="source_name", default=None, help="Esegui il processo solo per una fonte specifica (es. 'camera').")
    parser.add_argument("--skip-download", action="store_true", help="Salta la fase di download.")
    parser.add_argument("--skip-upload", action="store_true", help="Salta la fase di upload.")
    parser.add_argument("--skip-rename", action="store_true", help="Salta la fase di rinomina.")
    parser.add_argument("--refresh-gcs", action="store_true", help="Svuota la cartella di destinazione su GCS prima di caricare.")
    args = parser.parse_args()

    if not args.config.exists():
        sys.exit(f"❌ ERRORE: File di configurazione '{args.config}' non trovato.")
    with open(args.config, "r", encoding="utf-8") as f:
        config = json.load(f)

    sources_to_run = [s for s in config["sources"] if s.get("enabled", False)]
    if args.source_name:
        sources_to_run = [s for s in sources_to_run if s["name"] == args.source_name]
        if not sources_to_run:
            sys.exit(f"❌ ERRORE: Fonte '{args.source_name}' non trovata o non abilitata nel config.")
    for source in sources_to_run:
        name = source["name"]
        print(f"\n\n{'='*25} AVVIO PROCESSO PER LA FONTE: {name.upper()} {'='*25}")
        local_source_path = args.out / source["local_output_subdir"]
        local_source_path.mkdir(parents=True, exist_ok=True)
        gcs_bucket = source["bucket"]
        gcs_prefix = source.get("gcs_prefix", "")

        print("\n--- 0. FASE DI DETERMINAZIONE DATA ---")
        start_date = args.from_date
        latest_gcs_date = get_latest_date_from_gcs(gcs_bucket, gcs_prefix)
        if latest_gcs_date:
            start_date = latest_gcs_date + _dt.timedelta(days=1)
            print(f"===> DATA DI PARTENZA IMPOSTATA A: {start_date.isoformat()} (dal GCS)")
        elif args.from_date:
            start_date = args.from_date
            print(f"===> DATA DI PARTENZA IMPOSTATA A: {start_date.isoformat()} (dal flag --from)")
        else:
             print(f"===> ATTENZIONE: Nessuna data trovata o specificata. Lo script di download potrebbe scaricare tutto.")
        date_arg = f"--from {start_date.isoformat()}" if start_date else ""

        if not args.skip_download:
            print(f"\n--- 1. FASE DI DOWNLOAD ---")
            downloader_script = source["downloader_script"]
            args_str = " ".join([f"--{key} {value}" for key, value in source["downloader_args"].items()])
            out_arg = f"--out {local_source_path}"
            cmd = f"python {downloader_script} {args_str} {date_arg} {out_arg}"
            run_command(cmd)
        if not args.skip_upload:
            print(f"\n--- 2. FASE DI UPLOAD ---")
            upload_script = config["upload"]["script"]
            refresh_arg = "--refresh" if args.refresh_gcs else ""
            patterns = ",".join(source.get("file_patterns", ["*.pdf", "*.json"]))
            cmd = (f'python {upload_script} --src "{local_source_path}" --bucket {gcs_bucket} --prefix "{gcs_prefix}" --patterns "{patterns}" {refresh_arg}')
            run_command(cmd)
        if not args.skip_rename:
            print(f"\n--- 3. FASE DI RINOMINA ---")
            rename_script = config["rename"]["script"]
            gcs_input_uri = f"gs://{gcs_bucket}/{gcs_prefix}"
            cmd = f"python {rename_script} --gcs-input {gcs_input_uri}"
            run_command(cmd)
        print(f"\n\n{'='*25} PROCESSO COMPLETATO PER: {name.upper()} {'='*25}")
if __name__ == "__main__":
    main()