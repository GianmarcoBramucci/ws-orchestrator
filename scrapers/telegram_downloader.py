#!/usr/bin/env python3
"""
telegram_downloader.py – v5.0 GCS DUPLICATE CHECK
==========================
• Formato uniforme (.txt + .json per messaggio)
• Fix timezone‑aware / naive
• Fix Windows encoding per emoji/Unicode
• CONTROLLO DUPLICATI: Legge batch.jsonl da GCS per evitare ri-download
• Campi metadata extra:
    - source_type ("telegram")
    - video_id  ➜ id numerico del messaggio (la parte finale di https://t.me/<channel>/<id>)
    - facebook_url estratto da "Link post Fb: …" se presente
"""

from __future__ import annotations
import os
import re
import argparse
import datetime as dt
import json
import asyncio
import io
from pathlib import Path
from typing import Optional, Set

from dotenv import load_dotenv
from telethon import TelegramClient
from telethon.errors import SessionPasswordNeededError

# Import GCS lazy
try:
    from google.cloud import storage
    from google.auth.exceptions import DefaultCredentialsError
except ImportError:
    storage = None

load_dotenv()

DEFAULT_CHANNEL = "fdiufficiale"
CREDENTIALS_FILE = "GOOGLE_CREDENTIALS.json"

# ────────────────────────────────────────────────────────────────
# Helper
# ────────────────────────────────────────────────────────────────

def safe_print(msg: str):
    """Print sicuro per Windows e Unicode"""
    try:
        print(msg)
    except UnicodeEncodeError:
        ascii_msg = msg.encode('ascii', 'replace').decode('ascii')
        print(ascii_msg)


def parse_iso(date_str: str) -> dt.datetime:
    return dt.datetime.strptime(date_str, "%Y-%m-%d")


def sanitize_fragment(text: str, max_len: int = 50) -> str:
    frag = text.strip().split("\n", 1)[0][:max_len]
    frag = re.sub(r"[^\w\s-]", "", frag)
    frag = re.sub(r"\s+", "_", frag).strip("_")
    return frag or "msg"

# Regex per "Link post Fb:" / "Link post FB:"
FB_LINK_RE = re.compile(r"Link\s+post\s+F[BB]?:?\s*(https?://\S+)", re.IGNORECASE)


def get_existing_video_ids_from_gcs(bucket_name: str, gcs_prefix: str, channel: str) -> Set[str]:
    """
    Legge batch.jsonl da GCS e estrae tutti i video_id già presenti per il canale Telegram specificato
    """
    if not storage:
        safe_print("⚠️  google-cloud-storage non installato, skip controllo duplicati")
        return set()
    
    existing_ids = set()
    
    try:
        # Inizializza client GCS
        if Path(CREDENTIALS_FILE).exists():
            client = storage.Client.from_service_account_json(CREDENTIALS_FILE)
            safe_print(f"🔑 GCS client con credenziali {CREDENTIALS_FILE}")
        else:
            client = storage.Client()
            safe_print("🔑 GCS client con credenziali di default")
        
        # Path del batch.jsonl
        batch_blob_path = f"{gcs_prefix}/ingest/batch.jsonl".lstrip("/")
        safe_print(f"🔍 Controllo duplicati: gs://{bucket_name}/{batch_blob_path}")
        
        bucket = client.bucket(bucket_name)
        blob = bucket.blob(batch_blob_path)
        
        if not blob.exists():
            safe_print("📝 batch.jsonl non trovato - primo caricamento")
            return set()
        
        # Scarica e processa il batch.jsonl
        content = blob.download_as_text()
        processed_records = 0
        telegram_records = 0
        
        with io.StringIO(content) as f:
            for line_num, line in enumerate(f, 1):
                if not line.strip():
                    continue
                    
                try:
                    record = json.loads(line)
                    processed_records += 1
                    
                    # Filtra solo record Telegram del canale specifico
                    struct_data = record.get('structData', {})
                    if (struct_data.get('source_type') == 'telegram' and 
                        struct_data.get('video_id') and
                        channel in record.get('content', {}).get('uri', '')):
                        
                        video_id = str(struct_data['video_id'])
                        existing_ids.add(video_id)
                        telegram_records += 1
                        
                except (json.JSONDecodeError, KeyError, TypeError) as e:
                    safe_print(f"⚠️  Record malformato alla riga {line_num}: {e}")
                    continue
        
        safe_print(f"📊 Processati {processed_records} record totali")
        safe_print(f"📱 Trovati {telegram_records} record Telegram per {channel}")
        safe_print(f"🔒 Video ID da saltare: {len(existing_ids)}")
        
        return existing_ids
        
    except Exception as e:
        safe_print(f"❌ Errore controllo duplicati GCS: {e}")
        safe_print("⚠️  Continuo senza controllo duplicati")
        return set()


async def fetch_messages(
    api_id: int,
    api_hash: str,
    session: Path,
    channel: str,
    out_dir: Path,
    from_dt: Optional[dt.datetime],
    to_dt: Optional[dt.datetime],
    bucket_name: Optional[str] = None,
    gcs_prefix: Optional[str] = "",
) -> None:
    out_dir.mkdir(parents=True, exist_ok=True)

    # CONTROLLO DUPLICATI: Leggi video_id esistenti da GCS
    existing_ids = set()
    if bucket_name:
        existing_ids = get_existing_video_ids_from_gcs(bucket_name, gcs_prefix, channel)
    else:
        safe_print("⚠️  Bucket GCS non specificato, skip controllo duplicati")

    async with TelegramClient(str(session), api_id, api_hash) as client:
        # ── Login (una tantum) ──
        if not await client.is_user_authorized():
            phone = os.getenv("TELEGRAM_PHONE")
            if not phone:
                raise SystemExit("TELEGRAM_PHONE non definita per la prima autenticazione")
            await client.send_code_request(phone)
            code = input(f"Inserisci il codice inviato a {phone}: ")
            try:
                await client.sign_in(phone, code)
            except SessionPasswordNeededError:
                pwd = input("Password 2‑step Telegram: ")
                await client.sign_in(password=pwd)

        query_kwargs = {}
        if to_dt:
            query_kwargs["offset_date"] = to_dt + dt.timedelta(days=1)

        total = 0
        skipped = 0
        async for msg in client.iter_messages(channel, **query_kwargs):
            msg_dt_naive = msg.date.replace(tzinfo=None)
            if from_dt and msg_dt_naive < from_dt:
                break
            if to_dt and msg_dt_naive > to_dt:
                continue
            if not msg.message:
                continue

            # CONTROLLO DUPLICATI: Skip se già processato
            if str(msg.id) in existing_ids:
                skipped += 1
                continue

            local_dt = msg.date.astimezone()
            subdir = out_dir / str(local_dt.year) / f"{local_dt.month:02d}"
            subdir.mkdir(parents=True, exist_ok=True)

            base = f"{local_dt.date()}_{msg.id}_{sanitize_fragment(msg.message)}"
            txt_path = subdir / f"{base}.txt"
            json_path = subdir / f"{base}.json"

            if not txt_path.exists():
                txt_path.write_text(msg.message, encoding="utf-8")

            # video_id = id numerico del messaggio (parte finale dell'URL)
            video_id = str(msg.id)

            # facebook_url se presente nel testo
            fb_url = None
            m = FB_LINK_RE.search(msg.message)
            if m:
                fb_url = m.group(1).strip()

            metadata = {
                "id": msg.id,
                "date": msg.date.isoformat(),
                "views": msg.views,
                "forwards": msg.forwards,
                "reply_count": msg.replies.replies if msg.replies else None,
                "url": f"https://t.me/{channel}/{msg.id}",
                "source_type": "telegram",
                "video_id": video_id,
                "facebook_url": fb_url,
                "text_file": str(txt_path.relative_to(out_dir)),
            }
            json_path.write_text(json.dumps(metadata, ensure_ascii=False, indent=2), encoding="utf-8")
            total += 1

        # Report finale
        safe_print(f"SUCCESS: Salvati {total} nuovi messaggi, {skipped} saltati (duplicati)")
        safe_print(f"Directory: {out_dir}")


# ────────────────────────────────────────────────────────────────
# CLI
# ────────────────────────────────────────────────────────────────

def main():
    p = argparse.ArgumentParser(
        "Telegram text+metadata downloader con controllo duplicati GCS",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Esempi:
  # Download normale
  python telegram_downloader.py --out downloads/telegram --channel fdiufficiale
  
  # Con controllo duplicati GCS
  python telegram_downloader.py --out downloads/telegram --channel fdiufficiale --bucket documenti_fdi --gcs-prefix telegram
  
  # Range di date specifico
  python telegram_downloader.py --out downloads/telegram --from 2025-06-01 --to 2025-06-24
        """
    )
    p.add_argument("--out", required=True, help="Directory di output locale")
    p.add_argument("--from", dest="from_date", help="Data iniziale (YYYY-MM-DD)")
    p.add_argument("--to", dest="to_date", help="Data finale (YYYY-MM-DD)")
    p.add_argument("--channel", default=DEFAULT_CHANNEL, help=f"Canale Telegram (default: {DEFAULT_CHANNEL})")
    
    # Argomenti per controllo duplicati GCS
    p.add_argument("--bucket", help="Nome bucket GCS per controllo duplicati")
    p.add_argument("--gcs-prefix", default="", help="Prefisso GCS (default: root)")
    
    args = p.parse_args()

    try:
        api_id = int(os.getenv("TELEGRAM_API_ID"))
        api_hash = os.getenv("TELEGRAM_API_HASH")
    except (TypeError, ValueError):
        raise SystemExit("TELEGRAM_API_ID/HASH mancanti o invalidi nel .env")

    session = Path(os.getenv("TELEGRAM_SESSION_FILE", "telegram_fdi.session"))
    from_dt = parse_iso(args.from_date) if args.from_date else None
    to_dt = parse_iso(args.to_date) if args.to_date else None

    asyncio.run(
        fetch_messages(
            api_id, 
            api_hash, 
            session, 
            args.channel, 
            Path(args.out), 
            from_dt, 
            to_dt,
            args.bucket,
            args.gcs_prefix
        )
    )


if __name__ == "__main__":
    main()