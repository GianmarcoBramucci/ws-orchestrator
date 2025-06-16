#!/usr/bin/env python3
"""
download_camera_pdf.py - v4 "Il Pragmatico"
Basta ricerche complesse. Questo script interroga direttamente gli elenchi mensili
della Camera, un metodo molto più robusto e diretto per trovare i documenti.
"""
from __future__ import annotations
import argparse
import re
import sys
import time
import requests
import datetime as _dt
from pathlib import Path
import json
from urllib.parse import urljoin
from bs4 import BeautifulSoup

# URL DIRETTO per gli elenchi mensili. Molto più affidabile.
BASE_ARCHIVE_URL = "https://documenti.camera.it/leg/{leg}/resoconti/assemblea/elenco/{year}{month:02d}.htm"
BASE_PDF_URL_PREFIX = "https://documenti.camera.it"

session = requests.Session()
session.headers.update({"User-Agent": "Mozilla/5.0 (compatible; GringoBot/1.0)"})

def download_file(url: str, dest: Path) -> bool:
    if dest.exists():
        print(f"~ Già esiste: {dest.name}")
        return True
    try:
        r = session.get(url, timeout=60)
        r.raise_for_status()
        dest.parent.mkdir(parents=True, exist_ok=True)
        dest.write_bytes(r.content)
        print(f"✓ Scaricato: {dest.name}")
        return True
    except requests.RequestException as e:
        print(f"✗ Fallito download di {url}: {e}")
        return False

def main(argv=None):
    parser = argparse.ArgumentParser()
    parser.add_argument("--leg", type=int, required=True, help="Numero legislatura (es. 19)")
    parser.add_argument("--from", dest="from_date", type=lambda s: _dt.datetime.strptime(s, "%Y-%m-%d").date(), default=_dt.date.min, help="Data minima da cui scaricare.")
    parser.add_argument("--out", type=Path, required=True, help="Cartella di output.")
    args = parser.parse_args(argv)

    start_date = args.from_date
    end_date = _dt.date.today()
    
    print(f"Ricerca documenti da {start_date.isoformat()} a {end_date.isoformat()} per la legislatura {args.leg}...")

    # Itera attraverso ogni mese nel range di date
    current_date = start_date
    while current_date <= end_date:
        year, month = current_date.year, current_date.month
        
        print(f"\n--- Controllo Mese: {year}-{month:02d} ---")
        
        archive_url = BASE_ARCHIVE_URL.format(leg=args.leg, year=year, month=month)
        try:
            r = session.get(archive_url, timeout=20)
            if r.status_code == 404:
                print(f"Nessun elenco trovato per questo mese. Salto.")
                # Avanza al prossimo mese
                next_month_year = year + (month // 12)
                next_month = (month % 12) + 1
                current_date = _dt.date(next_month_year, next_month, 1)
                time.sleep(0.5)
                continue
            r.raise_for_status()
        except requests.RequestException as e:
            print(f"Errore nel recuperare l'elenco mensile {archive_url}: {e}")
            # Avanza comunque per non rimanere bloccato
            next_month_year = year + (month // 12)
            next_month = (month % 12) + 1
            current_date = _dt.date(next_month_year, next_month, 1)
            continue

        soup = BeautifulSoup(r.text, 'html.parser')
        
        # Cerca i link ai resoconti
        links = soup.select('a[href*="/resoconti/assemblea/html/sed"]')
        
        if not links:
            print("Nessun link a resoconti trovato in questo mese.")
        
        for link in links:
            # Estrae la data dal testo del link, es: "03/05"
            match = re.search(r'(\d{1,2})\/(\d{1,2})', link.get_text(strip=True))
            if not match:
                continue
                
            day = int(match.group(1))
            doc_date = _dt.date(year, month, day)

            if doc_date >= start_date:
                # Estrae il numero della seduta dal link
                href = link.get('href')
                sed_match = re.search(r'sed(\d+)', href)
                if not sed_match:
                    continue
                sed_num = int(sed_match.group(1))

                # Costruisce l'URL del PDF stenografico
                pdf_url = urljoin(BASE_PDF_URL_PREFIX, f"/leg{args.leg}/resoconti/assemblea/html/sed{sed_num:04d}/stenografico.pdf")
                
                pdf_path = args.out / str(year) / f"camera_leg{args.leg}_sed{sed_num:04d}_{doc_date.isoformat()}.pdf"
                
                if download_file(pdf_url, pdf_path):
                    # Crea il sidecar JSON
                    meta = {
                        "date": doc_date.isoformat(),
                        "legislatura": args.leg,
                        "seduta": sed_num,
                        "source": "camera"
                    }
                    meta_path = pdf_path.with_suffix(".json")
                    with open(meta_path, 'w', encoding='utf-8') as f:
                        json.dump(meta, f, ensure_ascii=False, indent=2)
            time.sleep(0.2) # Piccolo delay per cortesia

        # Passa al mese successivo
        next_month_year = year + (month // 12)
        next_month = (month % 12) + 1
        current_date = _dt.date(next_month_year, next_month, 1)
        time.sleep(1) # Delay più lungo tra un mese e l'altro

    print("\nDownload per la Camera completato.")


if __name__ == "__main__":
    main()