#!/usr/bin/env python3
"""
download_senato_pdf.py - v7 "Basato sul tuo script funzionante"
===============================================================
Mantiene ESATTAMENTE la logica del tuo scarica_senato_pdf.py funzionante,
solo con interfaccia --leg --from --out per l'orchestratore.
"""
from __future__ import annotations
import argparse
import datetime as dt
import json
import random
import re
import sys
import time
from pathlib import Path
from typing import List, Optional, Tuple
from urllib.parse import urljoin

import requests
from bs4 import BeautifulSoup

# ――― Config (identica al tuo script) ―――
DELAY_HTML = 10.0    # rispetto robots.txt
DELAY_PDF = 1.5
JITTER_HTML = 1.5
JITTER_PDF = 0.8
RETRIES = 3          # tentativi per PDF
TIMEOUT_HTML = 20
TIMEOUT_PDF = 60     # read‑timeout per i PDF (alcuni > 50 MB)
BACKOFF = 15         # s tra retry

BASE_TEMPLATE = (
    "https://www.senato.it/legislature/{leg}/lavori/assemblea/"
    "resoconti-elenco-cronologico?year={year}"
)
BASE_TEMPLATE_ATTUALE = (
    "https://www.senato.it/lavori/assemblea/"
    "resoconti-elenco-cronologico?year={year}"
)

HEADERS = {"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
           "(KHTML, like Gecko) Chrome/123.0 Safari/537.36"}

DATE_RE = re.compile(
    r"dal\s+(\d{1,2}\s+\w+\s+\d{4})(?:\s+al\s+(\d{1,2}\s+\w+\s+\d{4}))?",
    re.IGNORECASE,
)

# ――― HTTP session ―――
session = requests.Session()
session.headers.update(HEADERS)

# ――― Helpers (identici al tuo script) ―――

def _sleep(base: float, jitter: float):
    time.sleep(base + random.uniform(0, jitter))

def _ita_date_year(s: str) -> int:
    return int(s.split()[-1])

def _extract_years(html: str) -> Optional[Tuple[int, Optional[int]]]:
    m = DATE_RE.search(html)
    if not m:
        return None
    return _ita_date_year(m.group(1)), (_ita_date_year(m.group(2)) if m.group(2) else None)

# ――― Core functions (identiche al tuo script) ―――

def parse_pdf_links(leg: str, year: int) -> List[str]:
    _sleep(DELAY_HTML, JITTER_HTML)
    
    # USA TEMPLATE ATTUALE se siamo nell'anno corrente, altrimenti quello storico
    current_year = dt.datetime.now().year
    if year == current_year:
        url = BASE_TEMPLATE_ATTUALE.format(year=year)
        print(f"    🔄 Usando template ATTUALE per anno {year}")
    else:
        url = BASE_TEMPLATE.format(leg=leg, year=year)
        print(f"    📜 Usando template STORICO per anno {year}")
    
    r = session.get(url, timeout=TIMEOUT_HTML)
    if r.status_code == 404:
        return []
    r.raise_for_status()
    soup = BeautifulSoup(r.text, "html.parser")
    return [urljoin("https://www.senato.it/", a["href"]) for a in soup.select('a[href$=".pdf"]')]

def download_pdf(url: str, dest: Path, overwrite: bool = False):
    if dest.exists() and not overwrite:
        print(f"  ✓ Già esistente: {dest.name}")
        return
    
    dest.parent.mkdir(parents=True, exist_ok=True)

    for attempt in range(1, RETRIES + 1):
        try:
            print(f"  ⬇️  Scaricando: {dest.name} (tentativo {attempt})...")
            with session.get(url, stream=True, timeout=TIMEOUT_PDF) as r:
                r.raise_for_status()
                tmp = dest.with_suffix(".part")
                with open(tmp, "wb") as f:
                    for chunk in r.iter_content(8192):
                        f.write(chunk)
                tmp.rename(dest)
            print(f"  ✅ Completato: {dest.name}")
            break  # ok!
        except (requests.exceptions.ReadTimeout, requests.exceptions.ConnectionError) as e:
            if attempt == RETRIES:
                print(f"  ❌ Fallito dopo {RETRIES} tentativi: {dest.name}")
                raise
            print(f"       timeout, retry {attempt}/{RETRIES} fra {BACKOFF}s …")
            time.sleep(BACKOFF)
    _sleep(DELAY_PDF, JITTER_PDF)

def get_year_bounds(leg: str, hint: int) -> Tuple[int, Optional[int]]:
    # USA TEMPLATE ATTUALE se siamo nell'anno corrente, altrimenti quello storico
    current_year = dt.datetime.now().year
    if hint == current_year:
        url = BASE_TEMPLATE_ATTUALE.format(year=hint)
    else:
        url = BASE_TEMPLATE.format(leg=leg, year=hint)
        
    try:
        r = session.get(url, timeout=TIMEOUT_HTML)
        if r.status_code != 200:
            return hint, None
        yrs = _extract_years(r.text)
        return yrs if yrs else (hint, None)
    except requests.exceptions.RequestException:
        return hint, None

def create_metadata_file(pdf_path: Path, leg: str) -> bool:
    """Crea il file di metadata JSON"""
    try:
        # Estrae info dal path: legislatura_XX/YYYY/filename.pdf
        parts = pdf_path.parts
        year = None
        if len(parts) >= 2:
            try:
                year = int(parts[-2])  # Directory anno
            except ValueError:
                pass
        
        metadata = {
            "legislatura": leg,
            "source": "senato",
            "document_type": "stenographic_report",
            "institution": "senato_repubblica", 
            "language": "it",
            "created_at": dt.datetime.now(dt.timezone.utc).isoformat()
        }
        
        if year:
            metadata["year"] = year
        
        metadata_path = pdf_path.with_suffix(".json")
        with open(metadata_path, "w", encoding="utf-8") as f:
            json.dump(metadata, f, ensure_ascii=False, indent=2)
        
        return True
        
    except Exception as e:
        print(f"  ⚠️  Errore metadata: {e}")
        return False

# ――― Logica principale (adattata dal tuo fetch_legislature) ―――

def fetch_legislature_with_date_filter(leg: str, y_start: int, y_end_in: Optional[int], 
                                     start_date: Optional[dt.date], output_dir: Path):
    """Versione del tuo fetch_legislature con filtro data e output dir"""
    
    auto_start, auto_end = get_year_bounds(leg, y_start)
    y_start = auto_start or y_start
    y_end = y_end_in if y_end_in is not None else (auto_end or dt.datetime.now().year)

    print(f"🏛️  Senato Legislatura {leg}: {y_start}-{y_end}")
    
    total_downloaded = 0
    total_errors = 0

    for yr in range(y_start, y_end + 1):
        # Se abbiamo una data di filtro e l'anno è troppo vecchio, salta
        if start_date and yr < start_date.year:
            print(f"  📅 Anno {yr}: troppo vecchio, saltato")
            continue
            
        links = parse_pdf_links(leg, yr)
        if not links:
            print(f"  📭 {yr}: nessun pdf — stop.")
            break
            
        print(f"  📄 {yr}: {len(links)} pdf")
        
        year_downloads = 0
        year_errors = 0
        
        for link in links:
            fname = link.rsplit("/", 1)[1]
            # Usa la STESSA struttura del tuo script: legislatura_XX/anno/file.pdf
            path = output_dir / f"legislatura_{leg}" / str(yr) / fname
            
            try:
                download_pdf(link, path)
                # Crea metadata
                create_metadata_file(path, leg)
                year_downloads += 1
                total_downloaded += 1
            except Exception as exc:
                print(f"    ❌ ERRORE {fname}: {exc}")
                year_errors += 1
                total_errors += 1
        
        print(f"  📊 Anno {yr}: {year_downloads} scaricati, {year_errors} errori")
    
    print(f"\n🏁 COMPLETATO - Totale: {total_downloaded} scaricati, {total_errors} errori")
    return total_errors == 0

# ――― Interfaccia orchestratore ―――

def main():
    parser = argparse.ArgumentParser(
        description="Scarica resoconti stenografici del Senato (basato su script funzionante)",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Esempi:
  python download_senato_pdf.py --leg 19 --from 2024-01-01 --out ./downloads
  python download_senato_pdf.py --leg 19 --out ./downloads
        """
    )
    
    parser.add_argument("--leg", required=True, 
                       help="Numero della legislatura (es. 19)")
    
    parser.add_argument("--from", dest="from_date", 
                       type=lambda s: dt.datetime.strptime(s, "%Y-%m-%d").date(),
                       help="Data minima da cui scaricare (YYYY-MM-DD) - opzionale")
    
    parser.add_argument("--out", type=Path, required=True,
                       help="Cartella di output")
    
    args = parser.parse_args()
    
    try:
        # Usa la logica del tuo script: determina automaticamente gli anni
        # ma filtra per data se fornita
        start_year = args.from_date.year if args.from_date else 2000
        
        success = fetch_legislature_with_date_filter(
            args.leg, 
            start_year, 
            None,  # Auto-detect end year
            args.from_date,
            args.out
        )
        
        if success:
            print("\n🎉 Download completato con successo!")
            sys.exit(0)
        else:
            print("\n⚠️  Download completato con alcuni errori")
            sys.exit(1)
            
    except KeyboardInterrupt:
        print("\n🛑 Download interrotto dall'utente")
        sys.exit(130)
        
    except Exception as e:
        print(f"\n💥 Errore fatale: {e}")
        import traceback
        traceback.print_exc()
        sys.exit(1)

if __name__ == "__main__":
    main()