#!/usr/bin/env python3
"""
download_senato_pdf.py - v10.0 "SIMPLE BUT SMART"
=================================================
Basta cazzate, logica semplice ma efficace:
1. Input: legislatura di partenza + range date
2. Testa legislature una per una (indietro/avanti) fino a coprire il range
3. Quando trova una che funziona, la scarica TUTTA per quegli anni
4. Zero discovery complicata, zero hardcode, zero rotture di cazzo
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
from typing import List, Optional, Tuple, Set
from urllib.parse import urljoin

import requests
from bs4 import BeautifulSoup

# Config semplice
DELAY_HTML = 1.5
DELAY_PDF = 1.5  
JITTER_HTML = 1.5
JITTER_PDF = 0.8
RETRIES = 3
TIMEOUT_HTML = 5
TIMEOUT_PDF = 5
BACKOFF = 15

BASE_TEMPLATE = "https://www.senato.it/legislature/{leg}/lavori/assemblea/resoconti-elenco-cronologico?year={year}"
BASE_TEMPLATE_ATTUALE = "https://www.senato.it/lavori/assemblea/resoconti-elenco-cronologico?year={year}"

HEADERS = {"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36"}

# HTTP session
session = requests.Session()
session.headers.update(HEADERS)


class SimpleSenatoPDFDownloader:
    """Downloader semplice ma smart per Senato"""
    
    def __init__(self):
        self.processed_files = set()
        self.legislature_anni = {}  # Cache: leg -> lista anni che funzionano
    
    def _sleep(self, base: float, jitter: float):
        time.sleep(base + random.uniform(0, jitter))

    def test_legislatura_anno(self, leg: str, year: int) -> bool:
        """Testa se una legislatura ha documenti per un anno - SEMPLICE"""
        try:
            self._sleep(DELAY_HTML, JITTER_HTML)
            
            current_year = dt.datetime.now().year
            if year == current_year:
                url = BASE_TEMPLATE_ATTUALE.format(year=year)
            else:
                url = BASE_TEMPLATE.format(leg=leg, year=year)
            
            r = session.get(url, timeout=TIMEOUT_HTML)
            if r.status_code == 404:
                return False
            
            r.raise_for_status()
            soup = BeautifulSoup(r.text, "html.parser")
            pdf_links = soup.select('a[href$=".pdf"]')
            
            ha_pdf = len(pdf_links) > 0
            if ha_pdf:
                print(f"    ✅ Leg {leg}, anno {year}: {len(pdf_links)} PDF")
            
            return ha_pdf
            
        except Exception as e:
            print(f"    ❌ Errore test leg {leg}, anno {year}: {e}")
            return False

    def trova_legislature_per_range(self, leg_start: str, year_start: int, year_end: int) -> List[str]:
        """Trova le legislature che coprono il range di anni - LOGICA SEMPLICE"""
        print(f"🎯 Cerco legislature per coprire {year_start}-{year_end} (partendo da {leg_start})")
        
        legislature_ok = []
        leg_start_num = int(leg_start)
        anni_coperti = set()
        
        # Test legislatura di partenza
        print(f"\n🔍 Test legislatura {leg_start}...")
        anni_leg_start = []
        for year in range(year_start, year_end + 1):
            if self.test_legislatura_anno(leg_start, year):
                anni_leg_start.append(year)
                anni_coperti.add(year)
        
        if anni_leg_start:
            legislature_ok.append(leg_start)
            print(f"  📊 Legislatura {leg_start} copre anni: {anni_leg_start}")
        
        # Vai indietro se servono anni prima
        anni_mancanti_prima = [y for y in range(year_start, year_end + 1) if y < min(anni_leg_start) if anni_leg_start]
        if anni_mancanti_prima:
            print(f"\n⬅️  Servono anni precedenti: {anni_mancanti_prima}")
            for leg_num in range(leg_start_num - 1, max(1, leg_start_num - 10), -1):
                leg_str = str(leg_num)
                print(f"🔍 Test legislatura {leg_str}...")
                
                anni_questa_leg = []
                for year in anni_mancanti_prima:
                    if self.test_legislatura_anno(leg_str, year):
                        anni_questa_leg.append(year)
                        anni_coperti.add(year)
                
                if anni_questa_leg:
                    legislature_ok.append(leg_str)
                    print(f"  📊 Legislatura {leg_str} copre anni: {anni_questa_leg}")
                    
                    # Aggiorna anni mancanti
                    anni_mancanti_prima = [y for y in anni_mancanti_prima if y not in anni_coperti]
                    if not anni_mancanti_prima:
                        break
                else:
                    print(f"  ❌ Legislatura {leg_str} non ha documenti nel range")
        
        # Vai avanti se servono anni dopo  
        anni_mancanti_dopo = [y for y in range(year_start, year_end + 1) if y not in anni_coperti]
        if anni_mancanti_dopo:
            print(f"\n➡️  Servono anni successivi: {anni_mancanti_dopo}")
            for leg_num in range(leg_start_num + 1, leg_start_num + 5):
                leg_str = str(leg_num)
                print(f"🔍 Test legislatura {leg_str}...")
                
                anni_questa_leg = []
                for year in anni_mancanti_dopo:
                    if self.test_legislatura_anno(leg_str, year):
                        anni_questa_leg.append(year)
                        anni_coperti.add(year)
                
                if anni_questa_leg:
                    legislature_ok.append(leg_str)
                    print(f"  📊 Legislatura {leg_str} copre anni: {anni_questa_leg}")
                    
                    anni_mancanti_dopo = [y for y in anni_mancanti_dopo if y not in anni_coperti]
                    if not anni_mancanti_dopo:
                        break
                else:
                    print(f"  ❌ Legislatura {leg_str} non ha documenti nel range")
        
        # Ordina e return
        legislature_ok = sorted(list(set(legislature_ok)), key=int)
        print(f"\n📊 LEGISLATURE TROVATE: {', '.join(legislature_ok)}")
        
        anni_non_coperti = [y for y in range(year_start, year_end + 1) if y not in anni_coperti]
        if anni_non_coperti:
            print(f"⚠️  ANNI NON COPERTI: {anni_non_coperti}")
        
        return legislature_ok

    def get_pdf_links_with_dates(self, leg: str, year: int) -> List[Tuple[str, str, Optional[str]]]:
        """Prende PDF links + date dalla pagina - SEMPLICE"""
        self._sleep(DELAY_HTML, JITTER_HTML)
        
        current_year = dt.datetime.now().year
        if year == current_year:
            url = BASE_TEMPLATE_ATTUALE.format(year=year)
        else:
            url = BASE_TEMPLATE.format(leg=leg, year=year)
        
        try:
            r = session.get(url, timeout=TIMEOUT_HTML)
            if r.status_code == 404:
                return []
            r.raise_for_status()
            
            soup = BeautifulSoup(r.text, "html.parser")
            links = []
            
            # Mappa mesi italiani
            mesi = {
                'gennaio': '01', 'febbraio': '02', 'marzo': '03', 'aprile': '04',
                'maggio': '05', 'giugno': '06', 'luglio': '07', 'agosto': '08', 
                'settembre': '09', 'ottobre': '10', 'novembre': '11', 'dicembre': '12'
            }
            
            for a in soup.select('a[href$=".pdf"]'):
                pdf_url = urljoin("https://www.senato.it/", a["href"])
                filename = pdf_url.rsplit("/", 1)[1]
                
                # Cerca data nel testo vicino
                extracted_date = None
                for parent in [a.parent, a.parent.parent if a.parent else None]:
                    if not parent:
                        continue
                    
                    text = parent.get_text()
                    
                    # Pattern data italiana: "23 marzo 2025"
                    match = re.search(r'(\d{1,2})\s+(gennaio|febbraio|marzo|aprile|maggio|giugno|luglio|agosto|settembre|ottobre|novembre|dicembre)\s+(\d{4})', text, re.IGNORECASE)
                    if match:
                        day = match.group(1).zfill(2)
                        month = mesi.get(match.group(2).lower())
                        year_found = match.group(3)
                        if month:
                            extracted_date = f"{year_found}-{month}-{day}"
                            break
                    
                    # Pattern data slash: "23/03/2025"
                    match = re.search(r'(\d{1,2})/(\d{1,2})/(\d{4})', text)
                    if match:
                        day = match.group(1).zfill(2)
                        month = match.group(2).zfill(2)
                        year_found = match.group(3)
                        extracted_date = f"{year_found}-{month}-{day}"
                        break
                
                links.append((pdf_url, filename, extracted_date))
            
            return links
            
        except Exception as e:
            print(f"  ❌ Errore parsing leg {leg}, anno {year}: {e}")
            return []

    def download_pdf(self, url: str, filename: str, leg: str, year: int, extracted_date: Optional[str], dest_dir: Path) -> bool:
        """Download PDF - SEMPLICE"""
        file_id = f"{leg}_{year}_{filename}"
        if file_id in self.processed_files:
            return True
        
        dest_path = dest_dir / f"legislatura_{leg}" / str(year) / filename
        
        if dest_path.exists():
            print(f"  ✓ Esiste: {filename}")
            self.processed_files.add(file_id)
            return True
        
        dest_path.parent.mkdir(parents=True, exist_ok=True)

        for attempt in range(1, RETRIES + 1):
            try:
                print(f"  ⬇️  {filename} (tent. {attempt})...")
                with session.get(url, stream=True, timeout=TIMEOUT_PDF) as r:
                    r.raise_for_status()
                    tmp = dest_path.with_suffix(".part")
                    with open(tmp, "wb") as f:
                        for chunk in r.iter_content(8192):
                            f.write(chunk)
                    tmp.rename(dest_path)
                
                print(f"  ✅ OK: {filename}")
                
                # Metadata SEMPLICE
                self.create_metadata(dest_path, leg, year, extracted_date)
                self.processed_files.add(file_id)
                break
                
            except Exception as e:
                if attempt == RETRIES:
                    print(f"  ❌ FAIL: {filename} dopo {RETRIES} tentativi")
                    return False
                print(f"       Retry {attempt}/{RETRIES} fra {BACKOFF}s...")
                time.sleep(BACKOFF)
        
        self._sleep(DELAY_PDF, JITTER_PDF)
        return True

    def create_metadata(self, pdf_path: Path, leg: str, year: int, extracted_date: Optional[str]):
        """Crea metadata - SEMPLICE"""
        try:
            metadata = {
                "legislatura": leg,      # ← SO che è questa perché la sto scaricando da qui!
                "source": "senato",
                "document_type": "stenographic_report",
                "institution": "senato_repubblica",
                "language": "it", 
                "year": year,            # ← SO che è questo anno perché lo sto processando!
                "created_at": dt.datetime.now(dt.timezone.utc).isoformat()
            }
            
            if extracted_date:
                try:
                    dt.datetime.strptime(extracted_date, "%Y-%m-%d")  # Valida
                    metadata["date"] = extracted_date
                except ValueError:
                    pass
            
            json_path = pdf_path.with_suffix(".json")
            with open(json_path, "w", encoding="utf-8") as f:
                json.dump(metadata, f, ensure_ascii=False, indent=2)
                
        except Exception as e:
            print(f"  ⚠️  Errore metadata: {e}")

    def download_legislatura(self, leg: str, year_start: int, year_end: int, dest_dir: Path) -> Tuple[int, int]:
        """Scarica una legislatura per il range di anni - SEMPLICE"""
        print(f"\n📄 SCARICO LEGISLATURA {leg} ({year_start}-{year_end})")
        
        total_ok = 0
        total_err = 0
        
        for year in range(year_start, year_end + 1):
            links = self.get_pdf_links_with_dates(leg, year)
            if not links:
                print(f"  📭 Anno {year}: nessun PDF")
                continue
            
            print(f"  📄 Anno {year}: {len(links)} PDF")
            
            year_ok = 0
            year_err = 0
            
            for url, filename, date in links:
                if self.download_pdf(url, filename, leg, year, date, dest_dir):
                    year_ok += 1
                    total_ok += 1
                else:
                    year_err += 1
                    total_err += 1
            
            print(f"    📊 Anno {year}: {year_ok} OK, {year_err} errori")
        
        print(f"  📊 Legislatura {leg}: {total_ok} scaricati, {total_err} errori")
        return total_ok, total_err

    def run(self, leg_start: str, date_start: Optional[dt.date], date_end: Optional[dt.date], dest_dir: Path) -> bool:
        """RUN principale - SEMPLICE"""
        print(f"🏛️  SENATO - DOWNLOAD SEMPLICE MA SMART")
        print(f"📋 Partenza: legislatura {leg_start}")
        
        # Calcola range anni
        if date_start:
            year_start = date_start.year
        else:
            year_start = dt.datetime.now().year
            
        if date_end:
            year_end = date_end.year
        else:
            year_end = dt.datetime.now().year
        
        print(f"📅 Range anni: {year_start} - {year_end}")
        print(f"📁 Destinazione: {dest_dir}")
        
        # Trova legislature necessarie
        legislature = self.trova_legislature_per_range(leg_start, year_start, year_end)
        
        if not legislature:
            print("❌ Nessuna legislatura trovata")
            return False
        
        # Scarica ogni legislatura
        total_downloaded = 0
        total_errors = 0
        
        for i, leg in enumerate(legislature, 1):
            print(f"\n{'='*50}")
            print(f"LEGISLATURA {i}/{len(legislature)}: {leg}")
            print(f"{'='*50}")
            
            downloaded, errors = self.download_legislatura(leg, year_start, year_end, dest_dir)
            total_downloaded += downloaded
            total_errors += errors
        
        print(f"\n🏁 COMPLETATO")
        print(f"📈 Scaricati: {total_downloaded}")
        print(f"❌ Errori: {total_errors}")
        print(f"🏛️  Legislature: {', '.join(legislature)}")
        
        return total_errors == 0


def main():
    parser = argparse.ArgumentParser(
        description="Senato PDF Downloader - SIMPLE BUT SMART",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Esempi:
  # Dal 2013 a oggi partendo dalla 19
  python download_senato_pdf.py --leg 19 --from 2013-01-01 --out ./downloads
  
  # Range specifico
  python download_senato_pdf.py --leg 18 --from 2015-01-01 --to 2020-12-31 --out ./downloads
  
  # Solo la legislatura specificata
  python download_senato_pdf.py --leg 19 --out ./downloads
        """
    )
    
    parser.add_argument("--leg", required=True, help="Legislatura di partenza")
    parser.add_argument("--from", dest="from_date", type=lambda s: dt.datetime.strptime(s, "%Y-%m-%d").date(), help="Data iniziale (YYYY-MM-DD)")
    parser.add_argument("--to", dest="to_date", type=lambda s: dt.datetime.strptime(s, "%Y-%m-%d").date(), help="Data finale (YYYY-MM-DD)")
    parser.add_argument("--out", type=Path, required=True, help="Cartella di output")
    
    args = parser.parse_args()
    
    try:
        downloader = SimpleSenatoPDFDownloader()
        success = downloader.run(args.leg, args.from_date, args.to_date, args.out)
        sys.exit(0 if success else 1)
        
    except KeyboardInterrupt:
        print("\n🛑 Interrotto")
        sys.exit(130)
        
    except Exception as e:
        print(f"\n💥 ERRORE: {e}")
        import traceback
        traceback.print_exc()
        sys.exit(1)


if __name__ == "__main__":
    main()