#!/usr/bin/env python3
"""
download_senato_pdf.py - v9.0 "Super Smart Multi-Legislature"
==============================================================
Downloader super intelligente che:
1. Usa la legislatura passata solo come PUNTO DI PARTENZA
2. Scarica TUTTE le legislature necessarie per coprire il range di date
3. Organizza tutto in cartelle separate per legislatura/anno
4. Zero hardcoding - tutto dinamico
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
from typing import List, Optional, Tuple, Dict, Set
from urllib.parse import urljoin

import requests
from bs4 import BeautifulSoup

# Configurazione
DELAY_HTML = 10.0    # rispetto robots.txt
DELAY_PDF = 1.5
JITTER_HTML = 1.5
JITTER_PDF = 0.8
RETRIES = 3
TIMEOUT_HTML = 20
TIMEOUT_PDF = 60
BACKOFF = 15

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

# HTTP session
session = requests.Session()
session.headers.update(HEADERS)


class SuperSmartSenatoPDFDownloader:
    """Downloader multi-legislatura super intelligente per Senato"""
    
    def __init__(self):
        self.legislature_info = {}  # Cache delle info legislature
        self.processed_files = set()  # Per evitare duplicati
    
    def _sleep(self, base: float, jitter: float):
        time.sleep(base + random.uniform(0, jitter))

    def _ita_date_year(self, s: str) -> int:
        """Estrae anno da stringa data italiana"""
        return int(s.split()[-1])

    def _extract_years(self, html: str) -> Optional[Tuple[int, Optional[int]]]:
        """Estrae range anni dalla pagina HTML"""
        m = DATE_RE.search(html)
        if not m:
            return None
        return self._ita_date_year(m.group(1)), (self._ita_date_year(m.group(2)) if m.group(2) else None)

    def test_legislature_year(self, leg: str, year: int) -> Tuple[bool, int, Optional[Tuple[int, Optional[int]]]]:
        """Testa se una legislatura ha documenti per un dato anno"""
        self._sleep(DELAY_HTML, JITTER_HTML)
        
        current_year = dt.datetime.now().year
        if year == current_year:
            url = BASE_TEMPLATE_ATTUALE.format(year=year)
        else:
            url = BASE_TEMPLATE.format(leg=leg, year=year)
        
        try:
            r = session.get(url, timeout=TIMEOUT_HTML)
            if r.status_code == 404:
                return False, 0, None
            
            r.raise_for_status()
            
            year_range = self._extract_years(r.text)
            
            soup = BeautifulSoup(r.text, "html.parser")
            pdf_links = [urljoin("https://www.senato.it/", a["href"]) for a in soup.select('a[href$=".pdf"]')]
            
            doc_count = len(pdf_links)
            exists = doc_count > 0
            
            return exists, doc_count, year_range
            
        except Exception as e:
            print(f"    ❌ Errore test leg {leg}, anno {year}: {e}")
            return False, 0, None

    def discover_legislature_range(self, leg: str) -> Dict:
        """Scopre dinamicamente il range di una legislatura"""
        if leg in self.legislature_info:
            return self.legislature_info[leg]
        
        print(f"  🔍 Scoperta dinamica legislatura {leg}...")
        
        info = {
            "exists": False,
            "years_with_docs": [],
            "total_docs": 0,
            "extracted_range": None,
            "discovered_range": None
        }
        
        current_year = dt.datetime.now().year
        
        # Test anno corrente per range
        exists, doc_count, year_range = self.test_legislature_year(leg, current_year)
        
        if year_range:
            info["extracted_range"] = year_range
            start_year, end_year = year_range
            print(f"    📅 Range estratto: {start_year} - {end_year or 'in corso'}")
            test_years = list(range(start_year, (end_year or current_year) + 1))
        else:
            # Discovery estesa
            print(f"    🔍 Range non estratto, discovery estesa...")
            # Test più ampio per legislature vecchie
            base_year = current_year - 3
            test_years = list(range(base_year - 10, current_year + 2))
        
        years_with_docs = []
        total_docs = 0
        
        for year in test_years:
            exists, doc_count, _ = self.test_legislature_year(leg, year)
            if exists and doc_count > 0:
                years_with_docs.append(year)
                total_docs += doc_count
                print(f"    📊 Anno {year}: {doc_count} documenti")
        
        info["years_with_docs"] = years_with_docs
        info["total_docs"] = total_docs
        
        if total_docs > 0:
            info["exists"] = True
            if years_with_docs:
                info["discovered_range"] = (min(years_with_docs), max(years_with_docs))
            
            print(f"    ✅ Legislatura {leg} SCOPERTA:")
            print(f"       📊 {total_docs} documenti in {len(years_with_docs)} anni")
            if info["discovered_range"]:
                print(f"       🔍 Range: {info['discovered_range'][0]} - {info['discovered_range'][1]}")
        else:
            print(f"    ❌ Legislatura {leg} NON TROVATA")
        
        self.legislature_info[leg] = info
        return info

    def find_all_legislatures_for_range(self, start_date: dt.date, starting_leg: str, end_date: Optional[dt.date] = None) -> List[str]:
        """Trova TUTTE le legislature necessarie per coprire un range di date"""
        if not end_date:
            end_date = dt.date.today()
        
        print(f"🎯 Ricerca TUTTE le legislature per range {start_date} - {end_date}")
        print(f"   📍 Punto di partenza: legislatura {starting_leg}")
        
        starting_leg_num = int(starting_leg)
        target_start_year = start_date.year
        target_end_year = end_date.year
        legislatures_needed = []
        
        # Verifica legislatura di partenza
        start_info = self.discover_legislature_range(starting_leg)
        
        if start_info["exists"]:
            date_range = start_info["extracted_range"] or start_info["discovered_range"]
            
            if date_range:
                start_year, end_year = date_range
                end_year = end_year or dt.datetime.now().year
                
                # Include se copre parte del range
                if not (end_year < target_start_year or start_year > target_end_year):
                    legislatures_needed.append(starting_leg)
                    print(f"   ✅ Legislatura {starting_leg} copre anni {start_year}-{end_year}")
                
                # Cerca all'indietro se necessario
                if start_year > target_start_year:
                    print(f"   ⬅️  Cerco legislature precedenti per coprire dal {target_start_year}...")
                    for leg_num in range(starting_leg_num - 1, max(1, starting_leg_num - 20), -1):
                        leg_str = str(leg_num)
                        leg_info = self.discover_legislature_range(leg_str)
                        
                        if leg_info["exists"]:
                            leg_range = leg_info["extracted_range"] or leg_info["discovered_range"]
                            if leg_range:
                                leg_start, leg_end = leg_range
                                leg_end = leg_end or dt.datetime.now().year
                                
                                if leg_end >= target_start_year:
                                    legislatures_needed.append(leg_str)
                                    print(f"   ✅ Aggiungo legislatura {leg_str} ({leg_start}-{leg_end})")
                                    
                                    if leg_start <= target_start_year:
                                        break
                                elif leg_end < target_start_year:
                                    break
                
                # Cerca in avanti se necessario
                if end_year < target_end_year:
                    print(f"   ➡️  Cerco legislature successive per coprire fino al {target_end_year}...")
                    for leg_num in range(starting_leg_num + 1, starting_leg_num + 10):
                        leg_str = str(leg_num)
                        leg_info = self.discover_legislature_range(leg_str)
                        
                        if leg_info["exists"]:
                            leg_range = leg_info["extracted_range"] or leg_info["discovered_range"]
                            if leg_range:
                                leg_start, leg_end = leg_range
                                leg_end = leg_end or dt.datetime.now().year
                                
                                if leg_start <= target_end_year:
                                    legislatures_needed.append(leg_str)
                                    print(f"   ✅ Aggiungo legislatura {leg_str} ({leg_start}-{leg_end})")
                                    
                                    if leg_end >= target_end_year:
                                        break
                        else:
                            break
        else:
            # Ricerca estesa se la legislatura di partenza non è valida
            print(f"   🔍 Legislatura di partenza non valida, ricerca estesa...")
            for offset in range(-15, 10):
                leg_num = starting_leg_num + offset
                if leg_num < 1:
                    continue
                    
                leg_str = str(leg_num)
                leg_info = self.discover_legislature_range(leg_str)
                
                if leg_info["exists"]:
                    date_range = leg_info["extracted_range"] or leg_info["discovered_range"]
                    if date_range:
                        start_year, end_year = date_range
                        end_year = end_year or dt.datetime.now().year
                        
                        if not (end_year < target_start_year or start_year > target_end_year):
                            legislatures_needed.append(leg_str)
                            print(f"   ✅ Trovata legislatura {leg_str} ({start_year}-{end_year})")
        
        # Ordina le legislature
        legislatures_needed = sorted(list(set(legislatures_needed)), key=int)
        
        print(f"\n📊 LEGISLATURE DA PROCESSARE: {', '.join(legislatures_needed)}")
        return legislatures_needed

    def parse_pdf_links(self, leg: str, year: int) -> List[Tuple[str, str]]:
        """Parse PDF links con i loro nomi"""
        self._sleep(DELAY_HTML, JITTER_HTML)
        
        current_year = dt.datetime.now().year
        if year == current_year:
            url = BASE_TEMPLATE_ATTUALE.format(year=year)
        else:
            url = BASE_TEMPLATE.format(leg=leg, year=year)
        
        r = session.get(url, timeout=TIMEOUT_HTML)
        if r.status_code == 404:
            return []
        r.raise_for_status()
        
        soup = BeautifulSoup(r.text, "html.parser")
        links = []
        for a in soup.select('a[href$=".pdf"]'):
            pdf_url = urljoin("https://www.senato.it/", a["href"])
            filename = pdf_url.rsplit("/", 1)[1]
            links.append((pdf_url, filename))
        
        return links

    def download_pdf(self, url: str, filename: str, leg: str, year: int, dest_dir: Path) -> bool:
        """Download PDF con gestione path multi-legislatura"""
        # ID univoco per evitare duplicati
        file_id = f"{leg}_{year}_{filename}"
        if file_id in self.processed_files:
            print(f"  ⏭️  File {filename} già processato")
            return True
        
        # Path con struttura legislatura_XX/YYYY/filename (senza duplicare senato)
        leg_subdir = f"legislatura_{leg}"
        year_subdir = str(year)
        dest_path = dest_dir / leg_subdir / year_subdir / filename
        
        if dest_path.exists():
            print(f"  ✓ Già esistente: {filename}")
            self.processed_files.add(file_id)
            return True
        
        # Crea directory
        try:
            dest_path.parent.mkdir(parents=True, exist_ok=True)
        except Exception as e:
            print(f"  ❌ Errore creazione directory: {e}")
            return False

        for attempt in range(1, RETRIES + 1):
            try:
                print(f"  ⬇️  Scaricando: {filename} (tentativo {attempt})...")
                with session.get(url, stream=True, timeout=TIMEOUT_PDF) as r:
                    r.raise_for_status()
                    tmp = dest_path.with_suffix(".part")
                    with open(tmp, "wb") as f:
                        for chunk in r.iter_content(8192):
                            f.write(chunk)
                    tmp.rename(dest_path)
                print(f"  ✅ Completato: {filename}")
                
                # Crea metadata
                self.create_metadata_file(dest_path, leg, year)
                
                self.processed_files.add(file_id)
                break
                
            except (requests.exceptions.ReadTimeout, requests.exceptions.ConnectionError) as e:
                if attempt == RETRIES:
                    print(f"  ❌ Fallito dopo {RETRIES} tentativi: {filename}")
                    return False
                print(f"       timeout, retry {attempt}/{RETRIES} fra {BACKOFF}s...")
                time.sleep(BACKOFF)
        
        self._sleep(DELAY_PDF, JITTER_PDF)
        return True

    def create_metadata_file(self, pdf_path: Path, leg: str, year: int) -> bool:
        """Crea il file di metadata JSON"""
        try:
            metadata = {
                "legislatura": leg,
                "source": "senato",
                "document_type": "stenographic_report",
                "institution": "senato_repubblica", 
                "language": "it",
                "year": year,
                "created_at": dt.datetime.now(dt.timezone.utc).isoformat()
            }
            
            metadata_path = pdf_path.with_suffix(".json")
            with open(metadata_path, "w", encoding="utf-8") as f:
                json.dump(metadata, f, ensure_ascii=False, indent=2)
            
            return True
            
        except Exception as e:
            print(f"  ⚠️  Errore metadata: {e}")
            return False

    def download_legislature(self, leg: str, start_date: Optional[dt.date], end_date: Optional[dt.date], output_dir: Path) -> Tuple[int, int]:
        """Scarica una singola legislatura nel range di date specificato"""
        print(f"\n📄 Processing Legislatura {leg}...")
        
        leg_info = self.legislature_info.get(leg, self.discover_legislature_range(leg))
        
        if not leg_info["exists"]:
            print(f"  ❌ Legislatura {leg} non trovata")
            return 0, 0
        
        # Determina range anni
        date_range = leg_info["extracted_range"] or leg_info["discovered_range"]
        if date_range:
            y_start, y_end = date_range
            y_end = y_end or dt.datetime.now().year
        else:
            # Fallback
            y_start = start_date.year if start_date else 2020
            y_end = end_date.year if end_date else dt.datetime.now().year
        
        # Applica filtri date se specificati
        if start_date:
            y_start = max(y_start, start_date.year)
        if end_date:
            y_end = min(y_end, end_date.year)
        
        print(f"  📊 Range anni da processare: {y_start} - {y_end}")
        
        total_downloaded = 0
        total_errors = 0
        
        for year in range(y_start, y_end + 1):
            links = self.parse_pdf_links(leg, year)
            if not links:
                print(f"    📭 Anno {year}: nessun pdf")
                continue
                
            print(f"    📄 Anno {year}: {len(links)} pdf")
            
            year_downloads = 0
            year_errors = 0
            
            for url, filename in links:
                try:
                    if self.download_pdf(url, filename, leg, year, output_dir):
                        year_downloads += 1
                        total_downloaded += 1
                    else:
                        year_errors += 1
                        total_errors += 1
                except Exception as e:
                    print(f"      ❌ ERRORE {filename}: {e}")
                    year_errors += 1
                    total_errors += 1
            
            print(f"      📊 Anno {year}: {year_downloads} scaricati, {year_errors} errori")
        
        print(f"  📊 Legislatura {leg} completata: {total_downloaded} scaricati, {total_errors} errori")
        return total_downloaded, total_errors

    def smart_multi_legislature_download(self, requested_leg: str, start_date: Optional[dt.date], end_date: Optional[dt.date], output_dir: Path) -> bool:
        """Download super intelligente multi-legislatura"""
        print(f"🏛️  SENATO DELLA REPUBBLICA - DOWNLOAD MULTI-LEGISLATURA")
        print(f"📋 Legislatura di partenza: {requested_leg}")
        print(f"📅 Range date: {start_date.isoformat() if start_date else 'inizio'} - {end_date.isoformat() if end_date else 'oggi'}")
        
        output_dir = Path(output_dir).resolve()
        print(f"📁 Directory output: {output_dir}")
        
        # Trova TUTTE le legislature necessarie
        if start_date:
            legislatures = self.find_all_legislatures_for_range(start_date, requested_leg, end_date)
        else:
            # Se non c'è data di inizio, usa solo la legislatura richiesta
            legislatures = [requested_leg]
        
        if not legislatures:
            print("❌ Nessuna legislatura trovata per il range specificato")
            return False
        
        # Download di ogni legislatura
        total_downloaded = 0
        total_errors = 0
        
        for i, leg in enumerate(legislatures, 1):
            print(f"\n{'='*60}")
            print(f"LEGISLATURA {i}/{len(legislatures)}: {leg}")
            print(f"{'='*60}")
            
            downloaded, errors = self.download_legislature(leg, start_date, end_date, output_dir)
            total_downloaded += downloaded
            total_errors += errors
        
        print(f"\n{'='*60}")
        print(f"🏁 DOWNLOAD MULTI-LEGISLATURA COMPLETATO")
        print(f"📈 Totale scaricati: {total_downloaded}")
        print(f"❌ Totale errori: {total_errors}")
        print(f"🏛️  Legislature processate: {', '.join(legislatures)}")
        print(f"{'='*60}")
        
        return total_errors == 0


def main():
    parser = argparse.ArgumentParser(
        description="Super Smart Multi-Legislature Senato Downloader",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Esempi:
  # Scarica TUTTE le legislature dal 2010 ad oggi (partendo dalla 19)
  python download_senato_pdf.py --leg 19 --from 2010-01-01 --out ./downloads
  
  # Scarica un range specifico di date attraverso più legislature
  python download_senato_pdf.py --leg 18 --from 2015-01-01 --to 2020-12-31 --out ./downloads
  
  # Scarica solo la legislatura specificata (senza date)
  python download_senato_pdf.py --leg 19 --out ./downloads
        """
    )
    
    parser.add_argument("--leg", required=True, 
                       help="Legislatura di PARTENZA (il sistema trova automaticamente le altre)")
    
    parser.add_argument("--from", dest="from_date", 
                       type=lambda s: dt.datetime.strptime(s, "%Y-%m-%d").date(),
                       help="Data iniziale (YYYY-MM-DD)")
    
    parser.add_argument("--to", dest="to_date",
                       type=lambda s: dt.datetime.strptime(s, "%Y-%m-%d").date(),
                       help="Data finale (YYYY-MM-DD) - default: oggi")
    
    parser.add_argument("--out", type=Path, required=True,
                       help="Cartella di output")
    
    args = parser.parse_args()
    
    try:
        downloader = SuperSmartSenatoPDFDownloader()
        success = downloader.smart_multi_legislature_download(
            args.leg,
            args.from_date,
            args.to_date,
            args.out
        )
        
        if success:
            print("\n🎉 Download multi-legislatura completato con successo!")
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