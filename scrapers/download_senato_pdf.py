#!/usr/bin/env python3
"""
download_senato_pdf.py - v13.0 "SIMPLE AND CORRECT"
===================================================
Logica corretta e semplice:
1. Test anni: SEMPRE con BASE_TEMPLATE per legislature passate
2. Legislatura attuale: anno fine = anno corrente, anno inizio = fine precedente + 1
3. Cartelle semplici: solo legislatura_XX senza sottocartelle anni
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
from typing import List, Optional, Tuple, Set, Dict
from urllib.parse import urljoin

import requests
from bs4 import BeautifulSoup

# Config
DELAY_HTML = 2.0
DELAY_PDF = 2.0  
JITTER_HTML = 1.5
JITTER_PDF = 1.0
RETRIES = 3
TIMEOUT_HTML = 3
TIMEOUT_PDF = 3
BACKOFF = 20

BASE_TEMPLATE = "https://www.senato.it/legislature/{leg}/lavori/assemblea/resoconti-elenco-cronologico?year={year}"
BASE_TEMPLATE_ATTUALE = "https://www.senato.it/lavori/assemblea/resoconti-elenco-cronologico?year={year}"

HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36",
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    "Accept-Language": "it-IT,it;q=0.9,en;q=0.8"
}

session = requests.Session()
session.headers.update(HEADERS)


class SimpleCorrectSenatoPDFDownloader:
    """Downloader semplice e corretto per Senato"""
    
    def __init__(self):
        self.processed_files = set()
        self.legislature_info = {}
        self.current_legislature = None
        self.current_year = dt.datetime.now().year
    
    def _sleep(self, base: float, jitter: float):
        time.sleep(base + random.uniform(0, jitter))

    def identify_current_legislature(self) -> Optional[str]:
        """Identifica la legislatura corrente dal sito"""
        print("🔍 Identificazione legislatura corrente...")
        
        try:
            self._sleep(DELAY_HTML, JITTER_HTML)
            
            url = BASE_TEMPLATE_ATTUALE.format(year=self.current_year)
            r = session.get(url, timeout=TIMEOUT_HTML)
            
            if r.status_code != 200:
                return None
                
            soup = BeautifulSoup(r.text, "html.parser")
            
            # Cerca nei link PDF
            pdf_links = soup.select('a[href$=".pdf"]')
            for link in pdf_links[:5]:
                href = link.get('href', '')
                # Cerca pattern /legislature/XX/ o simili
                match = re.search(r'/legislature/(\d+)/', href)
                if match:
                    leg = match.group(1)
                    print(f"  ✅ Legislatura corrente: {leg}")
                    return leg
                
                # O cerca leg19, leg 19, etc
                match = re.search(r'leg\s*(\d+)', href, re.IGNORECASE)
                if match:
                    leg = match.group(1)
                    print(f"  ✅ Legislatura corrente: {leg}")
                    return leg
            
            # Cerca nel testo
            text = soup.get_text()
            patterns = [
                r'XIX\s+legislatura',  # Assumiamo XIX = 19
                r'(\d+)ª?\s+legislatura',
                r'Legislatura\s+(\d+)'
            ]
            
            for pattern in patterns:
                match = re.search(pattern, text, re.IGNORECASE)
                if match:
                    leg = match.group(1) if '(' in pattern else '19'  # XIX = 19
                    print(f"  ✅ Legislatura corrente: {leg}")
                    return leg
            
            return None
            
        except Exception as e:
            print(f"  ❌ Errore: {e}")
            return None

    def test_legislature_years(self, leg: str) -> Tuple[Optional[int], Optional[int]]:
        """Testa gli anni di una legislatura PASSATA usando BASE_TEMPLATE"""
        print(f"  🔍 Test anni legislatura {leg}...")
        
        # Range di test
        min_year = max(1946, self.current_year - 80)
        max_year = self.current_year
        
        years_with_docs = []
        
        for year in range(min_year, max_year + 1):
            try:
                self._sleep(DELAY_HTML, JITTER_HTML)
                
                # SEMPRE BASE_TEMPLATE per testare
                url = BASE_TEMPLATE.format(leg=leg, year=year)
                r = session.get(url, timeout=TIMEOUT_HTML)
                
                if r.status_code == 403:
                    print(f"    ⚠️  403 per anno {year} - pausa...")
                    time.sleep(30)
                    continue
                    
                if r.status_code == 404:
                    continue
                    
                if r.status_code == 200:
                    soup = BeautifulSoup(r.text, "html.parser")
                    pdf_links = soup.select('a[href$=".pdf"]')
                    
                    if pdf_links:
                        years_with_docs.append(year)
                        print(f"    ✅ Anno {year}: {len(pdf_links)} documenti")
                        
            except Exception:
                continue
        
        if years_with_docs:
            start_year = min(years_with_docs)
            end_year = max(years_with_docs)
            print(f"    📊 Legislatura {leg}: {start_year}-{end_year}")
            return start_year, end_year
        else:
            print(f"    ❌ Legislatura {leg}: nessun documento trovato")
            return None, None

    def determine_all_legislatures_info(self, start_leg: str) -> Dict[str, Dict]:
        """Determina info di tutte le legislature necessarie"""
        print("📊 Determinazione info legislature...")
        
        # Prima identifica la corrente
        self.current_legislature = self.identify_current_legislature()
        if not self.current_legislature:
            print("  ⚠️  Non riesco a identificare la legislatura corrente, assumo 19")
            self.current_legislature = "19"
        
        current_num = int(self.current_legislature)
        start_num = int(start_leg)
        
        # Testa tutte le legislature passate nel range
        for leg_num in range(max(1, start_num - 5), current_num):
            leg_str = str(leg_num)
            start_year, end_year = self.test_legislature_years(leg_str)
            
            if start_year and end_year:
                self.legislature_info[leg_str] = {
                    'start_year': start_year,
                    'end_year': end_year,
                    'exists': True
                }
        
        # Info legislatura corrente
        if self.current_legislature:
            # Fine della precedente
            prev_num = current_num - 1
            prev_info = self.legislature_info.get(str(prev_num), {})
            
            if prev_info.get('end_year'):
                current_start = prev_info['end_year']
            else:
                current_start = self.current_year - 5  # Stima
            
            self.legislature_info[self.current_legislature] = {
                'start_year': current_start,
                'end_year': self.current_year,
                'exists': True,
                'is_current': True
            }
            
            print(f"  📍 Legislatura corrente {self.current_legislature}: {current_start}-{self.current_year}")
        
        return self.legislature_info

    def find_legislatures_for_range(self, start_date: dt.date, end_date: dt.date) -> List[str]:
        """Trova legislature che coprono il range di date"""
        print(f"🎯 Selezione legislature per {start_date} → {end_date}")
        
        selected = []
        
        for leg, info in sorted(self.legislature_info.items(), key=lambda x: int(x[0])):
            if not info.get('exists'):
                continue
                
            leg_start = dt.date(info['start_year'], 1, 1)
            leg_end = dt.date(info['end_year'], 12, 31)
            
            # Check overlap
            if leg_end >= start_date and leg_start <= end_date:
                selected.append(leg)
                print(f"  ✅ Legislatura {leg} ({info['start_year']}-{info['end_year']})")
        
        return selected

    def get_pdf_links_with_dates(self, leg: str, year: int) -> List[Tuple[str, str, Optional[str]]]:
        """Ottiene i link PDF con le date"""
        self._sleep(DELAY_HTML, JITTER_HTML)
        
        # Usa template corretto
        is_current = (leg == self.current_legislature)
        if is_current:
            url = BASE_TEMPLATE_ATTUALE.format(year=year)
        else:
            url = BASE_TEMPLATE.format(leg=leg, year=year)
        
        try:
            r = session.get(url, timeout=TIMEOUT_HTML)
            
            if r.status_code == 403:
                print(f"    ⚠️  403 Forbidden")
                time.sleep(30)
                return []
                
            if r.status_code != 200:
                return []
            
            soup = BeautifulSoup(r.text, "html.parser")
            links = []
            
            mesi = {
                'gennaio': '01', 'febbraio': '02', 'marzo': '03', 'aprile': '04',
                'maggio': '05', 'giugno': '06', 'luglio': '07', 'agosto': '08', 
                'settembre': '09', 'ottobre': '10', 'novembre': '11', 'dicembre': '12'
            }
            
            for a in soup.select('a[href$=".pdf"]'):
                pdf_url = urljoin("https://www.senato.it/", a["href"])
                filename = pdf_url.rsplit("/", 1)[1]
                
                # Cerca data
                extracted_date = None
                for parent in [a.parent, a.parent.parent if a.parent else None]:
                    if not parent:
                        continue
                    
                    text = parent.get_text()
                    
                    # Pattern data italiana
                    match = re.search(r'(\d{1,2})\s+(gennaio|febbraio|marzo|aprile|maggio|giugno|luglio|agosto|settembre|ottobre|novembre|dicembre)\s+(\d{4})', text, re.IGNORECASE)
                    if match:
                        day = match.group(1).zfill(2)
                        month = mesi.get(match.group(2).lower())
                        year_found = match.group(3)
                        if month:
                            extracted_date = f"{year_found}-{month}-{day}"
                            break
                
                links.append((pdf_url, filename, extracted_date))
            
            return links
            
        except Exception as e:
            print(f"  ❌ Errore: {e}")
            return []

    def download_pdf(self, url: str, filename: str, leg: str, extracted_date: Optional[str], dest_dir: Path) -> bool:
        """Download PDF - SENZA SOTTOCARTELLE ANNI"""
        file_id = f"{leg}_{filename}"
        if file_id in self.processed_files:
            return True
        
        # Path semplice: solo legislatura_XX/filename.pdf
        dest_path = dest_dir / f"legislatura_{leg}" / filename
        
        if dest_path.exists():
            print(f"  ✓ Esiste: {filename}")
            self.processed_files.add(file_id)
            return True
        
        dest_path.parent.mkdir(parents=True, exist_ok=True)

        for attempt in range(1, RETRIES + 1):
            try:
                print(f"  ⬇️  {filename} (tent. {attempt})...")
                self._sleep(DELAY_PDF, JITTER_PDF)
                
                with session.get(url, stream=True, timeout=TIMEOUT_PDF) as r:
                    if r.status_code == 403:
                        print(f"       ⚠️  403 - pausa lunga...")
                        time.sleep(60)
                        continue
                        
                    r.raise_for_status()
                    
                    tmp = dest_path.with_suffix(".part")
                    with open(tmp, "wb") as f:
                        for chunk in r.iter_content(8192):
                            f.write(chunk)
                    tmp.rename(dest_path)
                
                print(f"  ✅ OK: {filename}")
                
                # Metadata
                self.create_metadata(dest_path, leg, extracted_date)
                self.processed_files.add(file_id)
                return True
                
            except Exception as e:
                if attempt == RETRIES:
                    print(f"  ❌ FAIL: {filename}")
                    return False
                print(f"       Retry {attempt}/{RETRIES}...")
                time.sleep(BACKOFF)
        
        return False

    def create_metadata(self, pdf_path: Path, leg: str, extracted_date: Optional[str]):
        """Crea metadata JSON"""
        try:
            leg_info = self.legislature_info.get(leg, {})
            
            metadata = {
                "legislatura": leg,
                "source": "senato",
                "document_type": "stenographic_report",
                "institution": "senato_repubblica",
                "language": "it",
                "is_current_legislature": leg == self.current_legislature,
                "created_at": dt.datetime.now(dt.timezone.utc).isoformat()
            }
            
            if extracted_date:
                metadata["date"] = extracted_date
                # Estrai anno dalla data
                try:
                    year = int(extracted_date.split('-')[0])
                    metadata["year"] = year
                except:
                    pass
            
            if leg_info:
                metadata["legislature_years"] = f"{leg_info.get('start_year', '?')}-{leg_info.get('end_year', '?')}"
            
            json_path = pdf_path.with_suffix(".json")
            with open(json_path, "w", encoding="utf-8") as f:
                json.dump(metadata, f, ensure_ascii=False, indent=2)
                
        except Exception as e:
            print(f"  ⚠️  Errore metadata: {e}")

    def download_legislature(self, leg: str, start_date: dt.date, end_date: dt.date, dest_dir: Path) -> Tuple[int, int]:
        """Scarica una legislatura nel range di date"""
        print(f"\n📄 DOWNLOAD LEGISLATURA {leg}")
        
        leg_info = self.legislature_info.get(leg, {})
        is_current = leg_info.get('is_current', False)
        
        if is_current:
            print(f"  🌟 LEGISLATURA CORRENTE - uso template attuale")
        
        # Anni da processare
        year_start = max(leg_info.get('start_year', start_date.year), start_date.year)
        year_end = min(leg_info.get('end_year', end_date.year), end_date.year)
        
        total_ok = 0
        total_err = 0
        
        for year in range(year_start, year_end + 1):
            links = self.get_pdf_links_with_dates(leg, year)
            
            if not links:
                print(f"  📭 Anno {year}: nessun PDF")
                continue
            
            # Filtra per date se necessario
            filtered_links = []
            for url, filename, date_str in links:
                if date_str:
                    try:
                        doc_date = dt.datetime.strptime(date_str, "%Y-%m-%d").date()
                        if doc_date >= start_date and doc_date <= end_date:
                            filtered_links.append((url, filename, date_str))
                    except:
                        filtered_links.append((url, filename, date_str))
                else:
                    # Se non ho la data, includo comunque
                    filtered_links.append((url, filename, date_str))
            
            if not filtered_links:
                print(f"  📭 Anno {year}: nessun PDF nel range date")
                continue
                
            print(f"  📄 Anno {year}: {len(filtered_links)} PDF")
            
            for url, filename, date in filtered_links:
                if self.download_pdf(url, filename, leg, date, dest_dir):
                    total_ok += 1
                else:
                    total_err += 1
        
        print(f"  📊 Totale: {total_ok} OK, {total_err} errori")
        return total_ok, total_err

    def run(self, leg_start: str, date_start: Optional[dt.date], date_end: Optional[dt.date], dest_dir: Path) -> bool:
        """Run principale"""
        print(f"🏛️  SENATO - SIMPLE AND CORRECT DOWNLOADER")
        print(f"📋 Legislatura riferimento: {leg_start}")
        
        if not date_start:
            date_start = dt.date(1946, 1, 1)
        if not date_end:
            date_end = dt.date.today()
            
        print(f"📅 Range date: {date_start} → {date_end}")
        print(f"📁 Output: {dest_dir}")
        
        # Step 1: Determina info legislature
        self.determine_all_legislatures_info(leg_start)
        
        # Step 2: Seleziona legislature per il range
        legislature = self.find_legislatures_for_range(date_start, date_end)
        
        if not legislature:
            print("❌ Nessuna legislatura nel range")
            return False
        
        # Step 3: Download
        total_ok = 0
        total_err = 0
        
        for i, leg in enumerate(legislature, 1):
            print(f"\n{'='*50}")
            print(f"LEGISLATURA {i}/{len(legislature)}: {leg}")
            print(f"{'='*50}")
            
            ok, err = self.download_legislature(leg, date_start, date_end, dest_dir)
            total_ok += ok
            total_err += err
        
        print(f"\n🏁 COMPLETATO")
        print(f"✅ Scaricati: {total_ok}")
        print(f"❌ Errori: {total_err}")
        
        return total_err == 0


def main():
    parser = argparse.ArgumentParser(
        description="Senato Downloader - Simple and Correct",
        epilog="""
Esempi:
  python download_senato_pdf.py --leg 17 --from 2013-03-15 --out ./downloads
  python download_senato_pdf.py --leg 18 --from 2018-03-23 --to 2022-10-12 --out ./downloads
        """
    )
    
    parser.add_argument("--leg", required=True, help="Legislatura di riferimento")
    parser.add_argument("--from", dest="from_date", type=lambda s: dt.datetime.strptime(s, "%Y-%m-%d").date(), help="Data inizio")
    parser.add_argument("--to", dest="to_date", type=lambda s: dt.datetime.strptime(s, "%Y-%m-%d").date(), help="Data fine")
    parser.add_argument("--out", type=Path, required=True, help="Cartella output")
    
    args = parser.parse_args()
    
    try:
        downloader = SimpleCorrectSenatoPDFDownloader()
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