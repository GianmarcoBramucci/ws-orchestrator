#!/usr/bin/env python3
"""
download_senato_pdf_smart.py - v8.0 "Truly Smart - No Hardcoded Bullshit"
===========================================================================
Downloader intelligente per Senato che ricava TUTTO dinamicamente dai siti:
1. Usa la tua logica get_year_bounds() per estrarre range dalle pagine
2. Testa le legislature direttamente sui server Senato
3. Calcola automaticamente quale legislatura serve per una data
4. Zero hardcoding - tutto ricavato dinamicamente come nei tuoi script originali
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
from typing import List, Optional, Tuple, Dict
from urllib.parse import urljoin

import requests
from bs4 import BeautifulSoup

# ――― Config (identica al tuo script funzionante) ―――
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


class TrulySmartSenatoPDFDownloader:
    """Downloader completamente dinamico per il Senato"""
    
    def __init__(self):
        self.legislature_info = {}  # Cache delle info legislature ricavate dinamicamente
    
    def _sleep(self, base: float, jitter: float):
        time.sleep(base + random.uniform(0, jitter))

    def _ita_date_year(self, s: str) -> int:
        """Estrae anno da stringa data italiana (dal tuo script originale)"""
        return int(s.split()[-1])

    def _extract_years(self, html: str) -> Optional[Tuple[int, Optional[int]]]:
        """Estrae range anni dalla pagina HTML (dal tuo script originale)"""
        m = DATE_RE.search(html)
        if not m:
            return None
        return self._ita_date_year(m.group(1)), (self._ita_date_year(m.group(2)) if m.group(2) else None)

    def test_legislature_year(self, leg: str, year: int) -> Tuple[bool, int, Optional[Tuple[int, Optional[int]]]]:
        """Testa se una legislatura ha documenti per un dato anno E estrae il range"""
        self._sleep(DELAY_HTML, JITTER_HTML)
        
        # USA TEMPLATE ATTUALE se siamo nell'anno corrente, altrimenti quello storico
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
            
            # Estrae range anni dalla pagina (tua logica originale)
            year_range = self._extract_years(r.text)
            
            # Conta i PDF
            soup = BeautifulSoup(r.text, "html.parser")
            pdf_links = [urljoin("https://www.senato.it/", a["href"]) for a in soup.select('a[href$=".pdf"]')]
            
            doc_count = len(pdf_links)
            exists = doc_count > 0
            
            print(f"    📊 Leg {leg}, Anno {year}: {doc_count} documenti" + 
                  (f", range estratto: {year_range}" if year_range else ""))
            
            return exists, doc_count, year_range
            
        except Exception as e:
            print(f"    ❌ Errore test leg {leg}, anno {year}: {e}")
            return False, 0, None

    def discover_legislature_range(self, leg: str) -> Dict:
        """Scopre dinamicamente il range di una legislatura usando la tua logica get_year_bounds"""
        if leg in self.legislature_info:
            return self.legislature_info[leg]
        
        print(f"  🔍 Scoperta dinamica legislatura {leg}...")
        
        info = {
            "exists": False,
            "years_with_docs": [],
            "total_docs": 0,
            "extracted_range": None,  # Range estratto dalle pagine HTML
            "discovered_range": None  # Range scoperto testando
        }
        
        current_year = dt.datetime.now().year
        
        # Strategia: testa prima l'anno corrente per estrarre il range dalle pagine
        print(f"    🧪 Test anno corrente {current_year} per estrarre range...")
        exists, doc_count, year_range = self.test_legislature_year(leg, current_year)
        
        if year_range:
            info["extracted_range"] = year_range
            start_year, end_year = year_range
            print(f"    📅 Range estratto dalle pagine: {start_year} - {end_year or 'in corso'}")
            
            # Testa tutti gli anni nel range estratto
            test_years = list(range(start_year, (end_year or current_year) + 1))
        else:
            # Se non riesco a estrarre il range, uso una strategia di discovery più ampia
            print(f"    🔍 Range non estratto, provo discovery estesa...")
            # Testa anni attorno a una stima ragionevole
            base_year = current_year - 2
            test_years = list(range(base_year - 5, current_year + 2))
        
        years_with_docs = []
        total_docs = 0
        
        print(f"    🧪 Test anni: {test_years}")
        
        for year in test_years:
            exists, doc_count, _ = self.test_legislature_year(leg, year)
            if exists and doc_count > 0:
                years_with_docs.append(year)
                total_docs += doc_count
        
        info["years_with_docs"] = years_with_docs
        info["total_docs"] = total_docs
        
        if total_docs > 0:
            info["exists"] = True
            if years_with_docs:
                info["discovered_range"] = (min(years_with_docs), max(years_with_docs))
            
            print(f"    ✅ Legislatura {leg} SCOPERTA:")
            print(f"       📊 {total_docs} documenti in {len(years_with_docs)} anni")
            if info["extracted_range"]:
                print(f"       📅 Range estratto: {info['extracted_range']}")
            if info["discovered_range"]:
                print(f"       🔍 Range scoperto: {info['discovered_range']}")
        else:
            print(f"    ❌ Legislatura {leg} NON TROVATA")
        
        self.legislature_info[leg] = info
        return info

    def find_legislature_for_date(self, target_date: dt.date, starting_leg: str) -> str:
        """Trova dinamicamente quale legislatura contiene una data specifica"""
        print(f"🎯 Ricerca legislatura per data {target_date}...")
        print(f"   📍 Punto di partenza: legislatura {starting_leg}")
        
        starting_leg_num = int(starting_leg)
        target_year = target_date.year
        
        # Prima controlla la legislatura di partenza
        start_info = self.discover_legislature_range(starting_leg)
        
        if start_info["exists"]:
            # Usa il range estratto se disponibile, altrimenti quello scoperto
            date_range = start_info["extracted_range"] or start_info["discovered_range"]
            
            if date_range:
                start_year, end_year = date_range
                end_year = end_year or dt.datetime.now().year  # Se None, usa anno corrente
                
                if start_year <= target_year <= end_year:
                    print(f"   ✅ Anno target {target_year} trovato nella legislatura di partenza {starting_leg} ({start_year}-{end_year})")
                    return starting_leg
                
                # Determina direzione di ricerca
                if target_year < start_year:
                    print(f"   ⬅️  Anno {target_year} più vecchio del range {start_year}-{end_year}, cerco nelle legislature precedenti")
                    search_range = range(starting_leg_num - 1, max(1, starting_leg_num - 8), -1)
                else:
                    print(f"   ➡️  Anno {target_year} più recente del range {start_year}-{end_year}, cerco nelle legislature successive")
                    search_range = range(starting_leg_num + 1, starting_leg_num + 8)
            else:
                print(f"   ⚠️  Range non determinabile per legislatura {starting_leg}")
                search_range = list(range(starting_leg_num - 3, starting_leg_num + 3))
                search_range = [x for x in search_range if x > 0 and x != starting_leg_num]
        else:
            # Se la legislatura di partenza non esiste, cerca in entrambe le direzioni
            print(f"   🔍 Legislatura di partenza non valida, ricerca bidirezionale")
            search_range = list(range(starting_leg_num - 5, starting_leg_num + 5))
            search_range = [x for x in search_range if x > 0 and x != starting_leg_num]
        
        # Cerca nelle altre legislature
        for leg_num in search_range:
            leg_str = str(leg_num)
            print(f"   🔍 Controllo legislatura {leg_str}...")
            
            leg_info = self.discover_legislature_range(leg_str)
            
            if leg_info["exists"]:
                date_range = leg_info["extracted_range"] or leg_info["discovered_range"]
                
                if date_range:
                    start_year, end_year = date_range
                    end_year = end_year or dt.datetime.now().year
                    
                    if start_year <= target_year <= end_year:
                        print(f"   ✅ Anno target {target_year} trovato nella legislatura {leg_str} ({start_year}-{end_year})!")
                        return leg_str
                    else:
                        print(f"   📅 Legislatura {leg_str}: {start_year}-{end_year} (non copre {target_year})")
                else:
                    print(f"   ⚠️  Legislatura {leg_str}: range non determinabile")
            else:
                print(f"   ❌ Legislatura {leg_str}: non valida")
        
        # Se non trovo nulla, uso la legislatura di partenza come fallback
        print(f"   ⚠️  Nessuna legislatura trovata per anno {target_year}, uso {starting_leg} come fallback")
        return starting_leg

    def get_year_range_for_legislature(self, leg: str, start_date: Optional[dt.date]) -> Tuple[int, Optional[int]]:
        """Determina il range di anni da processare per una legislatura"""
        leg_info = self.legislature_info.get(leg, self.discover_legislature_range(leg))
        
        if not leg_info["exists"]:
            # Fallback intelligente basato sulla data
            if start_date:
                return start_date.year, dt.datetime.now().year
            else:
                return 2022, dt.datetime.now().year
        
        # Usa il range estratto se disponibile, altrimenti quello scoperto
        date_range = leg_info["extracted_range"] or leg_info["discovered_range"]
        
        if date_range:
            start_year, end_year = date_range
            end_year = end_year or dt.datetime.now().year
            
            # Se abbiamo una data di start, aggiusta il range
            if start_date:
                start_year = min(start_year, start_date.year)
        else:
            # Fallback sui dati scoperti
            if leg_info["years_with_docs"]:
                start_year = min(leg_info["years_with_docs"])
                end_year = max(leg_info["years_with_docs"])
            else:
                start_year = start_date.year if start_date else 2022
                end_year = dt.datetime.now().year
        
        print(f"    📊 Range anni per leg {leg}: {start_year}-{end_year}")
        return start_year, end_year

    def parse_pdf_links(self, leg: str, year: int) -> List[str]:
        """Parse PDF links (identica al tuo script originale)"""
        self._sleep(DELAY_HTML, JITTER_HTML)
        
        # USA TEMPLATE ATTUALE se siamo nell'anno corrente, altrimenti quello storico
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
        return [urljoin("https://www.senato.it/", a["href"]) for a in soup.select('a[href$=".pdf"]')]

    def download_pdf(self, url: str, dest: Path, overwrite: bool = False):
        """Download PDF (identica al tuo script originale) con gestione path corretta"""
        
        dest = Path(dest)  # Assicura Path object
        
        # Controllo sicurezza path
        dest_str = str(dest)
        if any(problem in dest_str.lower() for problem in ["downloadsenato", "senato2025", "legislatura19"]):
            error_msg = f"❌ DEST PATH MALFORMATO: {dest}"
            print(error_msg)
            raise ValueError(error_msg)
        
        if dest.exists() and not overwrite:
            print(f"  ✓ Già esistente: {dest.name}")
            return
        
        # Crea directory
        try:
            dest.parent.mkdir(parents=True, exist_ok=True)
        except Exception as e:
            print(f"  ❌ Errore creazione directory {dest.parent}: {e}")
            raise

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
        
        self._sleep(DELAY_PDF, JITTER_PDF)

    def create_metadata_file(self, pdf_path: Path, leg: str) -> bool:
        """Crea il file di metadata JSON"""
        try:
            pdf_path = Path(pdf_path)
            
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

    def smart_download(self, requested_leg: str, start_date: Optional[dt.date], output_dir: Path) -> bool:
        """Smart download completamente dinamico"""
        print(f"🏛️  Senato Repubblica - Download Dinamico")
        print(f"📋 Legislatura richiesta: {requested_leg}")
        if start_date:
            print(f"📅 Data di partenza: {start_date.isoformat()}")
        
        output_dir = Path(output_dir).resolve()
        print(f"📁 Directory output: {output_dir}")
        
        # Controllo sicurezza path
        if any(problem in str(output_dir).lower() for problem in ["downloadsenato", "senato2025"]):
            print(f"❌ OUTPUT DIR MALFORMATA: {output_dir}")
            return False
        
        # Trova la legislatura giusta dinamicamente
        if start_date:
            target_leg = self.find_legislature_for_date(start_date, requested_leg)
        else:
            target_leg = requested_leg
            # Verifica comunque che esista
            leg_info = self.discover_legislature_range(target_leg)
            if not leg_info["exists"]:
                print(f"❌ Legislatura {target_leg} non esiste!")
                return False
        
        print(f"\n📄 Download da Legislatura {target_leg}...")
        
        # Determina range anni per questa legislatura
        y_start, y_end = self.get_year_range_for_legislature(target_leg, start_date)
        
        total_downloaded = 0
        total_errors = 0
        
        for yr in range(y_start, (y_end or dt.datetime.now().year) + 1):
            # Se abbiamo una data di filtro e l'anno è troppo vecchio, salta
            if start_date and yr < start_date.year:
                print(f"    📅 Anno {yr}: troppo vecchio, saltato")
                continue
                
            links = self.parse_pdf_links(target_leg, yr)
            if not links:
                print(f"    📭 {yr}: nessun pdf")
                continue
                
            print(f"    📄 {yr}: {len(links)} pdf")
            
            year_downloads = 0
            year_errors = 0
            
            for link in links:
                fname = link.rsplit("/", 1)[1]
                
                # Path construction sicura (identica al tuo script)
                leg_subdir = f"legislatura_{target_leg}"
                year_subdir = str(yr)
                path = output_dir / leg_subdir / year_subdir / fname
                
                # Controllo sicurezza path finale
                path_str = str(path)
                if any(problem in path_str.lower() for problem in ["downloadsenato", "senato2025"]):
                    print(f"      ❌ PATH FINALE MALFORMATO: {path}")
                    year_errors += 1
                    continue
                
                try:
                    self.download_pdf(link, path)
                    # Crea metadata
                    self.create_metadata_file(path, target_leg)
                    year_downloads += 1
                    total_downloaded += 1
                except Exception as exc:
                    print(f"      ❌ ERRORE {fname}: {exc}")
                    year_errors += 1
                    total_errors += 1
            
            print(f"      📊 Anno {yr}: {year_downloads} scaricati, {year_errors} errori")
        
        print(f"\n🏁 DOWNLOAD DINAMICO COMPLETATO")
        print(f"📈 Totale scaricati: {total_downloaded}")
        print(f"❌ Totale errori: {total_errors}")
        print(f"🏛️  Legislatura utilizzata: {target_leg}")
        
        return total_errors == 0


def main():
    parser = argparse.ArgumentParser(
        description="Dynamic Senato Downloader - Discovers everything from the websites",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Esempi:
  # Il sistema scopre dinamicamente la legislatura giusta per la data
  python download_senato_pdf_smart.py --leg 19 --from 2024-01-01 --out ./downloads
  
  # Per date vecchie, scopre automaticamente la legislatura corretta testando i siti
  python download_senato_pdf_smart.py --leg 19 --from 2018-01-01 --out ./downloads
  
  # Anche per legislature future, testa dinamicamente
  python download_senato_pdf_smart.py --leg 25 --from 2030-01-01 --out ./downloads
        """
    )
    
    parser.add_argument("--leg", required=True, 
                       help="Numero della legislatura richiesta (usata come punto di partenza)")
    
    parser.add_argument("--from", dest="from_date", 
                       type=lambda s: dt.datetime.strptime(s, "%Y-%m-%d").date(),
                       help="Data minima da cui scaricare (YYYY-MM-DD)")
    
    parser.add_argument("--out", type=Path, required=True,
                       help="Cartella di output")
    
    args = parser.parse_args()
    
    args.out = Path(args.out).resolve()
    print(f"📁 Directory output normalizzata: {args.out}")
    
    try:
        downloader = TrulySmartSenatoPDFDownloader()
        success = downloader.smart_download(args.leg, args.from_date, args.out)
        
        if success:
            print("\n🎉 Download dinamico completato con successo!")
            sys.exit(0)
        else:
            print("\n⚠️  Download dinamico completato con alcuni errori")
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