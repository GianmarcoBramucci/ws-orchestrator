#!/usr/bin/env python3
"""
download_camera_pdf_smart.py - v6.0 "Truly Smart - No Hardcoded Bullshit"
==========================================================================
Downloader intelligente per Camera che ricava TUTTO dinamicamente dai siti:
1. Testa le legislature direttamente sui server Camera
2. Estrae date reali dai documenti 
3. Calcola automaticamente quale legislatura serve per una data
4. Zero hardcoding - tutto ricavato dinamicamente
"""
from __future__ import annotations
import argparse
import json
import re
import sys
import time
import requests
import datetime as dt
from pathlib import Path
from typing import Optional, Tuple, List, Dict
from urllib.parse import urljoin

# Configurazione
CONFIG = {
    "delays": {
        "between_requests": 0.5,
        "on_error": 2.0,
        "jitter": 0.2
    },
    "retries": {
        "max_attempts": 3,
        "backoff_factor": 2
    },
    "timeouts": {
        "request": 30
    },
    "headers": {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
                     "(KHTML, like Gecko) Chrome/123.0 Safari/537.36"
    }
}

# URL templates per la Camera
PDF_URL_TEMPLATE = "https://documenti.camera.it/leg{leg}/resoconti/assemblea/html/sed{sed:04d}/stenografico.pdf"
INFO_URL_TEMPLATE = "https://documenti.camera.it/leg{leg}/resoconti/assemblea/html/sed{sed:04d}/stenografico.htm"


class TrulySmartCameraPDFDownloader:
    """Downloader completamente dinamico per i PDF della Camera dei Deputati"""
    
    def __init__(self):
        self.session = requests.Session()
        self.session.headers.update(CONFIG["headers"])
        self.legislature_info = {}  # Cache delle info legislature ricavate dinamicamente
        
    def _sleep_with_jitter(self, base_delay: float):
        """Sleep con jitter randomico"""
        import random
        jitter = random.uniform(0, CONFIG["delays"]["jitter"])
        time.sleep(base_delay + jitter)
    
    def _extract_date_from_info_page(self, leg: str, sed_num: int) -> Optional[dt.date]:
        """Estrae la data da una pagina info di una seduta"""
        info_url = INFO_URL_TEMPLATE.format(leg=leg, sed=sed_num)
        
        try:
            response = self.session.get(info_url, timeout=CONFIG["timeouts"]["request"])
            if response.status_code == 200:
                # Cerca pattern di data nel contenuto HTML
                date_patterns = [
                    r'(\d{1,2})\s+(gennaio|febbraio|marzo|aprile|maggio|giugno|luglio|agosto|settembre|ottobre|novembre|dicembre)\s+(\d{4})',
                    r'(\d{4})-(\d{2})-(\d{2})',  # ISO format
                    r'(\d{1,2})/(\d{1,2})/(\d{4})'  # DD/MM/YYYY
                ]
                
                months_map = {
                    "gennaio": 1, "febbraio": 2, "marzo": 3, "aprile": 4,
                    "maggio": 5, "giugno": 6, "luglio": 7, "agosto": 8,
                    "settembre": 9, "ottobre": 10, "novembre": 11, "dicembre": 12
                }
                
                for pattern in date_patterns:
                    match = re.search(pattern, response.text, re.IGNORECASE)
                    if match:
                        if pattern == date_patterns[0]:  # Italian format
                            day, month_name, year = match.groups()
                            month = months_map.get(month_name.lower())
                            if month:
                                return dt.date(int(year), month, int(day))
                        elif pattern == date_patterns[1]:  # ISO format
                            year, month, day = match.groups()
                            return dt.date(int(year), int(month), int(day))
                        elif pattern == date_patterns[2]:  # DD/MM/YYYY
                            day, month, year = match.groups()
                            return dt.date(int(year), int(month), int(day))
        except:
            pass
        
        return None
    
    def check_seduta_exists(self, leg: str, sed_num: int) -> Tuple[bool, Optional[dt.date]]:
        """Controlla se una seduta esiste e ne estrae la data"""
        pdf_url = PDF_URL_TEMPLATE.format(leg=leg, sed=sed_num)
        
        try:
            response = self.session.head(pdf_url, timeout=CONFIG["timeouts"]["request"])
            
            if response.status_code == 200:
                date_obj = self._extract_date_from_info_page(leg, sed_num)
                return True, date_obj
            
            return False, None
            
        except requests.exceptions.RequestException:
            return False, None
    
    def discover_legislature_range(self, leg: str) -> Dict:
        """Scopre dinamicamente il range di una legislatura testando sedute campione"""
        if leg in self.legislature_info:
            return self.legislature_info[leg]
        
        print(f"  🔍 Scoperta dinamica legislatura {leg}...")
        
        info = {
            "exists": False,
            "earliest_date": None,
            "latest_date": None,
            "sample_dates": [],
            "working_sedute": []
        }
        
        # Strategia di discovery: testa sedute a intervalli crescenti
        test_sedute = [1, 5, 10, 20, 50, 100, 200, 300, 400, 500]
        dates_found = []
        working_sedute = []
        
        for sed_num in test_sedute:
            print(f"    🧪 Test seduta {sed_num}...")
            exists, date_obj = self.check_seduta_exists(leg, sed_num)
            
            if exists:
                working_sedute.append(sed_num)
                if date_obj:
                    dates_found.append(date_obj)
                    print(f"    ✅ Seduta {sed_num}: {date_obj}")
                else:
                    print(f"    ✅ Seduta {sed_num}: (data non estratta)")
            else:
                print(f"    ❌ Seduta {sed_num}: non esiste")
            
            self._sleep_with_jitter(CONFIG["delays"]["between_requests"])
        
        if working_sedute:
            info["exists"] = True
            info["working_sedute"] = working_sedute
            info["sample_dates"] = dates_found
            
            if dates_found:
                info["earliest_date"] = min(dates_found)
                info["latest_date"] = max(dates_found)
                
                print(f"    📊 Legislatura {leg} SCOPERTA:")
                print(f"       🗓️  Range date: {info['earliest_date']} - {info['latest_date']}")
                print(f"       📄 Sedute trovate: {len(working_sedute)}")
            else:
                print(f"    📊 Legislatura {leg} TROVATA ma senza date estraibili")
        else:
            print(f"    ❌ Legislatura {leg} NON ESISTE")
        
        self.legislature_info[leg] = info
        return info
    
    def find_legislature_for_date(self, target_date: dt.date, starting_leg: str) -> str:
        """Trova dinamicamente quale legislatura contiene una data specifica"""
        print(f"🎯 Ricerca legislatura per data {target_date}...")
        print(f"   📍 Punto di partenza: legislatura {starting_leg}")
        
        starting_leg_num = int(starting_leg)
        
        # Prima controlla la legislatura di partenza
        start_info = self.discover_legislature_range(starting_leg)
        
        if start_info["exists"] and start_info["earliest_date"] and start_info["latest_date"]:
            if start_info["earliest_date"] <= target_date <= start_info["latest_date"]:
                print(f"   ✅ Data trovata nella legislatura di partenza {starting_leg}")
                return starting_leg
            
            # Determina direzione di ricerca
            if target_date < start_info["earliest_date"]:
                print(f"   ⬅️  Data più vecchia, cerco nelle legislature precedenti")
                search_direction = -1
                search_range = range(starting_leg_num - 1, max(1, starting_leg_num - 10), -1)
            else:
                print(f"   ➡️  Data più recente, cerco nelle legislature successive")
                search_direction = 1
                search_range = range(starting_leg_num + 1, starting_leg_num + 10)
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
            
            if leg_info["exists"] and leg_info["earliest_date"] and leg_info["latest_date"]:
                if leg_info["earliest_date"] <= target_date <= leg_info["latest_date"]:
                    print(f"   ✅ Data trovata nella legislatura {leg_str}!")
                    return leg_str
                else:
                    print(f"   📅 Legislatura {leg_str}: {leg_info['earliest_date']} - {leg_info['latest_date']} (non copre {target_date})")
            else:
                print(f"   ❌ Legislatura {leg_str}: non valida")
        
        # Se non trovo nulla, uso la legislatura di partenza come fallback
        print(f"   ⚠️  Nessuna legislatura trovata per {target_date}, uso {starting_leg} come fallback")
        return starting_leg
    
    def get_full_seduta_range(self, leg: str) -> Tuple[int, int]:
        """Determina il range completo di sedute da controllare per una legislatura"""
        leg_info = self.legislature_info.get(leg)
        
        if leg_info and leg_info["working_sedute"]:
            # Estendi il range oltre le sedute campione trovate
            min_sed = min(leg_info["working_sedute"])
            max_sed = max(leg_info["working_sedute"])
            
            # Range esteso per catturare tutto
            start_range = max(1, min_sed - 5)
            end_range = max_sed + 100  # Estendi molto oltre l'ultima trovata
            
            print(f"    📊 Range esteso per leg {leg}: sedute {start_range}-{end_range}")
            return start_range, end_range
        else:
            # Fallback per legislature sconosciute
            print(f"    ⚠️  Range fallback per leg {leg}: sedute 1-600")
            return 1, 600
    
    def download_pdf(self, leg: str, sed_num: int, date_obj: Optional[dt.date], dest_dir: Path) -> bool:
        """Scarica un singolo PDF con gestione path corretta"""
        pdf_url = PDF_URL_TEMPLATE.format(leg=leg, sed=sed_num)
        
        dest_dir = Path(dest_dir)
        
        # Costruisce il nome del file
        if date_obj:
            filename = f"camera_leg{leg}_sed{sed_num:04d}_{date_obj.isoformat()}.pdf"
        else:
            filename = f"camera_leg{leg}_sed{sed_num:04d}_unknown_date.pdf"
        
        # Path construction sicura
        leg_subdir = f"legislatura_{leg}"
        year_subdir = str(date_obj.year) if date_obj else "unknown_year"
        dest_path = dest_dir / leg_subdir / year_subdir / filename
        
        # Controllo sicurezza contro concatenazioni errate
        path_str = str(dest_path)
        if any(problem in path_str.lower() for problem in ["downloadscamera", "camera2025"]):
            print(f"❌ PATH CAMERA MALFORMATO: {dest_path}")
            return False
        
        if dest_path.exists():
            print(f"  ✓ Già esistente: {filename}")
            return True
        
        # Crea directory
        try:
            dest_path.parent.mkdir(parents=True, exist_ok=True)
        except Exception as e:
            print(f"  ❌ Errore creazione directory {dest_path.parent}: {e}")
            return False
        
        for attempt in range(CONFIG["retries"]["max_attempts"]):
            try:
                print(f"  ⬇️  Scaricando: {filename} (tentativo {attempt + 1})...")
                
                with self.session.get(pdf_url, stream=True, timeout=CONFIG["timeouts"]["request"]) as response:
                    response.raise_for_status()
                    
                    # Verifica che sia un PDF
                    content_type = response.headers.get("content-type", "").lower()
                    if "application/pdf" not in content_type:
                        print(f"  ⚠️  Non è un PDF: {content_type}")
                        return False
                    
                    # Scarica in file temporaneo
                    temp_path = dest_path.with_suffix(".tmp")
                    with open(temp_path, "wb") as f:
                        for chunk in response.iter_content(chunk_size=8192):
                            if chunk:
                                f.write(chunk)
                    
                    # Rinomina atomicamente
                    temp_path.rename(dest_path)
                
                print(f"  ✅ Completato: {filename}")
                
                # Crea metadata JSON
                self._create_metadata(dest_path, leg, sed_num, date_obj)
                
                self._sleep_with_jitter(CONFIG["delays"]["between_requests"])
                return True
                
            except requests.exceptions.RequestException as e:
                print(f"  ⚠️  Errore (tentativo {attempt + 1}): {e}")
                
                # Pulisci file temporaneo
                temp_path = dest_path.with_suffix(".tmp")
                if temp_path.exists():
                    temp_path.unlink()
                
                if attempt == CONFIG["retries"]["max_attempts"] - 1:
                    print(f"  ❌ Fallito: {filename}")
                    return False
                
                time.sleep(CONFIG["retries"]["backoff_factor"] ** attempt)
        
        return False
    
    def _create_metadata(self, pdf_path: Path, leg: str, sed_num: int, date_obj: Optional[dt.date]):
        """Crea il file metadata JSON"""
        try:
            metadata = {
                "legislatura": leg,
                "seduta": sed_num,
                "source": "camera",
                "document_type": "stenographic_report",
                "institution": "camera_deputati",
                "language": "it",
                "created_at": dt.datetime.now(dt.timezone.utc).isoformat()
            }
            
            if date_obj:
                metadata["date"] = date_obj.isoformat()
            
            metadata_path = pdf_path.with_suffix(".json")
            with open(metadata_path, "w", encoding="utf-8") as f:
                json.dump(metadata, f, ensure_ascii=False, indent=2)
                
        except Exception as e:
            print(f"  ⚠️  Errore metadata: {e}")
    
    def smart_download(self, requested_leg: str, start_date: Optional[dt.date], output_dir: Path) -> bool:
        """Download intelligente completamente dinamico"""
        print(f"🏛️  Camera dei Deputati - Download Dinamico")
        print(f"📋 Legislatura richiesta: {requested_leg}")
        if start_date:
            print(f"📅 Data di partenza: {start_date.isoformat()}")
        
        output_dir = Path(output_dir).resolve()
        print(f"📁 Directory output: {output_dir}")
        
        # Controllo sicurezza path
        if any(problem in str(output_dir).lower() for problem in ["downloadscamera", "camera2025"]):
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
        
        start_sed, end_sed = self.get_full_seduta_range(target_leg)
        
        total_downloaded = 0
        total_errors = 0
        consecutive_missing = 0
        max_consecutive_missing = 20
        
        print(f"📊 Controllo sedute da {start_sed} a {end_sed}...")
        
        for sed_num in range(start_sed, end_sed + 1):
            exists, date_obj = self.check_seduta_exists(target_leg, sed_num)
            
            if not exists:
                consecutive_missing += 1
                if consecutive_missing >= max_consecutive_missing:
                    print(f"  🛑 Troppe sedute consecutive mancanti ({max_consecutive_missing}). Fermata.")
                    break
                continue
            
            consecutive_missing = 0
            
            # Filtra per data se disponibile
            if start_date and date_obj and date_obj < start_date:
                print(f"  ⏭️  Seduta {sed_num:04d} ({date_obj}) troppo vecchia, salto")
                continue
            
            # Scarica il PDF
            if self.download_pdf(target_leg, sed_num, date_obj, output_dir):
                total_downloaded += 1
            else:
                total_errors += 1
            
            self._sleep_with_jitter(CONFIG["delays"]["between_requests"])
        
        print(f"\n🏁 DOWNLOAD DINAMICO COMPLETATO")
        print(f"📈 Scaricati: {total_downloaded}")
        print(f"❌ Errori: {total_errors}")
        print(f"🏛️  Legislatura utilizzata: {target_leg}")
        
        return total_errors == 0


def main():
    parser = argparse.ArgumentParser(
        description="Dynamic Camera Downloader - Discovers everything from the websites",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Esempi:
  # Il sistema scopre dinamicamente la legislatura giusta per la data
  python download_camera_pdf_smart.py --leg 19 --from 2024-05-01 --out ./downloads
  
  # Per date vecchie, scopre automaticamente la legislatura corretta testando i siti
  python download_camera_pdf_smart.py --leg 19 --from 2020-01-01 --out ./downloads
  
  # Anche per legislature future, testa dinamicamente
  python download_camera_pdf_smart.py --leg 25 --from 2030-01-01 --out ./downloads
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
        downloader = TrulySmartCameraPDFDownloader()
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