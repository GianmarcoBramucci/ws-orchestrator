#!/usr/bin/env python3
"""
download_camera_pdf.py - v7.0 "Super Smart Multi-Legislature"
==============================================================
Downloader super intelligente che:
1. Usa la legislatura passata solo come PUNTO DI PARTENZA
2. Scarica TUTTE le legislature necessarie per coprire il range di date
3. Organizza tutto in cartelle separate per legislatura/anno
4. Zero hardcoding - tutto dinamico
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
from typing import Optional, Tuple, List, Dict, Set
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


class SuperSmartCameraPDFDownloader:
    """Downloader multi-legislatura super intelligente per Camera"""
    
    def __init__(self):
        self.session = requests.Session()
        self.session.headers.update(CONFIG["headers"])
        self.legislature_info = {}  # Cache delle info legislature
        self.processed_sedute = set()  # Per evitare duplicati tra legislature
        
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
        """Scopre dinamicamente il range di una legislatura"""
        if leg in self.legislature_info:
            return self.legislature_info[leg]
        
        print(f"  🔍 Scoperta dinamica legislatura {leg}...")
        
        info = {
            "exists": False,
            "earliest_date": None,
            "latest_date": None,
            "sample_dates": [],
            "working_sedute": [],
            "max_seduta_found": 0
        }
        
        # Test più ampio per trovare i limiti
        test_sedute = [1, 5, 10, 20, 50, 100, 200, 300, 400, 500, 600, 700, 800, 900, 1000]
        dates_found = []
        working_sedute = []
        max_seduta = 0
        
        for sed_num in test_sedute:
            exists, date_obj = self.check_seduta_exists(leg, sed_num)
            
            if exists:
                working_sedute.append(sed_num)
                max_seduta = max(max_seduta, sed_num)
                if date_obj:
                    dates_found.append(date_obj)
                    print(f"    ✅ Seduta {sed_num}: {date_obj}")
                else:
                    print(f"    ✅ Seduta {sed_num}: esiste (data non estratta)")
            
            self._sleep_with_jitter(CONFIG["delays"]["between_requests"])
        
        if working_sedute:
            info["exists"] = True
            info["working_sedute"] = working_sedute
            info["sample_dates"] = dates_found
            info["max_seduta_found"] = max_seduta
            
            if dates_found:
                info["earliest_date"] = min(dates_found)
                info["latest_date"] = max(dates_found)
                
                print(f"    📊 Legislatura {leg} SCOPERTA:")
                print(f"       🗓️  Range date: {info['earliest_date']} - {info['latest_date']}")
                print(f"       📄 Max seduta trovata: {max_seduta}")
        else:
            print(f"    ❌ Legislatura {leg} NON ESISTE")
        
        self.legislature_info[leg] = info
        return info
    
    def find_all_legislatures_for_range(self, start_date: dt.date, starting_leg: str, end_date: Optional[dt.date] = None) -> List[str]:
        """Trova TUTTE le legislature necessarie per coprire un range di date"""
        if not end_date:
            end_date = dt.date.today()
        
        print(f"🎯 Ricerca TUTTE le legislature per range {start_date} - {end_date}")
        print(f"   📍 Punto di partenza: legislatura {starting_leg}")
        
        starting_leg_num = int(starting_leg)
        legislatures_needed = []
        
        # Prima verifica la legislatura di partenza
        start_info = self.discover_legislature_range(starting_leg)
        
        if start_info["exists"] and start_info["earliest_date"] and start_info["latest_date"]:
            # Se la legislatura di partenza copre parte del range, includila
            if not (start_info["latest_date"] < start_date or start_info["earliest_date"] > end_date):
                legislatures_needed.append(starting_leg)
                print(f"   ✅ Legislatura {starting_leg} copre parte del range")
            
            # Ora cerca le altre legislature necessarie
            # Cerca all'indietro se serve
            if start_info["earliest_date"] > start_date:
                print(f"   ⬅️  Cerco legislature precedenti...")
                for leg_num in range(starting_leg_num - 1, max(1, starting_leg_num - 20), -1):
                    leg_str = str(leg_num)
                    leg_info = self.discover_legislature_range(leg_str)
                    
                    if leg_info["exists"] and leg_info["latest_date"]:
                        if leg_info["latest_date"] >= start_date:
                            legislatures_needed.append(leg_str)
                            print(f"   ✅ Aggiungo legislatura {leg_str} ({leg_info['earliest_date']} - {leg_info['latest_date']})")
                            
                            # Se questa legislatura copre l'inizio del range, possiamo fermarci
                            if leg_info["earliest_date"] and leg_info["earliest_date"] <= start_date:
                                break
                        elif leg_info["latest_date"] < start_date:
                            # Siamo andati troppo indietro
                            break
            
            # Cerca in avanti se serve
            if start_info["latest_date"] < end_date:
                print(f"   ➡️  Cerco legislature successive...")
                for leg_num in range(starting_leg_num + 1, starting_leg_num + 10):
                    leg_str = str(leg_num)
                    leg_info = self.discover_legislature_range(leg_str)
                    
                    if leg_info["exists"] and leg_info["earliest_date"]:
                        if leg_info["earliest_date"] <= end_date:
                            legislatures_needed.append(leg_str)
                            print(f"   ✅ Aggiungo legislatura {leg_str} ({leg_info['earliest_date']} - {leg_info['latest_date']})")
                            
                            # Se questa legislatura copre la fine del range, possiamo fermarci
                            if leg_info["latest_date"] and leg_info["latest_date"] >= end_date:
                                break
                    else:
                        # Non ci sono più legislature
                        break
        else:
            # Legislatura di partenza non valida, prova ricerca bidirezionale
            print(f"   🔍 Legislatura di partenza non valida, ricerca estesa...")
            for offset in range(-10, 10):
                leg_num = starting_leg_num + offset
                if leg_num < 1:
                    continue
                    
                leg_str = str(leg_num)
                leg_info = self.discover_legislature_range(leg_str)
                
                if leg_info["exists"] and leg_info["earliest_date"] and leg_info["latest_date"]:
                    # Verifica se questa legislatura copre parte del range
                    if not (leg_info["latest_date"] < start_date or leg_info["earliest_date"] > end_date):
                        legislatures_needed.append(leg_str)
                        print(f"   ✅ Trovata legislatura {leg_str} ({leg_info['earliest_date']} - {leg_info['latest_date']})")
        
        # Ordina le legislature
        legislatures_needed = sorted(list(set(legislatures_needed)), key=int)
        
        print(f"\n📊 LEGISLATURE DA PROCESSARE: {', '.join(legislatures_needed)}")
        return legislatures_needed
    
    def download_pdf(self, leg: str, sed_num: int, date_obj: Optional[dt.date], dest_dir: Path) -> bool:
        """Scarica un singolo PDF"""
        # Crea un ID univoco per evitare duplicati tra legislature
        seduta_id = f"{leg}_{sed_num}"
        if seduta_id in self.processed_sedute:
            print(f"  ⏭️  Seduta {sed_num} già processata in altra legislatura")
            return True
        
        pdf_url = PDF_URL_TEMPLATE.format(leg=leg, sed=sed_num)
        
        # Costruisce il nome del file
        if date_obj:
            filename = f"camera_leg{leg}_sed{sed_num:04d}_{date_obj.isoformat()}.pdf"
        else:
            filename = f"camera_leg{leg}_sed{sed_num:04d}_unknown_date.pdf"
        
        # Path con struttura legislatura_XX/YYYY/filename.pdf (senza duplicare camera)
        leg_subdir = f"legislatura_{leg}"
        year_subdir = str(date_obj.year) if date_obj else "unknown_year"
        dest_path = dest_dir / leg_subdir / year_subdir / filename
        
        if dest_path.exists():
            print(f"  ✓ Già esistente: {filename}")
            self.processed_sedute.add(seduta_id)
            return True
        
        # Crea directory
        try:
            dest_path.parent.mkdir(parents=True, exist_ok=True)
        except Exception as e:
            print(f"  ❌ Errore creazione directory: {e}")
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
                
                self.processed_sedute.add(seduta_id)
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
    
    def download_legislature(self, leg: str, start_date: Optional[dt.date], end_date: Optional[dt.date], output_dir: Path) -> Tuple[int, int]:
        """Scarica una singola legislatura nel range di date specificato"""
        print(f"\n📄 Processing Legislatura {leg}...")
        
        leg_info = self.legislature_info.get(leg, self.discover_legislature_range(leg))
        
        if not leg_info["exists"]:
            print(f"  ❌ Legislatura {leg} non trovata")
            return 0, 0
        
        # Determina il range di sedute da controllare
        start_sed = 1
        end_sed = leg_info["max_seduta_found"] + 100  # Estendi oltre l'ultima trovata
        
        print(f"  📊 Controllo sedute da {start_sed} a {end_sed}...")
        
        downloaded = 0
        errors = 0
        consecutive_missing = 0
        max_consecutive_missing = 50  # Più tollerante per legislature vecchie
        
        for sed_num in range(start_sed, end_sed + 1):
            exists, date_obj = self.check_seduta_exists(leg, sed_num)
            
            if not exists:
                consecutive_missing += 1
                if consecutive_missing >= max_consecutive_missing:
                    print(f"  🛑 Troppe sedute consecutive mancanti ({max_consecutive_missing}). Fine legislatura.")
                    break
                continue
            
            consecutive_missing = 0
            
            # Filtra per range di date se specificato
            if date_obj:
                if start_date and date_obj < start_date:
                    continue
                if end_date and date_obj > end_date:
                    continue
            
            # Scarica il PDF
            if self.download_pdf(leg, sed_num, date_obj, output_dir):
                downloaded += 1
            else:
                errors += 1
            
            self._sleep_with_jitter(CONFIG["delays"]["between_requests"])
        
        print(f"  📊 Legislatura {leg} completata: {downloaded} scaricati, {errors} errori")
        return downloaded, errors
    
    def smart_multi_legislature_download(self, requested_leg: str, start_date: Optional[dt.date], end_date: Optional[dt.date], output_dir: Path) -> bool:
        """Download super intelligente multi-legislatura"""
        print(f"🏛️  CAMERA DEI DEPUTATI - DOWNLOAD MULTI-LEGISLATURA")
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
        description="Super Smart Multi-Legislature Camera Downloader",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Esempi:
  # Scarica TUTTE le legislature dal 2012 ad oggi (partendo dalla 19)
  python download_camera_pdf.py --leg 19 --from 2012-01-01 --out ./downloads
  
  # Scarica un range specifico di date attraverso più legislature
  python download_camera_pdf.py --leg 18 --from 2015-01-01 --to 2020-12-31 --out ./downloads
  
  # Scarica solo la legislatura specificata (senza date)
  python download_camera_pdf.py --leg 19 --out ./downloads
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
        downloader = SuperSmartCameraPDFDownloader()
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