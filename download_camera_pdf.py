#!/usr/bin/env python3
"""
download_camera_pdf_fixed.py - v5.1 "Il Definitivo FIXED"
==========================================================
Sistema robusto per scaricare PDF della Camera che funziona con la struttura URL attuale.
Non si basa più sugli elenchi mensili ma prova direttamente le sedute in sequenza.
FIXED: Gestione path corretta per evitare concatenazioni errate.
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
from typing import Optional, Tuple
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


class CameraPDFDownloader:
    """Downloader robusto per i PDF della Camera dei Deputati"""
    
    def __init__(self):
        self.session = requests.Session()
        self.session.headers.update(CONFIG["headers"])
        self.found_sedute = []
        
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
                # Formato tipico: "giovedì 19 settembre 2024" o simili
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
            # Prova prima con HEAD request per essere veloce
            response = self.session.head(pdf_url, timeout=CONFIG["timeouts"]["request"])
            
            if response.status_code == 200:
                # Se il PDF esiste, prova a estrarre la data
                date_obj = self._extract_date_from_info_page(leg, sed_num)
                return True, date_obj
            
            return False, None
            
        except requests.exceptions.RequestException:
            return False, None
    
    def download_pdf(self, leg: str, sed_num: int, date_obj: Optional[dt.date], dest_dir: Path) -> bool:
        """Scarica un singolo PDF con gestione path corretta"""
        pdf_url = PDF_URL_TEMPLATE.format(leg=leg, sed=sed_num)
        
        # ===== FIX CRITICO: Assicura che dest_dir sia Path object =====
        dest_dir = Path(dest_dir)  # Forza conversione a Path
        
        # Costruisce il nome del file
        if date_obj:
            filename = f"camera_leg{leg}_sed{sed_num:04d}_{date_obj.isoformat()}.pdf"
        else:
            filename = f"camera_leg{leg}_sed{sed_num:04d}_unknown_date.pdf"
        
        # ===== FIX CRITICO: Path construction sicura COME SENATO =====
        # Struttura: legislatura_XX/anno/file.pdf (come Senato)
        leg_subdir = f"legislatura_{leg}"
        year_subdir = str(date_obj.year) if date_obj else "unknown_year"
        
        # Path joining sicuro usando l'operatore /
        dest_path = dest_dir / leg_subdir / year_subdir / filename
        
        # Debug path creation
        print(f"  📁 Path debug:")
        print(f"    dest_dir: {dest_dir} (tipo: {type(dest_dir)})")
        print(f"    leg_subdir: {leg_subdir}")
        print(f"    year_subdir: {year_subdir}")
        print(f"    filename: {filename}")
        print(f"    dest_path finale: {dest_path}")
        
        # Controllo sicurezza contro concatenazioni errate
        path_str = str(dest_path)
        if any(problem in path_str.lower() for problem in ["downloadscamera", "camera2025"]):
            error_msg = f"❌ PATH CAMERA MALFORMATO: {dest_path}"
            print(error_msg)
            return False
        
        if dest_path.exists():
            print(f"  ✓ Già esistente: {filename}")
            return True
        
        # Crea directory con path sicuro
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
    
    def find_seduta_range(self, leg: str, start_date: dt.date) -> Tuple[int, int]:
        """Trova il range di sedute da controllare"""
        print(f"  🔍 Cercando range di sedute valide...")
        
        # Prova alcune sedute campione per capire il range
        test_sedute = [1, 50, 100, 200, 300, 400, 500, 600]
        valid_sedute = []
        
        for sed_num in test_sedute:
            exists, date_obj = self.check_seduta_exists(leg, sed_num)
            if exists:
                valid_sedute.append((sed_num, date_obj))
                print(f"    ✓ Seduta {sed_num} esiste {f'({date_obj})' if date_obj else ''}")
            else:
                print(f"    ✗ Seduta {sed_num} non esiste")
            
            self._sleep_with_jitter(CONFIG["delays"]["between_requests"])
        
        if not valid_sedute:
            print("  ❌ Nessuna seduta trovata nel range testato")
            return 1, 100  # Fallback
        
        # Determina range basato sulle sedute trovate
        min_sed = min(s[0] for s in valid_sedute)
        max_sed = max(s[0] for s in valid_sedute)
        
        # Estendi il range per essere sicuri
        start_range = max(1, min_sed - 50)
        end_range = max_sed + 100
        
        print(f"  📊 Range stimato: sedute {start_range}-{end_range}")
        return start_range, end_range
    
    def download_for_legislature(self, leg: str, start_date: dt.date, output_dir: Path) -> bool:
        """Scarica tutti i documenti per una legislatura dalla data specificata"""
        print(f"🏛️  Camera dei Deputati - Legislatura {leg}")
        print(f"📅 Data di partenza: {start_date.isoformat()}")
        
        # ===== FIX CRITICO: Normalizza output_dir =====
        output_dir = Path(output_dir).resolve()
        print(f"📁 Directory output: {output_dir} (tipo: {type(output_dir)})")
        
        # Controllo sicurezza path
        if any(problem in str(output_dir).lower() for problem in ["downloadscamera", "camera2025"]):
            error_msg = f"❌ OUTPUT DIR MALFORMATA: {output_dir}"
            print(error_msg)
            return False
        
        # Trova il range di sedute da controllare
        start_sed, end_sed = self.find_seduta_range(leg, start_date)
        
        total_downloaded = 0
        total_errors = 0
        consecutive_missing = 0
        max_consecutive_missing = 20  # Fermati dopo 20 sedute consecutive mancanti
        
        print(f"\n📄 Controllo sedute da {start_sed} a {end_sed}...")
        
        for sed_num in range(start_sed, end_sed + 1):
            exists, date_obj = self.check_seduta_exists(leg, sed_num)
            
            if not exists:
                consecutive_missing += 1
                if consecutive_missing >= max_consecutive_missing:
                    print(f"  🛑 Troppe sedute consecutive mancanti ({max_consecutive_missing}). Fermata.")
                    break
                continue
            
            consecutive_missing = 0  # Reset counter
            
            # Filtra per data se disponibile
            if date_obj and date_obj < start_date:
                print(f"  ⏭️  Seduta {sed_num:04d} ({date_obj}) troppo vecchia, salto")
                continue
            
            # Scarica il PDF
            if self.download_pdf(leg, sed_num, date_obj, output_dir):
                total_downloaded += 1
            else:
                total_errors += 1
            
            # Sleep tra richieste
            self._sleep_with_jitter(CONFIG["delays"]["between_requests"])
        
        print(f"\n🏁 COMPLETATO - Scaricati: {total_downloaded}, Errori: {total_errors}")
        return total_errors == 0


def main():
    parser = argparse.ArgumentParser(
        description="Scarica resoconti stenografici della Camera dei Deputati - FIXED",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Esempi:
  # Scarica tutto dalla legislatura 19 dal 2024-05-01
  python download_camera_pdf_fixed.py --leg 19 --from 2024-05-01 --out ./downloads
  
  # Solo documenti dal 2023
  python download_camera_pdf_fixed.py --leg 19 --from 2023-01-01 --out ./camera_docs
        """
    )
    
    parser.add_argument("--leg", required=True,
                       help="Numero della legislatura (es. 19)")
    
    parser.add_argument("--from", dest="from_date",
                       type=lambda s: dt.datetime.strptime(s, "%Y-%m-%d").date(),
                       default=dt.date(1948, 1, 1),
                       help="Data minima da cui scaricare (YYYY-MM-DD)")
    
    parser.add_argument("--out", type=Path, required=True,
                       help="Cartella di output")
    
    args = parser.parse_args()
    
    # ===== FIX CRITICO: Normalizza args.out =====
    args.out = Path(args.out).resolve()
    print(f"📁 Directory output normalizzata: {args.out}")
    
    try:
        downloader = CameraPDFDownloader()
        success = downloader.download_for_legislature(args.leg, args.from_date, args.out)
        
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