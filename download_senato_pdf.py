#!/usr/bin/env python3
"""
download_senato_pdf_fixed.py - v4 "Il Robusto"
================================================
Versione corretta e migliorata per scaricare i PDF del Senato.
Sistema di retry robusto e gestione errori migliorata.
"""
from __future__ import annotations  # ✅ CON import, non _import
import argparse
import datetime as dt
import json
import random
import re
import sys
import time
from pathlib import Path
from typing import List, Tuple, Optional
from urllib.parse import urljoin

import requests
from bs4 import BeautifulSoup

# Configurazione migliorata
CONFIG = {
    "delays": {
        "html": 1.5,
        "pdf": 0.5,
        "jitter": 0.3
    },
    "retries": {
        "max_attempts": 3,
        "backoff_factor": 2
    },
    "timeouts": {
        "html": 20,
        "pdf": 120
    },
    "headers": {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
                     "(KHTML, like Gecko) Chrome/123.0 Safari/537.36"
    }
}

# Mappatura mesi italiani
ITA_MONTHS = {
    "gennaio": 1, "febbraio": 2, "marzo": 3, "aprile": 4,
    "maggio": 5, "giugno": 6, "luglio": 7, "agosto": 8,
    "settembre": 9, "ottobre": 10, "novembre": 11, "dicembre": 12
}

BASE_URL_TEMPLATE = (
    "https://www.senato.it/legislature/{leg}/lavori/assemblea/"
    "resoconti-elenco-cronologico?year={year}"
)


class SenatoPDFDownloader:
    """Downloader robusto per i PDF del Senato"""
    
    def __init__(self):
        self.session = requests.Session()
        self.session.headers.update(CONFIG["headers"])
        
    def _sleep_with_jitter(self, base_delay: float):
        """Sleep con jitter randomico"""
        jitter = random.uniform(0, CONFIG["delays"]["jitter"])
        time.sleep(base_delay + jitter)
    
    def _parse_italian_date(self, date_text: str) -> Optional[dt.date]:
        """Converte una data italiana in oggetto date"""
        try:
            # Formato: "15 marzo 2024"
            parts = date_text.strip().split()
            if len(parts) != 3:
                return None
            
            day = int(parts[0])
            month_name = parts[1].lower()
            year = int(parts[2])
            
            if month_name not in ITA_MONTHS:
                return None
            
            month = ITA_MONTHS[month_name]
            return dt.date(year, month, day)
            
        except (ValueError, IndexError):
            return None
    
    def get_pdf_links_for_year(self, leg: str, year: int) -> List[Tuple[str, dt.date]]:
        """Ottiene tutti i link PDF per un anno specifico"""
        url = BASE_URL_TEMPLATE.format(leg=leg, year=year)
        
        for attempt in range(CONFIG["retries"]["max_attempts"]):
            try:
                print(f"  📡 Recupero elenco per anno {year} (tentativo {attempt + 1})...")
                
                response = self.session.get(url, timeout=CONFIG["timeouts"]["html"])
                
                if response.status_code == 404:
                    print(f"  ℹ️  Nessun elenco trovato per l'anno {year}")
                    return []
                
                response.raise_for_status()
                self._sleep_with_jitter(CONFIG["delays"]["html"])
                
                soup = BeautifulSoup(response.text, "html.parser")
                pdf_links = []
                
                for link in soup.select('a[href$=".pdf"]'):
                    # Estrae la data dal testo del link
                    date_text = link.get_text(strip=True).split("–", 1)[0].strip()
                    parsed_date = self._parse_italian_date(date_text)
                    
                    if parsed_date:
                        href = link.get("href")
                        if href:
                            full_url = urljoin("https://www.senato.it/", href)
                            pdf_links.append((full_url, parsed_date))
                
                print(f"  📄 Trovati {len(pdf_links)} PDF per l'anno {year}")
                return pdf_links
                
            except requests.exceptions.RequestException as e:
                print(f"  ⚠️  Errore nel recupero (tentativo {attempt + 1}): {e}")
                if attempt == CONFIG["retries"]["max_attempts"] - 1:
                    print(f"  ❌ Fallito recupero elenco per anno {year}")
                    return []
                
                # Backoff esponenziale
                wait_time = CONFIG["retries"]["backoff_factor"] ** attempt
                time.sleep(wait_time)
        
        return []
    
    def download_pdf(self, url: str, dest_path: Path) -> bool:
        """Scarica un singolo PDF con retry"""
        if dest_path.exists():
            print(f"  ✓ Già esistente: {dest_path.name}")
            return True
        
        dest_path.parent.mkdir(parents=True, exist_ok=True)
        
        for attempt in range(CONFIG["retries"]["max_attempts"]):
            try:
                print(f"  ⬇️  Scaricando: {dest_path.name} (tentativo {attempt + 1})...")
                
                with self.session.get(url, stream=True, timeout=CONFIG["timeouts"]["pdf"]) as response:
                    response.raise_for_status()
                    
                    # Verifica che sia effettivamente un PDF
                    content_type = response.headers.get("content-type", "").lower()
                    if "application/pdf" not in content_type:
                        print(f"  ⚠️  Tipo di contenuto non valido: {content_type}")
                        return False
                    
                    # Scarica in file temporaneo
                    temp_path = dest_path.with_suffix(".tmp")
                    with open(temp_path, "wb") as f:
                        for chunk in response.iter_content(chunk_size=8192):
                            if chunk:
                                f.write(chunk)
                    
                    # Atomicamente rinomina il file
                    temp_path.rename(dest_path)
                
                print(f"  ✅ Completato: {dest_path.name}")
                self._sleep_with_jitter(CONFIG["delays"]["pdf"])
                return True
                
            except requests.exceptions.RequestException as e:
                print(f"  ⚠️  Errore download (tentativo {attempt + 1}): {e}")
                
                # Pulizia file temporaneo
                temp_path = dest_path.with_suffix(".tmp")
                if temp_path.exists():
                    temp_path.unlink()
                
                if attempt == CONFIG["retries"]["max_attempts"] - 1:
                    print(f"  ❌ Fallito download di: {dest_path.name}")
                    return False
                
                wait_time = CONFIG["retries"]["backoff_factor"] ** attempt
                time.sleep(wait_time)
        
        return False
    
    def create_metadata_file(self, pdf_path: Path, date_obj: dt.date, leg: str) -> bool:
        """Crea il file di metadata JSON"""
        try:
            metadata = {
                "date": date_obj.isoformat(),
                "legislatura": leg,
                "source": "senato",
                "document_type": "stenographic_report",
                "institution": "senato_repubblica",
                "language": "it",
                "created_at": dt.datetime.now(dt.timezone.utc).isoformat()
            }
            
            metadata_path = pdf_path.with_suffix(".json")
            with open(metadata_path, "w", encoding="utf-8") as f:
                json.dump(metadata, f, ensure_ascii=False, indent=2)
            
            return True
            
        except Exception as e:
            print(f"  ⚠️  Errore creazione metadata: {e}")
            return False
    
    def download_for_legislature(self, leg: str, start_date: dt.date, output_dir: Path) -> bool:
        """Scarica tutti i documenti per una legislatura dalla data specificata"""
        print(f"🏛️  Inizio download Senato - Legislatura {leg}")
        print(f"📅 Data di partenza: {start_date.isoformat()}")
        print(f"📁 Directory output: {output_dir}")
        
        current_year = dt.date.today().year
        start_year = start_date.year
        
        total_downloaded = 0
        total_errors = 0
        
        # Processa anni dal più recente al più vecchio
        for year in range(current_year, start_year - 1, -1):
            print(f"\n📅 === ANNO {year} ===")
            
            pdf_links = self.get_pdf_links_for_year(leg, year)
            
            if not pdf_links:
                print(f"  📭 Nessun documento trovato per l'anno {year}")
                continue
            
            # Ordina per data (più vecchi prima)
            pdf_links.sort(key=lambda x: x[1])
            
            year_downloads = 0
            year_errors = 0
            
            for url, date_obj in pdf_links:
                # Filtra per data
                if date_obj < start_date:
                    continue
                
                # Costruisce il nome del file
                filename = f"senato_leg{leg}_{date_obj.isoformat()}_{url.rsplit('/', 1)[1]}"
                dest_path = output_dir / str(date_obj.year) / filename
                
                # Scarica il PDF
                if self.download_pdf(url, dest_path):
                    # Crea metadata
                    self.create_metadata_file(dest_path, date_obj, leg)
                    year_downloads += 1
                    total_downloaded += 1
                else:
                    year_errors += 1
                    total_errors += 1
            
            print(f"  📊 Anno {year}: {year_downloads} scaricati, {year_errors} errori")
        
        print(f"\n🏁 COMPLETATO - Totale: {total_downloaded} scaricati, {total_errors} errori")
        return total_errors == 0


def main():
    parser = argparse.ArgumentParser(
        description="Scarica resoconti stenografici del Senato",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Esempi:
  # Scarica tutto dalla legislatura 19 dal 2024-01-01
  python download_senato_pdf_fixed.py --leg 19 --from 2024-01-01 --out ./downloads
  
  # Scarica solo dal 2023 in poi
  python download_senato_pdf_fixed.py --leg 19 --from 2023-01-01 --out ./senato_docs
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
    
    try:
        downloader = SenatoPDFDownloader()
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