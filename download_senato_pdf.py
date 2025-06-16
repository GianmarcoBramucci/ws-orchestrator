# SOSTITUISCI TUTTO il contenuto di download_senato_pdf.py con questo.
#!/usr/bin/env python3
"""
download_senato_pdf.py - v3 "Il Risolutore"
Logica di download corretta, itera sugli anni in modo più affidabile.
"""
from __future_ import annotations
import argparse, datetime as _dt, random, re, sys, time, requests, json
from pathlib import Path
from typing import List, Tuple
from urllib.parse import urljoin
from bs4 import BeautifulSoup

DELAY_HTML, DELAY_PDF = 0.5, 0.2
JITTER = 0.2
RETRIES, BACKOFF = 3, 5
HEADERS = {"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36"}
BASE_URL = "https://www.senato.it/legislature/{leg}/lavori/assemblea/resoconti-elenco-cronologico?year={year}"

_ITA2MON = {m: i + 1 for i, m in enumerate("gennaio febbraio marzo aprile maggio giugno luglio agosto settembre ottobre novembre dicembre".split())}
session = requests.Session()
session.headers.update(HEADERS)

def _sleep(base, jitter): time.sleep(base + random.uniform(0, jitter))

def ita_date(s:str) -> _dt.date:
    d, m, y = s.split()
    return _dt.date(int(y), _ITA2MON[m.lower()], int(d))

def parse_pdf_links(leg: str, year: int) -> List[Tuple[str, _dt.date]]:
    url = BASE_URL.format(leg=leg, year=year)
    try:
        r = session.get(url, timeout=20)
        r.raise_for_status()
    except requests.exceptions.RequestException as e:
        print(f"[!] Errore nel contattare {url}: {e}", file=sys.stderr)
        return []
    
    _sleep(DELAY_HTML, JITTER)
    soup = BeautifulSoup(r.text, "html.parser")
    out = []
    for a in soup.select('a[href$=".pdf"]'):
        txt_date = a.get_text(strip=True).split("–", 1)[0].strip()
        try:
            d = ita_date(txt_date)
            href = a.get("href")
            if href: out.append((urljoin("https://www.senato.it/", href), d))
        except (ValueError, KeyError): continue
    return out

def download(url: str, dest: Path):
    if dest.exists():
        print(f"~ File esiste già: {dest.name}")
        return
    dest.parent.mkdir(parents=True, exist_ok=True)
    for att in range(1, RETRIES + 1):
        try:
            with session.get(url, stream=True, timeout=60) as r:
                r.raise_for_status()
                tmp = dest.with_suffix(".part")
                with open(tmp, "wb") as f:
                    for chunk in r.iter_content(8192): f.write(chunk)
                tmp.rename(dest)
                print(f"✓ Scaricato: {dest.name}")
                _sleep(DELAY_PDF, JITTER)
                return
        except requests.exceptions.RequestException:
            if att == RETRIES: print(f"✗ Fallito download di {dest.name}", file=sys.stderr); return
            time.sleep(BACKOFF)

def main(argv=None):
    parser = argparse.ArgumentParser(description="Scarica resoconti PDF del Senato.")
    parser.add_argument("--leg", required=True, help="Numero della legislatura (es. 19).")
    parser.add_argument("--from", dest="from_date", type=lambda s: _dt.datetime.strptime(s, "%Y-%m-%d").date(), default=_dt.date(1948, 1, 1), help="Data minima da cui scaricare (YYYY-MM-DD).")
    parser.add_argument("--out", type=Path, required=True, help="Cartella di output.")
    args = parser.parse_args(argv)

    start_year = args.from_date.year
    end_year = _dt.date.today().year
    
    print(f"Scansiono anni dal {start_year} al {end_year} per la legislatura {args.leg}, cercando documenti dal {args.from_date.isoformat()}...")

    for year in range(end_year, start_year - 1, -1):
        print(f"\n--- Anno {year} ---")
        links = parse_pdf_links(args.leg, year)
        if not links:
            print("Nessun link trovato.")
            continue
        
        found_in_year = False
        for url, date_obj in reversed(links): # Partiamo dai più vecchi dell'anno
            if date_obj >= args.from_date:
                found_in_year = True
                fname = f"senato_leg{args.leg}_{date_obj.isoformat()}_{url.rsplit('/', 1)[1]}"
                dest_path = args.out / str(date_obj.year) / fname
                download(url, dest_path)
                meta_path = dest_path.with_suffix(".json")
                if not meta_path.exists() and dest_path.exists():
                    meta_info = {"date": date_obj.isoformat(), "legislatura": args.leg, "source": "senato"}
                    with open(meta_path, "w", encoding="utf-8") as fh:
                        json.dump(meta_info, fh, ensure_ascii=False)
        if not found_in_year:
             print("Nessun documento in questo anno supera il filtro data.")
    
    print("Download per il Senato completato.")


if __name__ == "__main__":
    main()