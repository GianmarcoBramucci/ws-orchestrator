#!/usr/bin/env python3
"""
download_youtube_transcripts.py - v1.1 "Super Smart YouTube Scraper FIXED"
==========================================================================
Scraper YouTube super intelligente che:
1. Usa YouTube Data API v3 per metadata e lista video
2. Estrae trascrizioni con youtube-transcript-api
3. Supporta filtering per date e gestione multi-canale
4. Completamente compatibile con sistema esistente
5. Rate limiting intelligente e error handling robusto
6. FIXED: API response handling corretto
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
from typing import Optional, Dict, List, Tuple, Set
from concurrent.futures import ThreadPoolExecutor, as_completed
from threading import Semaphore
import random

# Import lazy per librerie YouTube
try:
    from googleapiclient.discovery import build
    from googleapiclient.errors import HttpError
    from youtube_transcript_api import YouTubeTranscriptApi
    from youtube_transcript_api._errors import TranscriptsDisabled, VideoUnavailable, NoTranscriptFound
    from tqdm import tqdm
except ImportError as e:
    sys.exit(f"❌ Libreria mancante: {e}. Installa con: pip install google-api-python-client youtube-transcript-api tqdm")

# ───────────────────────────── CONFIG
CHANNELS = {
    "UC6wP9lyGnU9Znt4idvQhKLg": "giorgiameloni",      # GiorgiaMeloniUfficiale  
    "UCp8W1bzofvzB8MfZSkYW2xw": "fratelliditalia",    # FratellidItaliaTV
    "UC74FLAfxj6U1Q8O67hz8XjQ": "palazzochigi"        # palazzochigi
}

DEFAULT_API_KEY = "AIzaSyDbIPtC3WaFT0CNZsPh45-VV4mSeuBnsMg"

# Rate limiting config
CONFIG = {
    "api_delay": 1.0,          # Delay tra chiamate API
    "transcript_delay": 0.5,   # Delay tra download trascrizioni
    "jitter": 0.3,             # Jitter randomico
    "max_workers": 8,          # Thread concorrenti
    "retries": 3,              # Retry per errori
    "timeout": 30,             # Timeout requests
    "chunk_size": 50           # Video per batch API
}

TRANSCRIPT_LANGUAGES = ["it", "en", "auto"]  # Priorità lingue trascrizioni
# ───────────────────────────── END CONFIG


class SuperSmartYouTubeScraper:
    """Scraper YouTube super intelligente per canali politici italiani"""
    
    def __init__(self, api_key: str):
        self.api_key = api_key
        self.youtube = build('youtube', 'v3', developerKey=api_key)
        self.transcript_api = YouTubeTranscriptApi()
        self.rate_limiter = Semaphore(CONFIG["max_workers"])
        self.processed_videos = set()  # Cache per evitare duplicati
        self.quota_used = 0
        
    def _sleep_with_jitter(self, base_delay: float):
        """Sleep con jitter randomico per evitare pattern detection"""
        jitter = random.uniform(0, CONFIG["jitter"])
        time.sleep(base_delay + jitter)
    
    def _safe_api_call(self, request_object):
        """Wrapper sicuro per chiamate API con retry e rate limiting - FIXED"""
        for attempt in range(CONFIG["retries"]):
            try:
                self._sleep_with_jitter(CONFIG["api_delay"])
                # FIX: Execute the request object to get actual response
                result = request_object.execute()
                self.quota_used += 1  # Tracking quota usage
                return result
            except HttpError as e:
                if e.resp.status == 403:
                    if "quota" in str(e).lower():
                        print(f"❌ Quota API esaurita! Usate {self.quota_used} unità")
                        sys.exit(1)
                    elif attempt == CONFIG["retries"] - 1:
                        print(f"❌ Errore API 403: {e}")
                        return None
                elif e.resp.status == 404:
                    print(f"⚠️  Risorsa non trovata: {e}")
                    return None
                else:
                    print(f"⚠️  Errore API {e.resp.status} (tentativo {attempt + 1}): {e}")
                    
                if attempt < CONFIG["retries"] - 1:
                    time.sleep(2 ** attempt)  # Exponential backoff
            except Exception as e:
                print(f"⚠️  Errore generico (tentativo {attempt + 1}): {e}")
                if attempt < CONFIG["retries"] - 1:
                    time.sleep(2 ** attempt)
                    
        return None
    
    def get_channel_info(self, channel_id: str) -> Optional[Dict]:
        """Ottiene informazioni del canale"""
        print(f"  📡 Recupero info canale {CHANNELS.get(channel_id, channel_id)}...")
        
        # FIX: Create request object first, then execute it
        request = self.youtube.channels().list(
            part='snippet,statistics',
            id=channel_id
        )
        
        response = self._safe_api_call(request)
        
        if not response or not response.get('items'):
            print(f"  ❌ Canale {channel_id} non trovato")
            return None
            
        channel = response['items'][0]
        return {
            'id': channel['id'],
            'title': channel['snippet']['title'],
            'description': channel['snippet']['description'],
            'subscriber_count': channel['statistics'].get('subscriberCount', 'unknown'),
            'video_count': channel['statistics'].get('videoCount', 'unknown'),
            'view_count': channel['statistics'].get('viewCount', 'unknown')
        }
    
    def get_channel_videos(self, channel_id: str, start_date: Optional[dt.date] = None, 
                          end_date: Optional[dt.date] = None) -> List[str]:
        """Ottiene lista video ID di un canale con filtro date"""
        print(f"  📼 Recupero video canale {CHANNELS.get(channel_id, channel_id)}...")
        
        # Prima ottieni l'uploads playlist ID - FIX
        channel_request = self.youtube.channels().list(
            part='contentDetails',
            id=channel_id
        )
        
        channel_response = self._safe_api_call(channel_request)
        
        if not channel_response or not channel_response.get('items'):
            return []
            
        uploads_playlist = channel_response['items'][0]['contentDetails']['relatedPlaylists']['uploads']
        
        # Recupera tutti i video dalla playlist uploads
        video_ids = []
        next_page_token = None
        
        while True:
            # FIX: Create request object properly
            playlist_request = self.youtube.playlistItems().list(
                part='snippet',
                playlistId=uploads_playlist,
                maxResults=50,
                pageToken=next_page_token
            )
            
            playlist_response = self._safe_api_call(playlist_request)
            
            if not playlist_response:
                break
                
            for item in playlist_response.get('items', []):
                video_id = item['snippet']['resourceId']['videoId']
                
                # Filtra per data se specificato
                if start_date or end_date:
                    published = dt.datetime.fromisoformat(
                        item['snippet']['publishedAt'].replace('Z', '+00:00')
                    ).date()
                    
                    if start_date and published < start_date:
                        continue
                    if end_date and published > end_date:
                        continue
                        
                video_ids.append(video_id)
            
            next_page_token = playlist_response.get('nextPageToken')
            if not next_page_token:
                break
                
            print(f"    📄 Recuperati {len(video_ids)} video...")
        
        print(f"  ✅ Trovati {len(video_ids)} video nel range di date")
        return video_ids
    
    def get_video_metadata(self, video_ids: List[str]) -> List[Dict]:
        """Ottiene metadata dettagliato per lista di video (batch processing)"""
        all_videos = []
        
        # Processa in chunk per rispettare limiti API
        for i in range(0, len(video_ids), CONFIG["chunk_size"]):
            chunk = video_ids[i:i + CONFIG["chunk_size"]]
            
            # FIX: Create request object properly
            video_request = self.youtube.videos().list(
                part='snippet,statistics,contentDetails,liveStreamingDetails',
                id=','.join(chunk)
            )
            
            response = self._safe_api_call(video_request)
            
            if response and response.get('items'):
                all_videos.extend(response['items'])
                
        return all_videos
    
    def extract_transcript(self, video_id: str) -> Dict:
        """Estrae trascrizione con fallback strategy"""
        try:
            self._sleep_with_jitter(CONFIG["transcript_delay"])
            
            # Prova diverse lingue in ordine di priorità
            for lang in TRANSCRIPT_LANGUAGES:
                try:
                    if lang == "auto":
                        # Auto-generated captions
                        transcript = self.transcript_api.get_transcript(video_id)
                    else:
                        # Manual captions in specified language
                        transcript = self.transcript_api.get_transcript(video_id, languages=[lang])
                    
                    # Combina tutto il testo
                    full_text = ' '.join([entry['text'] for entry in transcript])
                    
                    return {
                        'success': True,
                        'language': lang,
                        'type': 'auto_generated' if lang == 'auto' else 'manual',
                        'content': full_text,
                        'entries': transcript,
                        'length': len(transcript)
                    }
                    
                except (TranscriptsDisabled, NoTranscriptFound):
                    continue
                    
        except VideoUnavailable:
            return {'success': False, 'error': 'Video unavailable'}
        except Exception as e:
            return {'success': False, 'error': f'Transcript error: {str(e)}'}
            
        return {'success': False, 'error': 'No transcripts available'}
    
    def process_single_video(self, video_data: Dict, channel_slug: str, output_dir: Path) -> bool:
        """Processa un singolo video con metadata e trascrizione"""
        video_id = video_data['id']
        
        # Evita duplicati
        if video_id in self.processed_videos:
            return True
            
        try:
            snippet = video_data['snippet']
            statistics = video_data.get('statistics', {})
            content_details = video_data.get('contentDetails', {})
            
            # Determina tipo di video
            is_live = 'liveStreamingDetails' in video_data
            video_type = 'live_stream' if is_live else 'video'
            
            # Parse data pubblicazione
            published_at = dt.datetime.fromisoformat(
                snippet['publishedAt'].replace('Z', '+00:00')
            )
            
            # Crea struttura directory: channel_slug/YYYY/MM/
            date_dir = output_dir / channel_slug / str(published_at.year) / f"{published_at.month:02d}"
            date_dir.mkdir(parents=True, exist_ok=True)
            
            # Nome file basato su data e titolo
            safe_title = re.sub(r'[^\w\s-]', '', snippet['title'])[:50]
            safe_title = re.sub(r'\s+', '_', safe_title.strip())
            
            base_filename = f"{published_at.strftime('%Y-%m-%d')}_{video_type}_{safe_title}_{video_id}"
            
            # Estrai trascrizione
            print(f"    📝 Estraendo trascrizione per {snippet['title'][:50]}...")
            transcript_data = self.extract_transcript(video_id)
            
            # Crea metadata JSON completo
            metadata = {
                'video_id': video_id,
                'channel_id': snippet['channelId'],
                'channel_slug': channel_slug,
                'title': snippet['title'],
                'description': snippet.get('description', ''),
                'published_at': published_at.isoformat(),
                'duration': content_details.get('duration', ''),
                'video_type': video_type,
                'language': snippet.get('defaultLanguage', 'it'),
                'tags': snippet.get('tags', []),
                'category_id': snippet.get('categoryId'),
                
                # Statistics
                'view_count': int(statistics.get('viewCount', 0)),
                'like_count': int(statistics.get('likeCount', 0)),
                'comment_count': int(statistics.get('commentCount', 0)),
                
                # Live stream specifics
                'is_live': is_live,
                'live_details': video_data.get('liveStreamingDetails', {}),
                
                # Transcript info
                'transcript': transcript_data,
                
                # Technical metadata compatibili con sistema esistente
                'scraped_at': dt.datetime.now(dt.timezone.utc).isoformat(),
                'scraper_version': '1.1',
                'source': 'youtube',
                'document_type': 'video_transcript',
                'api_quota_used': self.quota_used,
                
                # Extra per compatibilità con upload_gcs_ingest.py
                'date': published_at.date().isoformat(),  # Importante per filtering
                'created_at': dt.datetime.now(dt.timezone.utc).isoformat()
            }
            
            # Salva metadata JSON
            metadata_path = date_dir / f"{base_filename}.json"
            with open(metadata_path, 'w', encoding='utf-8') as f:
                json.dump(metadata, f, ensure_ascii=False, indent=2)
            
            # Salva trascrizione come file separato se disponibile
            if transcript_data.get('success') and transcript_data.get('content'):
                transcript_path = date_dir / f"{base_filename}.txt"
                with open(transcript_path, 'w', encoding='utf-8') as f:
                    f.write(f"# {snippet['title']}\n")
                    f.write(f"# Video ID: {video_id}\n")
                    f.write(f"# Published: {published_at.isoformat()}\n")
                    f.write(f"# Language: {transcript_data.get('language', 'unknown')}\n")
                    f.write(f"# Type: {transcript_data.get('type', 'unknown')}\n\n")
                    f.write(transcript_data['content'])
            
            self.processed_videos.add(video_id)
            return True
            
        except Exception as e:
            print(f"    ❌ Errore processing video {video_id}: {e}")
            return False
    
    def download_channel(self, channel_id: str, start_date: Optional[dt.date], 
                        end_date: Optional[dt.date], output_dir: Path) -> Tuple[int, int]:
        """Scarica tutti i video di un canale con multi-threading"""
        channel_slug = CHANNELS.get(channel_id, channel_id)
        print(f"\n🎥 Processing canale: {channel_slug.upper()} ({channel_id})")
        
        # Ottieni info canale
        channel_info = self.get_channel_info(channel_id)
        if not channel_info:
            return 0, 0
            
        print(f"  📊 Canale: {channel_info['title']}")
        print(f"  📈 Video totali: {channel_info['video_count']}")
        print(f"  👥 Iscritti: {channel_info['subscriber_count']}")
        
        # Ottieni lista video
        video_ids = self.get_channel_videos(channel_id, start_date, end_date)
        if not video_ids:
            print(f"  ❌ Nessun video trovato nel range di date")
            return 0, 0
        
        # Ottieni metadata per tutti i video
        print(f"  📋 Recupero metadata per {len(video_ids)} video...")
        videos_metadata = self.get_video_metadata(video_ids)
        
        # Processa video con multi-threading
        print(f"  🚀 Processing {len(videos_metadata)} video con {CONFIG['max_workers']} thread...")
        
        downloaded = 0
        errors = 0
        
        with ThreadPoolExecutor(max_workers=CONFIG["max_workers"]) as executor:
            futures = {
                executor.submit(self.process_single_video, video, channel_slug, output_dir): video
                for video in videos_metadata
            }
            
            for future in tqdm(as_completed(futures), total=len(futures), desc=f"📥 {channel_slug}"):
                try:
                    if future.result():
                        downloaded += 1
                    else:
                        errors += 1
                except Exception as e:
                    print(f"    💥 Errore thread: {e}")
                    errors += 1
        
        print(f"  ✅ Canale {channel_slug} completato: {downloaded} video, {errors} errori")
        return downloaded, errors
    
    def smart_multi_channel_download(self, channel_ids: List[str], start_date: Optional[dt.date], 
                                   end_date: Optional[dt.date], output_dir: Path) -> bool:
        """Download intelligente multi-canale"""
        print(f"🔥 YOUTUBE SUPER SCRAPER v1.1 FIXED")
        print(f"📅 Range date: {start_date.isoformat() if start_date else 'inizio'} - {end_date.isoformat() if end_date else 'oggi'}")
        print(f"📁 Output directory: {output_dir.resolve()}")
        print(f"📊 Quota iniziale usata: {self.quota_used}")
        
        total_downloaded = 0
        total_errors = 0
        
        for i, channel_id in enumerate(channel_ids, 1):
            print(f"\n{'='*60}")
            print(f"CANALE {i}/{len(channel_ids)}: {CHANNELS.get(channel_id, channel_id)}")
            print(f"{'='*60}")
            
            downloaded, errors = self.download_channel(channel_id, start_date, end_date, output_dir)
            total_downloaded += downloaded
            total_errors += errors
        
        print(f"\n{'='*60}")
        print(f"🎉 YOUTUBE SCRAPING COMPLETATO!")
        print(f"📈 Totale video processati: {total_downloaded}")
        print(f"❌ Totale errori: {total_errors}")
        print(f"📊 Quota API totale usata: {self.quota_used}")
        print(f"🎥 Canali processati: {', '.join([CHANNELS.get(cid, cid) for cid in channel_ids])}")
        print(f"{'='*60}")
        
        return total_errors == 0


def main():
    parser = argparse.ArgumentParser(
        description="Super Smart YouTube Scraper v1.1 FIXED",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Esempi:
  # Scarica tutti i canali dal 2024
  python download_youtube_transcripts.py --from 2024-01-01 --out ./downloads
  
  # Scarica canale specifico con range di date
  python download_youtube_transcripts.py --channel UC6wP9lyGnU9Znt4idvQhKLg --from 2023-01-01 --to 2024-12-31 --out ./downloads
  
  # Scarica tutto fino ad oggi con API key custom
  python download_youtube_transcripts.py --api-key YOUR_KEY --out ./downloads
        """
    )
    
    parser.add_argument("--out", type=Path, required=True,
                       help="Cartella di output")
    
    parser.add_argument("--from", dest="from_date", 
                       type=lambda s: dt.datetime.strptime(s, "%Y-%m-%d").date(),
                       help="Data iniziale (YYYY-MM-DD)")
    
    parser.add_argument("--to", dest="to_date",
                       type=lambda s: dt.datetime.strptime(s, "%Y-%m-%d").date(),
                       help="Data finale (YYYY-MM-DD) - default: oggi")
    
    parser.add_argument("--channel", dest="channel_id",
                       help="ID canale specifico da scaricare")
    
    parser.add_argument("--api-key", default=DEFAULT_API_KEY,
                       help="YouTube Data API v3 key")
    
    args = parser.parse_args()
    
    # Determina canali da processare
    if args.channel_id:
        if args.channel_id not in CHANNELS:
            print(f"❌ Canale {args.channel_id} non supportato")
            print(f"💡 Canali disponibili: {', '.join(CHANNELS.keys())}")
            sys.exit(1)
        channel_ids = [args.channel_id]
    else:
        channel_ids = list(CHANNELS.keys())
    
    try:
        scraper = SuperSmartYouTubeScraper(args.api_key)
        success = scraper.smart_multi_channel_download(
            channel_ids,
            args.from_date,
            args.to_date,
            args.out
        )
        
        if success:
            print("\n🎉 Scraping YouTube completato con successo!")
            sys.exit(0)
        else:
            print("\n⚠️  Scraping completato con alcuni errori")
            sys.exit(1)
            
    except KeyboardInterrupt:
        print("\n🛑 Scraping interrotto dall'utente")
        sys.exit(130)
        
    except Exception as e:
        print(f"\n💥 Errore fatale: {e}")
        import traceback
        traceback.print_exc()
        sys.exit(1)


if __name__ == "__main__":
    main()