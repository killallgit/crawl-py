import os
import sqlite3
import yt_dlp
import logging
from datetime import datetime
from typing import List, Dict, Any, Optional, Callable, Union

# Logging Configuration
def setup_logging(level: int = logging.INFO) -> logging.Logger:
    """Set up and return a configured logger."""
    logging.basicConfig(
        level=level, 
        format='%(asctime)s - %(levelname)s: %(message)s'
    )
    return logging.getLogger(__name__)

# Directory Management
def ensure_directory(directory: str) -> str:
    """Ensure directory exists and return its path."""
    os.makedirs(directory, exist_ok=True)
    return directory

# Database Functions
def init_database(db_path: str) -> None:
    """Initialize the SQLite database."""
    try:
        with sqlite3.connect(db_path) as conn:
            cursor = conn.cursor()
            
            # Downloads tracking table
            cursor.execute('''
                CREATE TABLE IF NOT EXISTS downloaded_videos (
                    video_id TEXT PRIMARY KEY,
                    title TEXT,
                    download_date TEXT,
                    duration INTEGER,
                    source_query TEXT
                )
            ''')
            
            conn.commit()
    except Exception as e:
        logging.error(f"Database initialization error: {e}")

def record_download(
    db_path: str, 
    video_info: Dict[str, Any], 
    source_query: str
) -> None:
    """Record a download in the database."""
    try:
        with sqlite3.connect(db_path) as conn:
            cursor = conn.cursor()
            
            cursor.execute('''
                INSERT OR REPLACE INTO downloaded_videos 
                (video_id, title, download_date, duration, source_query)
                VALUES (?, ?, ?, ?, ?)
            ''', (
                video_info.get('id', ''),
                video_info.get('title', ''),
                datetime.now().isoformat(),
                video_info.get('duration', 0),
                source_query
            ))
            
            conn.commit()
    except Exception as e:
        logging.error(f"Download recording error: {e}")

def is_video_downloaded(db_path: str, video_id: str) -> bool:
    """Check if a video has been downloaded."""
    try:
        with sqlite3.connect(db_path) as conn:
            cursor = conn.cursor()
            cursor.execute("SELECT 1 FROM downloaded_videos WHERE video_id = ?", (video_id,))
            return cursor.fetchone() is not None
    except Exception as e:
        logging.error(f"Download check error: {e}")
        return False

# File Management
def find_audio_file(directory: str, video_id: str) -> Optional[str]:
    """Find the audio file for a given video ID."""
    audio_files = [
        os.path.join(directory, f) 
        for f in os.listdir(directory) 
        if video_id in f and f.endswith('.wav')
    ]
    return audio_files[0] if audio_files else None

# Download Management
def create_download_options(
    output_directory: str, 
    progress_hook: Callable[[Dict[str, Any]], None], 
    postprocess_hook: Callable[[Dict[str, Any]], None]
) -> Dict[str, Any]:
    """Create download options for yt-dlp."""
    return {
        'format': 'bestaudio/best',
        'overwrites': False,
        'ignoreerrors': True,
        'keepvideo': False,
        'no_warnings': True,
        'progress_hooks': [progress_hook],
        'postprocessor_hooks': [postprocess_hook],
        'postprocessors': [{
            'key': 'FFmpegExtractAudio',
            'preferredcodec': 'wav',
            'preferredquality': '192',
        }],
        'outtmpl': os.path.join(output_directory, '%(id)s.%(ext)s'),
    }

# Define type for hooks more explicitly
ProgressHook = Callable[[Dict[str, Any]], None]
PostprocessHook = Callable[[Dict[str, Any]], None]

def create_hooks(
    audio_directory: str, 
    db_path: str, 
    logger: logging.Logger
) -> tuple[ProgressHook, PostprocessHook]:
    """Create progress and postprocess hooks."""
    def progress_hook(d: Dict[str, Any]) -> None:
        if d['status'] != 'finished':
            return
        
        info_dict = d.get('info_dict', {})
        video_id = info_dict.get('id', '')
        
        # Find the audio file
        audio_path = find_audio_file(audio_directory, video_id)
        
        if not audio_path:
            logger.warning(f"No audio file found for {info_dict.get('title', '')}")
            return
        
        # Record the download
        record_download(db_path, info_dict, '')
        logger.info(f"Processed: {os.path.basename(audio_path)}")

    def postprocess_hook(d: Dict[str, Any]) -> None:
        if d['status'] == 'finished':
            logger.info("Postprocessing complete")

    return progress_hook, postprocess_hook

def download_commercials(
    search_queries: List[str], 
    output_directory: str, 
    db_path: str,
    logger: logging.Logger,
    max_duration: int = 360
) -> None:
    """Download commercials with comprehensive filtering."""
    audio_directory = ensure_directory(os.path.join(output_directory, 'audio'))
    
    # Create hooks
    progress_hook, postprocess_hook = create_hooks(audio_directory, db_path, logger)

    # Create download options
    ydl_opts = create_download_options(
        audio_directory, 
        progress_hook, 
        postprocess_hook
    )

    for query in search_queries:
        try:
            with yt_dlp.YoutubeDL(ydl_opts) as ydl:
                info = ydl.extract_info(f"ytsearch50:{query}", download=True)
                
                for entry in info.get('entries', []): # type: ignore
                    if not entry:
                        continue
                    
                    # Skip if already downloaded
                    if is_video_downloaded(db_path, entry.get('id', '')):
                        logger.info(f"Skipping already downloaded video: {entry.get('title')}")
                        continue
                    
                    # Check duration
                    duration = entry.get('duration', 0)
                    if duration > max_duration:
                        logger.info(f"Skipping video too long: {entry.get('title')} ({duration}s)")
                        continue
        
        except Exception as e:
            logger.error(f"Error in query {query}: {e}")

def main():
    # Configuration
    output_directory = './crawled-audio'
    db_path = os.path.join(output_directory, 'download_tracker.db')
    
    # Setup
    logger = setup_logging()
    init_database(db_path)
    
    # Search Queries (truncated for brevity)
    search_queries = [
        "official commercial", 
        "TV commercial", 
        "advertisement",
        # ... other queries
    ]

    # Download
    download_commercials(
        search_queries, 
        output_directory, 
        db_path, 
        logger
    )

if __name__ == "__main__":
    main()
