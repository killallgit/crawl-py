import logging
import sqlite3
from .config import VideoInfo


log = logging.getLogger(__name__)

def init_database(db_path: str) -> None:
    """Initialize the SQLite database."""
    try:
        with sqlite3.connect(db_path) as conn:
            cursor = conn.cursor()
            log.info("Initializing database...")
            cursor.execute('''
                CREATE TABLE IF NOT EXISTS crawled (
                    video_id TEXT PRIMARY KEY,
                    url TEXT,
                    title TEXT,
                    text TEXT,
                    caption_url TEXT
                )
            ''')
            
            conn.commit()
    except Exception:
        log.exception("Database initialization error")
        # force exit
        exit(1)

def record_download(
    db_path: str,
    video_info: VideoInfo
) -> None:
    """Record a download in the database."""
    try:
        log.info(f"Recording download: {video_info.video_id}")
        with sqlite3.connect(db_path) as conn:
            cursor = conn.cursor()
            cursor.execute('''
                INSERT OR REPLACE INTO crawled
                (video_id, url, title, text, caption_url)
                VALUES (?, ?, ?, ?, ?)
            ''', (
                video_info.video_id,
                video_info.url,
                video_info.title,
                video_info.text,
                video_info.caption_url
            ))
            
            conn.commit()
    except Exception as e:
        logging.error(f"Download recording error: {e}")
        exit(1)

def is_video_downloaded(db_path: str, video_id: str) -> bool:
    """Check if a video has been downloaded."""
    try:
        log.info(f"Checking if video {video_id} has been downloaded")
        with sqlite3.connect(db_path) as conn:
            cursor = conn.cursor()
            cursor.execute("SELECT 1 FROM crawled WHERE video_id = ?", (video_id,))
            return cursor.fetchone() is not None
    except Exception as e:
        log.error(f"Download check error: {e}")
        return False


