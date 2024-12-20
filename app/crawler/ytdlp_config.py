from typing import Any

from yt_dlp.utils import download_range_func

from .config import CrawlConfig
from .hooks import create_hooks

SUB_FORMAT = "srt"

def create_download_options(config: CrawlConfig) -> dict[str, Any]:
    """Create download options for yt-dlp."""
    progress_hook, postprocess_hook = create_hooks(config)
    return {
        "format": "bestaudio/best",
        "ignoreerrors": True,
        "no_warnings": True,
        "progress_hooks": [progress_hook],
        "postprocessor_hooks": [postprocess_hook],
        "subtitleslangs": ["en"],
        "postprocessors": [
            {
                "key": "FFmpegExtractAudio",
                "preferredcodec": "wav",
                "preferredquality": "160",
            }
        ],
        "outtmpl": f"{config.download_dir}/%(id)s.%(ext)s",
        "download_ranges": download_range_func(ranges=[(config.video_start_time,config.video_end_time,)], chapters=None),
        "force_keyframes_at_cuts": True,
    }
