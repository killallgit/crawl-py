import logging
from typing import Any

import yt_dlp
from .config import CrawlConfig
from .db import is_video_downloaded
from .ytdlp_config import create_download_options

log = logging.getLogger(__name__)

def precrawl_gather_info(config: CrawlConfig, opts: dict[str, Any]):
    with yt_dlp.YoutubeDL(opts) as ydl:
        info = ydl.extract_info(f"ytsearch{config.max_search}:{config.search_queries}", download=False)
        return info.get("entries", []) if info else []
 
def crawl(
    config: CrawlConfig,
) -> None:
    """Download commercials with comprehensive filtering."""
    opts = create_download_options(config)
    entities_to_download = precrawl_gather_info(config, opts)
    if len(entities_to_download) == 0:
        log.info("No videos found to download")
        return
    for entity in entities_to_download:
        video_id = entity.get("id")
        url = entity.get("original_url")

        if is_video_downloaded(config.db_path, video_id):
            log.info(f"Found db entry for: {video_id}")
            continue
        
        with yt_dlp.YoutubeDL(opts) as ydl:
            ydl.download([url])
        