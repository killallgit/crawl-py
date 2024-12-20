from typing import Any, Callable

from .metadata import create_video_info
from .db import record_download, is_video_downloaded
from .config import CrawlConfig
import logging

ProgressHook = Callable[[dict[str, Any]], None]
PostprocessHook = Callable[[dict[str, Any]], None]

log = logging.getLogger(__name__)

def create_info_hooks(config: CrawlConfig) -> tuple[ProgressHook, PostprocessHook]:
    def info_progress_hook(d: dict[str, Any]) -> None:
        info_dict = d["info_dict"]
        has_entry = is_video_downloaded(config.db_path, info_dict.get("id", ""))
        if has_entry:
            raise Exception(
                f"[info_progress_hook]: Entry exists for {info_dict.get('id', '')}"
            )
        return

    def info_posprocessor_hook(d: dict[str, Any]) -> None:
        info_dict = d["info_dict"]
        has_entry = is_video_downloaded(config.db_path, info_dict.get("id", ""))
        if has_entry:
            raise Exception(
                f"[info_postprocessor_hook]: Entry exists for {info_dict.get('id', '')}"
            )
        return

    return info_progress_hook, info_posprocessor_hook


def create_hooks(config: CrawlConfig) -> tuple[ProgressHook, PostprocessHook]:
    """Create progress and postprocess hooks."""

    def progress_hook(d: dict[str, Any]) -> None:
        info_dict = d["info_dict"]
        video_id = info_dict["id"]
        has_video = is_video_downloaded(db_path=config.db_path, video_id=video_id)
        if has_video:
            raise Exception(f"Found {video_id} in db")
        
        if d["status"] == "finished":
            record_download(config.db_path, create_video_info(info_dict))
            return

        return
        

    def postprocess_hook(d: dict[str, Any]) -> None:
        if d["status"] == "finished":
            log.info("Postprocessing complete")

    return progress_hook, postprocess_hook
