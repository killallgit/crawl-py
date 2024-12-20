from typing import Any
from .config import VideoInfo


def _find_dict_with_value(dicts, key, value):
    return next((d for d in dicts if d.get(key) == value), None)


def _get_caption_url(entity: dict[str, dict[str, list[dict[str, str]]]]):
    auto_captions = entity.get("automatic_captions")
    en_list = auto_captions.get("en") if auto_captions else None
    elem = _find_dict_with_value(en_list, "ext", "vtt") if en_list else None
    captions_url = elem.get("url", "") if elem else None
    return captions_url if captions_url else ""


def create_video_info(info_dict: dict[str, Any]):
    video_id = info_dict.get("id", "")
    title = info_dict.get("title", "")
    url = info_dict.get("url", "")
    caption_url = _get_caption_url(info_dict)
    duration = info_dict.get("duration", -1)
    return VideoInfo(
        video_id=video_id,
        url=url,
        title=title,
        caption_url=caption_url,
        duration=duration,
    )
