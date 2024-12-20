from pydantic import BaseModel, ConfigDict


class VideoInfo(BaseModel):
    video_id: str = ""
    url: str = ""
    title: str = ""
    text: str = ""
    caption_url: str = ""
    duration: int = -1
    class_labels: list[str] = []


class CrawlConfig(BaseModel):
    output_dir: str = "/Users/ryan/Code/killallgit/crawler-py/crawled"
    db_path: str = "/Users/ryan/Code/killallgit/crawler-py/crawled/crawler.db"
    download_dir: str = "/Users/ryan/Code/killallgit/crawler-py/crawled/downloaded"

    max_duration: float = 30.0
    max_download: int = 10
    max_search: int = 10

    search_queries: list[str] = ["podcast"]

    video_start_time: float = 15.0
    video_end_time: float = 45.0

    model_config = ConfigDict(arbitrary_types_allowed=True)
