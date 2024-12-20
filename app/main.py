from crawler.file_io import ensure_directory
from crawler.db import init_database
from crawler.config import CrawlConfig
from crawler.crawl import crawl

config = CrawlConfig(search_queries=["podcast"])

if __name__ == "__main__":
  try:
    ensure_directory(config.output_dir)
    ensure_directory(config.download_dir)
    init_database(config.db_path)
    crawl(config)
  except Exception:
    exit(1)
