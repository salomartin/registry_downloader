from typing import List
from .base import BaseDownloader

class FinnishDownloader(BaseDownloader):
    async def get_urls(self, url: str, only_last: bool = False) -> List[str]:
        # Finnish downloader always uses only_last=True
        return await super().get_urls(url, only_last=True)