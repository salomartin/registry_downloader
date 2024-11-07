from typing import List
from abc import ABC, abstractmethod
from bs4 import BeautifulSoup
from ..core.client import HTTPClient

class URLFetcher(ABC):
    @abstractmethod
    async def get_urls(self, url: str, only_last: bool = False) -> List[str]:
        pass

class BaseDownloader(URLFetcher):
    async def _fetch_and_parse(self, url: str) -> BeautifulSoup:
        response = await HTTPClient.fetch(url)
        return BeautifulSoup(response.text, 'html.parser')

    def _get_full_url(self, href: str, root_domain: str) -> str:
        return href if href.startswith('http') else root_domain + href

    async def get_urls(self, url: str, only_last: bool = False) -> List[str]:
        root_domain = '/'.join(url.split('/')[:3])
        soup = await self._fetch_and_parse(url)
        
        data_urls = list(set([
            self._get_full_url(a['href'], root_domain)
            for a in soup.find_all('a', href=True) 
            if a['href'].endswith(('.csv', '.json'))
        ]))
        
        if only_last and data_urls:
            data_urls = [data_urls[-1]]
            
        return data_urls