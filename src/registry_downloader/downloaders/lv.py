from typing import List, Set
from .base import BaseDownloader

class LatvianDownloader(BaseDownloader):
    async def get_urls(self, url: str, only_last: bool = False) -> List[str]:
        root_domain = '/'.join(url.split('/')[:3])
        download_urls: List[str] = []
        pages_to_visit: Set[str] = {url}
        visited_pages: Set[str] = set()
        dataset_pages: Set[str] = set()
        base_url = url.split('?')[0]

        # First find all dataset pages
        while pages_to_visit:
            current_url = pages_to_visit.pop()
            if current_url in visited_pages:
                continue
                
            visited_pages.add(current_url)
            soup = await self._fetch_and_parse(current_url)

            # Find dataset links
            for link in soup.find_all('a', href=True):
                href = link['href']
                if href.startswith('/dati/lv/dataset/'):
                    full_url = self._get_full_url(href, root_domain)
                    if full_url not in dataset_pages:
                        dataset_pages.add(full_url)

            # Find pagination links
            for link in soup.find_all('a', href=True):
                href = link['href']
                if 'page=' in href:
                    full_page_url = self._get_full_url(href, root_domain)
                    if (full_page_url.startswith(base_url) and 
                        full_page_url not in visited_pages and 
                        full_page_url not in pages_to_visit):
                        pages_to_visit.add(full_page_url)

        # Process dataset pages to find download URLs
        for dataset_url in dataset_pages:
            soup = await self._fetch_and_parse(dataset_url)
            for link in soup.find_all('a', href=True):
                href = link['href']
                if href.endswith(('.csv', '.json', '.zip')):
                    full_url = self._get_full_url(href, root_domain)
                    if full_url not in download_urls:
                        download_urls.append(full_url)

        return download_urls 