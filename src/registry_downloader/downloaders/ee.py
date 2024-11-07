from typing import List, Dict
import re
import os
from .base import BaseDownloader

class EstonianDownloader(BaseDownloader):
    async def get_urls(self, url: str, only_last: bool = False) -> List[str]:
        root_domain = '/'.join(url.split('/')[:3])
        soup = await self._fetch_and_parse(url)
        zip_urls: List[str] = []
        base_urls: Dict[str, Dict[str, str]] = {}

        # First pass - collect all URLs grouped by base filename
        for link in soup.find_all('a', href=True):
            href = link['href']
            full_url = self._get_full_url(href, root_domain)
            
            if full_url.endswith('.zip'):
                # Check if it's a data type specific file
                if any(ext in full_url.lower() for ext in ['.xml.', '.csv.', '.json.']):
                    base_filename = re.sub(r'\.(xml|csv|json)\.zip$', '', 
                                        os.path.basename(full_url))

                    if base_filename not in base_urls:
                        base_urls[base_filename] = {}

                    # Store URL by its format
                    if 'json' in full_url.lower():
                        base_urls[base_filename]['json'] = full_url
                    elif 'csv' in full_url.lower():
                        base_urls[base_filename]['csv'] = full_url
                    elif 'xml' in full_url.lower():
                        base_urls[base_filename]['xml'] = full_url
                else:
                    zip_urls.append(full_url)

        # Second pass - select preferred format for each base filename
        for formats in base_urls.values():
            if 'json' in formats:
                zip_urls.append(formats['json'])
            elif 'csv' in formats:
                zip_urls.append(formats['csv'])
            elif 'xml' in formats:
                zip_urls.append(formats['xml'])

        return zip_urls 