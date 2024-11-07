from typing import Optional, Dict
import httpx
import asyncio
from urllib.parse import urlparse
import logging

# Global httpx client configuration
HTTP_TIMEOUT = httpx.Timeout(timeout=5 * 60)
HTTP_LIMITS = httpx.Limits(max_keepalive_connections=20, max_connections=100)
MAX_RETRIES = 3
RETRY_BACKOFF_FACTOR = 1  # seconds
MAX_CONCURRENT_PER_DOMAIN = 5

class HTTPClient:
    _instance: Optional[httpx.AsyncClient] = None
    _domain_semaphores: Dict[str, asyncio.Semaphore] = {}

    @classmethod
    async def get_client(cls) -> httpx.AsyncClient:
        if cls._instance is None:
            cls._instance = httpx.AsyncClient(
                timeout=HTTP_TIMEOUT,
                limits=HTTP_LIMITS,
                follow_redirects=True
            )
        return cls._instance

    @classmethod
    async def close(cls) -> None:
        if cls._instance is not None:
            await cls._instance.aclose()
            cls._instance = None

    @classmethod
    def get_domain_semaphore(cls, url: str) -> asyncio.Semaphore:
        domain = urlparse(url).netloc
        if domain not in cls._domain_semaphores:
            cls._domain_semaphores[domain] = asyncio.Semaphore(MAX_CONCURRENT_PER_DOMAIN)
        return cls._domain_semaphores[domain]

    @classmethod
    async def fetch(cls, url: str) -> httpx.Response:
        client = await cls.get_client()
        semaphore = cls.get_domain_semaphore(url)
        
        async with semaphore:
            retries = 0
            while retries < MAX_RETRIES:
                try:
                    response = await client.get(url)
                    response.raise_for_status()
                    return response
                except (httpx.RequestError, httpx.HTTPStatusError) as exc:
                    retries += 1
                    wait_time = RETRY_BACKOFF_FACTOR * (2 ** (retries - 1))
                    logging.warning(f"Error fetching {url}: {exc}. Retrying in {wait_time} seconds...")
                    await asyncio.sleep(wait_time)
            
            raise Exception(f"Failed to fetch {url} after {MAX_RETRIES} retries.") 