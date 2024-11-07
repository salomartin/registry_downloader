import os
import asyncio
import logging
import zipfile
from typing import Dict, Any, Optional
from .core.client import HTTPClient
from .core.progress import ProgressManager
from .downloaders import (
    BaseDownloader,
    EstonianDownloader,
    FinnishDownloader,
    LatvianDownloader,
)

DEFAULT_COUNTRY_CONFIGS: Dict[str, Dict[str, Any]] = {
    'cz': {
        'url': 'https://csu.gov.cz/produkty/registr-ekonomickych-subjektu-otevrena-data',
        'downloader': BaseDownloader()
    },
    'ee': {
        'url': 'https://avaandmed.ariregister.rik.ee/et/avaandmete-allalaadimine',
        'downloader': EstonianDownloader()
    },
    'lv': {
        'url': 'https://data.gov.lv/dati/lv/organization/ur',
        'downloader': LatvianDownloader()
    },
    'fi': {
        'url': 'https://www.avoindata.fi/data/fi/dataset/yritykset',
        'downloader': FinnishDownloader()
    },
    'lt': {
        'url': 'https://www.registrucentras.lt/p/1094',
        'downloader': BaseDownloader()
    }
}

async def download_and_process_file(
    url: str,
    download_folder: str,
    common_prefix: str,
    progress: ProgressManager,
    country: str
) -> None:
    """Download and process a single file."""
    client = await HTTPClient.get_client()
    content_length: Optional[int] = None
    filename: Optional[str] = None

    # Try HEAD request first
    try:
        head_response = await client.head(url)
        head_response.raise_for_status()
        filename = os.path.basename(url)
        
        if 'content-length' in head_response.headers:
            content_length = int(head_response.headers['content-length'])
            progress.download_bar.total += content_length
            progress.download_bar.refresh()

    except Exception as e:
        logging.warning(f"HEAD request failed for {url}: {e}")

    # Download file
    try:
        async with client.stream('GET', url) as response:
            response.raise_for_status()
            
            if not filename:
                filename = os.path.basename(response.url.path) or 'download.bin'
            
            progress.download_bar.set_postfix(file=filename, country=country) # type: ignore

            if filename.lower().endswith('.zip'):
                # Handle ZIP files
                import tempfile
                with tempfile.NamedTemporaryFile(delete=False) as temp_file:
                    temp_filename = temp_file.name
                    async for chunk in response.aiter_bytes():
                        temp_file.write(chunk)
                        progress.download_bar.update(len(chunk))

                # Extract ZIP contents
                extract_bar = progress.create_extract_bar() # type: ignore
                with zipfile.ZipFile(temp_filename, 'r') as zip_file:
                    total_size = sum(info.file_size for info in zip_file.filelist)
                    extract_bar.total += total_size
                    extract_bar.refresh()

                    for zip_info in zip_file.filelist:
                        name = zip_info.filename
                        unique_name = name[len(common_prefix):] if name.lower().startswith(common_prefix.lower()) else name
                        extracted_path = os.path.join(download_folder, unique_name)
                        os.makedirs(os.path.dirname(extracted_path), exist_ok=True)

                        extract_bar.set_postfix(file=unique_name, country=country) # type: ignore
                        with zip_file.open(name) as source, open(extracted_path, 'wb') as target:
                            while chunk := source.read(8192):  # 8KB chunks
                                target.write(chunk)
                                extract_bar.update(len(chunk))

                os.unlink(temp_filename)

            else:
                # Handle non-ZIP files
                unique_filename = filename[len(common_prefix):] if filename.lower().startswith(common_prefix.lower()) else filename
                file_path = os.path.join(download_folder, unique_filename)
                os.makedirs(os.path.dirname(file_path), exist_ok=True)

                with open(file_path, 'wb') as f:
                    async for chunk in response.aiter_bytes():
                        f.write(chunk)
                        progress.download_bar.update(len(chunk))

    except Exception as e:
        logging.error(f"Error processing {url}: {e}")
        if content_length:
            progress.download_bar.total -= content_length
            progress.download_bar.refresh()

async def process_country(
    country: str,
    config: Dict[str, Any],
    progress: ProgressManager,
    download_dir: str
) -> None:
    """Process downloads for a specific country."""
    download_folder = os.path.join(download_dir, country)
    if not os.path.exists(download_folder):
        os.makedirs(download_folder)

    downloader: BaseDownloader = config['downloader']
    urls = await downloader.get_urls(config['url'])
    logging.info(f"Detected URLs for {country}: {urls}")

    # Find common prefix for multiple files
    common_prefix = ""
    if len(urls) > 1:
        filenames = [os.path.basename(url).lower() for url in urls]
        common_prefix = os.path.commonprefix(filenames)

    # Process files concurrently
    tasks = [
        download_and_process_file(url, download_folder, common_prefix, progress, country)
        for url in urls
    ]
    await asyncio.gather(*tasks)

async def run_downloader(
    download_dir: str = "./downloads",
    countries: Optional[list[str]] = None,
    override_url: Optional[list[str]] = None
) -> None:
    """Run the registry downloader with specified options."""
    country_configs = DEFAULT_COUNTRY_CONFIGS.copy()

    if countries:
        country_configs = {k: v for k, v in country_configs.items() if k in countries}

    if override_url:
        for override in override_url:
            country, url = override.split('=')
            if country in country_configs:
                country_configs[country]['url'] = url

    progress = ProgressManager()
    
    try:
        tasks = [
            process_country(country, config, progress, download_dir)
            for country, config in country_configs.items()
        ]
        await asyncio.gather(*tasks)
    finally:
        progress.close()
        await HTTPClient.close() 