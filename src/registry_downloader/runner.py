import os
import asyncio
import logging
import zipfile
from typing import Dict, Any, Optional, List, Tuple
from asyncio import Queue
from .core.client import HTTPClient
from .core.progress import ProgressManager
from .core.threadpool import ThreadPoolManager
from .downloaders import (
    BaseDownloader,
    EstonianDownloader,
    FinnishDownloader,
    LatvianDownloader,
)
import chardet
import httpx
import codecs

DEFAULT_COUNTRY_CONFIGS: Dict[str, Dict[str, Any]] = {
    'cz': {
        'url': 'https://csu.gov.cz/produkty/registr-ekonomickych-subjektu-otevrena-data',
        'downloader': BaseDownloader()
    },
    'ee': {
        'url': 'https://avaandmed.ariregister.rik.ee/et/avaandmete-allalaadimine',
        'downloader': EstonianDownloader()
    },
    'ee_emta': {
        'url': 'https://www.emta.ee/ariklient/amet-uudised-ja-kontakt/uudised-pressiinfo-statistika/statistika-ja-avaandmed',
        'downloader': BaseDownloader()
    },
    'lv': {
        'url': 'https://data.gov.lv/dati/lv/organization/ur',
        'downloader': LatvianDownloader()
    },
    'fi': {
        'url': 'https://www.avoindata.fi/data/fi/dataset/yritykset',
        'downloader': FinnishDownloader(skip_head_request=True)
    },
    'lt': {
        'url': 'https://www.registrucentras.lt/p/1094',
        'downloader': BaseDownloader(skip_head_request=True)
    }
}

def is_binary_string(data: bytes) -> bool:
    """Check if the byte string contains binary data."""
    # If there are null bytes, likely binary
    if b'\x00' in data:
        return True
    # Check for high ASCII characters
    text_characters = bytearray({7, 8, 9, 10, 12, 13, 27} | set(range(0x20, 0x100)))
    _null_trans = bytearray(256)
    trans_table = bytes.maketrans(_null_trans, _null_trans)
    nontext = data.translate(trans_table, text_characters)
    return bool(nontext)

def fix_encoding(data: bytes, sample_size: int = 256000) -> Tuple[bytes, str]:
    """Detect and fix encoding of byte data, converting to UTF-8 if needed."""
    # Check if data is binary
    if is_binary_string(data[:sample_size]):
        return data, 'binary'

    # Try to decode as UTF-8
    try:
        data.decode('utf-8')
        return data, 'utf-8'
    except UnicodeDecodeError:
        pass

    # Detect encoding using chardet
    sample = data[:sample_size]
    result = chardet.detect(sample)
    encoding = result['encoding'] if result['encoding'] else 'utf-8'

    # Try to decode with detected encoding
    try:
        decoded = data.decode(encoding)
        return decoded.encode('utf-8'), encoding
    except (UnicodeDecodeError, LookupError):
        # If decoding fails, return data as-is
        return data, 'unknown'

async def process_chunks(
    queue: Queue[bytes],
    file_path: str,
    progress: ProgressManager
) -> None:
    """Process chunks from queue and write to file."""
    def _write_chunks() -> None:
        buffer = bytearray()
        total_size = 0

        with open(file_path, 'wb') as f:
            loop = asyncio.new_event_loop()
            asyncio.set_event_loop(loop)
            try:
                while True:
                    try:
                        chunk = loop.run_until_complete(asyncio.wait_for(queue.get(), timeout=1.0))
                        if chunk == b'':  # EOF marker
                            # Process buffer
                            data = bytes(buffer)
                            fixed_data, detected_encoding = fix_encoding(data)

                            if detected_encoding == 'binary':
                                logging.debug(f"{file_path} is binary, writing without modification")
                                f.write(data)
                            elif detected_encoding == 'utf-8':
                                logging.debug(f"{file_path} is valid UTF-8, writing without modification")
                                f.write(data)
                            elif detected_encoding != 'unknown':
                                logging.info(f"Converted {file_path} from {detected_encoding} to UTF-8")
                                f.write(fixed_data)
                            else:
                                logging.warning(f"Unknown encoding for {file_path}, writing without modification")
                                f.write(data)

                            queue.task_done()
                            break
                        buffer.extend(chunk)
                        total_size += len(chunk)
                        progress.update_download_progress(len(chunk))
                        queue.task_done()
                    except asyncio.TimeoutError:
                        continue
                    except Exception as e:
                        logging.error(f"Error writing chunk to {file_path}: {e}", exc_info=True)
                        break
            finally:
                loop.close()

    await asyncio.get_event_loop().run_in_executor(
        ThreadPoolManager.get_pool(),
        _write_chunks
    )

def _process_zip_entry(
    zip_file: zipfile.ZipFile,
    zip_info: zipfile.ZipInfo,
    target_path: str,
    progress: ProgressManager
) -> None:
    """Process a single entry from the ZIP file."""
    try:
        with zip_file.open(zip_info) as source, open(target_path, 'wb') as target:
            data = source.read()
            progress.update_extract_progress(len(data))

            # Process encoding
            fixed_data, detected_encoding = fix_encoding(data)

            if detected_encoding == 'binary':
                logging.debug(f"{zip_info.filename} is binary, writing without modification")
                target.write(data)
            elif detected_encoding == 'utf-8':
                logging.debug(f"{zip_info.filename} is valid UTF-8, writing without modification")
                target.write(data)
            elif detected_encoding != 'unknown':
                logging.info(f"Converted {zip_info.filename} from {detected_encoding} to UTF-8")
                target.write(fixed_data)
            else:
                logging.warning(f"Unknown encoding for {zip_info.filename}, writing without modification")
                target.write(data)

    except Exception as e:
        logging.error(f"Error extracting {zip_info.filename}: {e}", exc_info=True)

async def extract_zip_parallel(
    temp_filename: str,
    download_folder: str,
    common_prefix: str,
    progress: ProgressManager,
    country: str,
    chunk_size: int = 8192
) -> None:
    """Extract ZIP file with parallel processing of entries."""
    try:
        def _extract() -> None:
            with zipfile.ZipFile(temp_filename, 'r') as zip_file:
                # Update total size once
                total_size = sum(info.file_size for info in zip_file.filelist)
                progress.update_extract_total(total_size)

                # Process each file in parallel using ThreadPoolExecutor
                pool = ThreadPoolManager.get_pool()
                futures = []

                for zip_info in zip_file.filelist:
                    name = zip_info.filename
                    unique_name = name[len(common_prefix):] if name.lower().startswith(common_prefix.lower()) else name
                    extracted_path = os.path.join(download_folder, unique_name)
                    os.makedirs(os.path.dirname(extracted_path), exist_ok=True)

                    # Update extract progress bar with current file
                    progress.add_extracting_country(country)
                    futures.append(
                        pool.submit(
                            _process_zip_entry,
                            zip_file,
                            zip_info,
                            extracted_path,
                            progress  # Pass progress manager
                        )
                    )

                # Wait for all extractions to complete
                for future in futures:
                    try:
                        future.result()
                    except Exception as e:
                        logging.error(f"Error in extraction task: {e}", exc_info=True)

        await asyncio.get_event_loop().run_in_executor(
            ThreadPoolManager.get_pool(),
            _extract
        )
    finally:
        # Clean up temp file
        try:
            os.unlink(temp_filename)
        except Exception as e:
            logging.error(f"Error removing temporary file {temp_filename}: {e}", exc_info=True)

async def download_and_process_file(
    url: str,
    download_folder: str,
    common_prefix: str,
    progress: ProgressManager,
    country: str,
    skip_head_request: bool = False,
    chunk_size: int = 64 * 1024,
    domain_semaphore: Optional[asyncio.Semaphore] = None,
    global_semaphore: Optional[asyncio.Semaphore] = None,
    max_retries: int = 3,
    retry_delay: float = 1.0
) -> None:
    """Download and process a single file."""
    if domain_semaphore is None or global_semaphore is None:
        raise ValueError("Both domain_semaphore and global_semaphore must be provided")

    try:
        # Add country and start download tracking
        progress.add_downloading_country(country)
        progress.start_download(country)
        
        # Use both domain-specific and global semaphores
        async with domain_semaphore:  # First acquire domain semaphore
            async with global_semaphore:  # Then acquire global semaphore
                client = await HTTPClient.get_client()
                content_length: Optional[int] = None
                filename: Optional[str] = None

                # Try HEAD request first if not skipped
                if not skip_head_request:
                    for retry in range(max_retries):
                        try:
                            head_response = await client.head(url)
                            head_response.raise_for_status()
                            filename = os.path.basename(url)
                            
                            if 'content-length' in head_response.headers:
                                content_length = int(head_response.headers['content-length'])
                                progress.update_download_total(content_length)
                            break
                        except Exception as e:
                            if retry == max_retries - 1:
                                logging.warning(f"HEAD request failed for {url}: {e}")
                            else:
                                await asyncio.sleep(retry_delay * (retry + 1))

                # Download file with retries
                for retry in range(max_retries):
                    try:
                        async with client.stream('GET', url) as response:
                            if response.status_code == 404:
                                logging.warning(f"File not found (404) for {url}")
                                if content_length:
                                    progress.update_download_total(-content_length)
                                return  # Exit immediately on 404, no retry
                            
                            response.raise_for_status()
                            
                            if content_length is None and 'content-length' in response.headers:
                                content_length = int(response.headers['content-length'])
                                progress.update_download_total(content_length)
                            
                            if not filename:
                                filename = os.path.basename(response.url.path) or 'download.bin'

                            # Create chunk processing queue
                            chunk_queue: Queue[bytes] = Queue()
                            
                            if filename.lower().endswith('.zip'):
                                # Handle ZIP files
                                import tempfile
                                temp_file = tempfile.NamedTemporaryFile(delete=False)
                                temp_filename = temp_file.name
                                temp_file.close()

                                # Start chunk processor
                                processor = asyncio.create_task(
                                    process_chunks(chunk_queue, temp_filename, progress)
                                )

                                # Feed chunks to queue
                                try:
                                    async for chunk in response.aiter_bytes(chunk_size):
                                        await chunk_queue.put(chunk)
                                finally:
                                    await chunk_queue.put(b'')  # EOF marker
                                    await processor
                                    await chunk_queue.join()  # Wait for all chunks to be processed

                                # Extract ZIP contents
                                progress.create_extract_bar()
                                progress.add_extracting_country(country)
                                try:
                                    await extract_zip_parallel(
                                        temp_filename,
                                        download_folder,
                                        common_prefix,
                                        progress,
                                        country
                                    )
                                finally:
                                    progress.remove_extracting_country(country)
                            else:
                                # Handle non-ZIP files
                                unique_filename = filename[len(common_prefix):] if filename.lower().startswith(common_prefix.lower()) else filename
                                file_path = os.path.join(download_folder, unique_filename)
                                os.makedirs(os.path.dirname(file_path), exist_ok=True)

                                # Start chunk processor
                                processor = asyncio.create_task(
                                    process_chunks(chunk_queue, file_path, progress)
                                )

                                # Feed chunks to queue
                                try:
                                    async for chunk in response.aiter_bytes(chunk_size):
                                        await chunk_queue.put(chunk)
                                finally:
                                    await chunk_queue.put(b'')  # EOF marker
                                    await processor
                                    await chunk_queue.join()  # Wait for all chunks to be processed
                            
                            # If we get here, download was successful
                            break

                    except Exception as e:
                        if isinstance(e, httpx.HTTPStatusError) and e.response.status_code == 404:
                            logging.warning(f"File not found (404) for {url}")
                            if content_length:
                                progress.update_download_total(-content_length)
                            return  # Exit immediately on 404, no retry
                        
                        if retry == max_retries - 1:
                            logging.error(f"Error processing {url}: {e}", exc_info=True)
                            if content_length:
                                progress.update_download_total(-content_length)
                            raise  # Re-raise on last retry
                        else:
                            logging.warning(f"Retry {retry + 1}/{max_retries} for {url}: {e}")
                            await asyncio.sleep(retry_delay * (retry + 1))

    finally:
        # Remove country and finish download tracking
        progress.finish_download(country)
        progress.remove_downloading_country(country)

async def process_country(
    country: str,
    config: Dict[str, Any],
    progress: ProgressManager,
    download_dir: str,
    global_semaphore: asyncio.Semaphore
) -> None:
    """Process downloads for a specific country."""
    download_folder = os.path.join(download_dir, country)
    if not os.path.exists(download_folder):
        os.makedirs(download_folder)

    try:
        downloader: BaseDownloader = config['downloader']
        # Add country before getting URLs to show it's active
        progress.add_downloading_country(country)
        
        urls = await downloader.get_urls(config['url'])
        logging.info(f"Detected URLs for {country}: {urls}")

        if not urls:
            logging.error(f"No URLs found for country {country}")
            return

        # Find common prefix for multiple files
        common_prefix = ""
        if len(urls) > 1:
            filenames = [os.path.basename(url).lower() for url in urls]
            common_prefix = os.path.commonprefix(filenames)

        # Create tasks for all URLs immediately
        download_tasks = []
        for url in urls:
            domain_semaphore = HTTPClient.get_domain_semaphore(url)
            task = asyncio.create_task(
                download_and_process_file(
                    url,
                    download_folder,
                    common_prefix,
                    progress,
                    country,
                    skip_head_request=downloader.skip_head_request,
                    domain_semaphore=domain_semaphore,
                    global_semaphore=global_semaphore
                )
            )
            download_tasks.append(task)

        # Wait for all downloads to complete
        if download_tasks:
            results = await asyncio.gather(*download_tasks, return_exceptions=True)
            failures = [r for r in results if isinstance(r, Exception)]
            
            if len(failures) == len(urls):
                error_msg = f"All downloads failed for country {country}. Last error: {failures[-1]}"
                if isinstance(failures[-1], Exception):
                    error_msg += f"\nDetails: {str(failures[-1])}"
                logging.error(error_msg)
            elif failures:
                logging.warning(f"{len(failures)}/{len(urls)} downloads failed for country {country}")

    except Exception as e:
        logging.error(f"Failed to process country {country}: {e}", exc_info=True)
        raise
    finally:
        # Only remove country when all its downloads are done
        progress.remove_downloading_country(country)

async def run_downloader(
    download_dir: str = "./downloads",
    countries: Optional[list[str]] = None,
    override_url: Optional[list[str]] = None,
    max_workers: Optional[int] = None,
    show_progress: bool = True,
    max_concurrent_downloads: int = 25  # Increased default to handle 5 countries * 5 connections each
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

    progress = ProgressManager(disable=not show_progress)
    
    # Create global semaphore for overall concurrency control
    global_semaphore = asyncio.Semaphore(max_concurrent_downloads)
    
    try:
        # Process all countries truly concurrently
        country_tasks = []
        for country, config in country_configs.items():
            task = asyncio.create_task(
                process_country(
                    country,
                    config,
                    progress,
                    download_dir,
                    global_semaphore
                )
            )
            country_tasks.append(task)
        
        # Wait for all countries to complete
        results = await asyncio.gather(*country_tasks, return_exceptions=True)
        
        # Check for complete country failures
        failures = [r for r in results if isinstance(r, Exception)]
        if failures:
            logging.error(f"{len(failures)}/{len(country_configs)} countries failed to process completely")
            for failure in failures:
                logging.error(f"Country processing error: {failure}")
    finally:
        progress.close()
        await HTTPClient.close()
        ThreadPoolManager.shutdown()