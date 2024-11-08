from typing import Optional, Set, Dict
from threading import RLock
from tqdm import tqdm

class ProgressBar:
    def __init__(self, desc: str, color: str, position: int, disable: bool = False) -> None:
        self.lock = RLock()
        self.bar = tqdm(
            total=0,
            unit='B',
            unit_scale=True,
            unit_divisor=1024,
            desc=desc,
            leave=True,
            disable=disable,
            dynamic_ncols=True,
            colour=color,
            position=position,
            mininterval=0.1,
            maxinterval=0.5
        )
        self._progress_batch: int = 0
        self._batch_size: int = 1024 * 1024  # Update progress every 1MB
        self.disabled = disable
        self.country_downloads: Dict[str, int] = {}  # Track downloads per country
        self.active_downloads: int = 0

    def update_total(self, size: int) -> None:
        """Thread-safe update of total"""
        if not self.disabled:
            with self.lock:
                self.bar.total += size
                self.bar.refresh()

    def update_progress(self, size: int) -> None:
        """Batched thread-safe update of progress"""
        if not self.disabled:
            self._progress_batch += size
            if self._progress_batch >= self._batch_size:
                with self.lock:
                    self.bar.update(self._progress_batch)
                    self._progress_batch = 0

    def flush_progress(self) -> None:
        """Flush any remaining progress"""
        if not self.disabled and self._progress_batch > 0:
            with self.lock:
                self.bar.update(self._progress_batch)
                self._progress_batch = 0

    def add_country(self, country: str) -> None:
        """Add a country to active downloads"""
        with self.lock:
            if country not in self.country_downloads:
                self.country_downloads[country] = 0
            self._update_postfix()

    def remove_country(self, country: str) -> None:
        """Remove a country from active downloads"""
        with self.lock:
            if country in self.country_downloads and self.country_downloads[country] == 0:
                del self.country_downloads[country]
            self._update_postfix()

    def start_download(self, country: str) -> None:
        """Start a new download for a country"""
        with self.lock:
            if country in self.country_downloads:
                self.country_downloads[country] += 1
                self.active_downloads += 1
            self._update_postfix()

    def finish_download(self, country: str) -> None:
        """Finish a download for a country"""
        with self.lock:
            if country in self.country_downloads:
                self.country_downloads[country] = max(0, self.country_downloads[country] - 1)
                self.active_downloads = max(0, self.active_downloads - 1)
                if self.country_downloads[country] == 0:
                    del self.country_downloads[country]
            self._update_postfix()

    def _update_postfix(self) -> None:
        """Update the postfix with current counts"""
        if not self.disabled:
            active_countries = len(self.country_downloads)
            self.bar.set_postfix(
                countries=active_countries,
                files=self.active_downloads,
                refresh=True
            )

    def set_postfix(self, **kwargs: str) -> None:
        """Thread-safe update of postfix"""
        if not self.disabled:
            with self.lock:
                self._update_postfix()

    def close(self) -> None:
        """Close the progress bar"""
        if not self.disabled:
            self.flush_progress()
            with self.lock:
                self.bar.close()

class ProgressManager:
    def __init__(self, disable: bool = False) -> None:
        # Initialize global lock for tqdm
        self.global_lock = RLock()
        tqdm.set_lock(self.global_lock)
        
        self.disabled = disable
        # Set up fixed positions for progress bars
        self.download_bar = ProgressBar("Download Progress", "blue", position=0, disable=disable)
        self.extract_bar: Optional[ProgressBar] = None
        self.extract_lock = RLock()

    def close(self) -> None:
        """Close progress bars in reverse order"""
        with self.global_lock:
            if self.extract_bar is not None:
                self.extract_bar.close()
                self.extract_bar = None
            self.download_bar.close()

    def create_extract_bar(self) -> None:
        """Thread-safe creation of extract bar at fixed position"""
        with self.extract_lock:
            if self.extract_bar is None:
                with self.global_lock:
                    self.extract_bar = ProgressBar("Extract Progress", "green", position=1, disable=self.disabled)

    def update_download_total(self, size: int) -> None:
        with self.global_lock:
            self.download_bar.update_total(size)

    def update_download_progress(self, size: int) -> None:
        with self.global_lock:
            self.download_bar.update_progress(size)

    def update_extract_total(self, size: int) -> None:
        if self.extract_bar is not None:
            with self.global_lock:
                self.extract_bar.update_total(size)

    def update_extract_progress(self, size: int) -> None:
        if self.extract_bar is not None:
            with self.global_lock:
                self.extract_bar.update_progress(size)

    def add_downloading_country(self, country: str) -> None:
        """Add a country to active downloads"""
        with self.global_lock:
            self.download_bar.add_country(country)

    def remove_downloading_country(self, country: str) -> None:
        """Remove a country from active downloads"""
        with self.global_lock:
            self.download_bar.remove_country(country)

    def start_download(self, country: str) -> None:
        """Start a new download"""
        with self.global_lock:
            self.download_bar.start_download(country)

    def finish_download(self, country: str) -> None:
        """Finish a download"""
        with self.global_lock:
            self.download_bar.finish_download(country)

    def add_extracting_country(self, country: str) -> None:
        """Add a country to active extractions"""
        if self.extract_bar is not None:
            with self.global_lock:
                self.extract_bar.add_country(country)
                self.extract_bar.start_download(country)

    def remove_extracting_country(self, country: str) -> None:
        """Remove a country from active extractions"""
        if self.extract_bar is not None:
            with self.global_lock:
                self.extract_bar.finish_download(country)
                self.extract_bar.remove_country(country)

    def set_extract_postfix(self, **kwargs: str) -> None:
        """Set postfix for extract progress bar"""
        if self.extract_bar is not None:
            with self.global_lock:
                self.extract_bar.set_postfix(**kwargs)