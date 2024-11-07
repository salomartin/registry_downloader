from typing import Dict, Optional
from threading import RLock
from tqdm import tqdm

class ProgressManager:
    def __init__(self) -> None:
        # Set up thread safety
        tqdm.set_lock(RLock()) # type: ignore
        
        self.download_bar = tqdm(
            total=0,
            unit='B',
            unit_scale=True,
            unit_divisor=1024,
            desc='Total Download',
            leave=True,
            disable=False,
            dynamic_ncols=True,
            colour='blue'
        )
        
        self.extract_bars: Dict[str, Optional[tqdm]] = {'pbar': None} # type: ignore
        self.extract_lock = RLock()

    def close(self) -> None:
        self.download_bar.close()
        if self.extract_bars['pbar'] is not None: # type: ignore
            self.extract_bars['pbar'].close() # type: ignore

    def create_extract_bar(self) -> tqdm: # type: ignore
        with self.extract_lock:
            if self.extract_bars['pbar'] is None: # type: ignore
                self.extract_bars['pbar'] = tqdm( # type: ignore
                    total=0,
                    unit='B',
                    unit_scale=True,
                    unit_divisor=1024,
                    desc='Total Extract',
                    leave=True,
                    disable=False,
                    dynamic_ncols=True,
                    colour='green'
                )
            return self.extract_bars['pbar'] # type: ignore