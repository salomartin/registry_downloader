from typing import Optional
import multiprocessing
from concurrent.futures import ThreadPoolExecutor

class ThreadPoolManager:
    _instance: Optional[ThreadPoolExecutor] = None
    
    @classmethod
    def get_pool(cls, max_workers: Optional[int] = None) -> ThreadPoolExecutor:
        if cls._instance is None:
            if max_workers is None:
                max_workers = multiprocessing.cpu_count()
            cls._instance = ThreadPoolExecutor(max_workers=max_workers)
        return cls._instance
    
    @classmethod
    def shutdown(cls) -> None:
        if cls._instance is not None:
            cls._instance.shutdown(wait=True)
            cls._instance = None 