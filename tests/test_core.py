from registry_downloader.runner import fix_encoding
from registry_downloader.core.threadpool import ThreadPoolManager

def test_fix_encoding_utf8():
    data = "Hello, world!".encode("utf-8")
    fixed_data, encoding = fix_encoding(data)
    assert fixed_data == data
    assert encoding == "utf-8"

def test_fix_encoding_latin1():
    data = "Olá, mundo!".encode("latin1")
    fixed_data, encoding = fix_encoding(data)
    assert fixed_data == "Olá, mundo!".encode("utf-8")
    assert encoding == "ISO-8859-1"

def test_thread_pool_manager():
    pool = ThreadPoolManager.get_pool(max_workers=5)
    assert pool._max_workers == 5
    ThreadPoolManager.shutdown()
    assert ThreadPoolManager._instance is None 