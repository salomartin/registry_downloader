import pytest
from registry_downloader.downloaders.ee import EstonianDownloader
from registry_downloader.downloaders.fi import FinnishDownloader
from registry_downloader.downloaders.lv import LatvianDownloader

@pytest.mark.asyncio
async def test_estonian_downloader():
    downloader = EstonianDownloader()
    urls = await downloader.get_urls(
        url="https://avaandmed.ariregister.rik.ee/et/avaandmete-allalaadimine"
    )
    assert urls, "EstonianDownloader did not return any URLs"

@pytest.mark.asyncio
async def test_finnish_downloader():
    downloader = FinnishDownloader()
    urls = await downloader.get_urls(
        url="https://www.avoindata.fi/data/fi/dataset/yritykset"
    )
    assert len(urls) == 1, "FinnishDownloader should return only the last URL"