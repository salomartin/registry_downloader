# Business Registry Download

This is a tool to download business registry data from Estonian, Finnish, Latvian, Lithuanian and Czech business registers. These files are usually updated daily by the registers and contain information about companies, their officers, and other relevant information.

Happy to take PRs for other countries!

It's easy to load these files with [dlt](https://dlthub.com/), [duckdb](https://duckdb.org/) or transform them with [dbt](https://www.getdbt.com/) and integrate this into your data pipelines.

The downloads are all done in parrallel and async so it's pretty fast.

## To use from command line

Make sure you have uv installed
```sh
curl -LsSf https://astral.sh/uv/install.sh | sh
```

_Run with default settings:_
```sh
uvx registry_downloader
```

_Or override options for download directory, countries, and override URL:_
```sh
uvx registry_downloader --download-dir "./downloads" --countries ee --override-url ee=https://avaandmed.ariregister.rik.ee/et/avaandmete-allalaadimine
```

## To use as a library

You can install the `registry_downloader` package using either `pip` or `uv`. Here are examples for both:

**Using pip:**
```sh
pip install registry_downloader
```

**Using uv:**
```sh
uv add registry_downloader
```

_Run with default settings:_
```python
import asyncio
from registry_downloader import run_downloader

async def main() -> None:
    await run_downloader()

if __name__ == "__main__":
    asyncio.run(main()) 
```

_Or override options for download directory, countries, and override URL:_
```python
import asyncio
from registry_downloader import run_downloader

async def main() -> None:
    await run_downloader(
        download_dir="./downloads",
        countries=["ee"],
        override_url=["ee=https://avaandmed.ariregister.rik.ee/et/avaandmete-allalaadimine"]
    )

if __name__ == "__main__":
    asyncio.run(main()) 
```

## To develop

1. Install uv
```sh
curl -LsSf https://astral.sh/uv/install.sh | sh
```

2. Create a virtual environment and activate it
```sh
uv venv && source .venv/bin/activate
```

3. Install dependencies and ensure the virtual environment is in sync
```sh
uv sync
```

4. Build the project or run it locally with defaults
```sh
uv build
```
_or_
```sh
uv run src/registry_downloader
```