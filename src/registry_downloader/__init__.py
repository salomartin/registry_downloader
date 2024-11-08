import argparse
import asyncio
import sys
from typing import Optional, Sequence
from registry_downloader.runner import run_downloader

__all__ = ['run_downloader', 'main']

def parse_arguments() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Registry Downloader")
    parser.add_argument(
        "--download-dir",
        type=str,
        default="./downloads",
        help="Directory to save downloaded files"
    )
    parser.add_argument(
        "--countries",
        type=str,
        nargs='+',
        choices=['cz', 'ee', 'lv', 'fi', 'lt'],
        help="List of countries to download data for"
    )
    parser.add_argument(
        "--override-url",
        type=str,
        nargs='+',
        help="Override URLs for specific countries in the format country=url"
    )
    parser.add_argument(
        "--max-workers",
        type=int,
        help="Maximum number of worker threads (defaults to CPU count)"
    )
    parser.add_argument(
        "--no-progress",
        action="store_true",
        help="Disable progress bars"
    )
    return parser.parse_args()

async def amain(
    download_dir: str = "./downloads",
    countries: Optional[Sequence[str]] = None,
    override_url: Optional[Sequence[str]] = None,
    max_workers: Optional[int] = None,
    show_progress: bool = True
) -> None:
    """Async entry point for the registry downloader."""
    await run_downloader(
        download_dir=download_dir,
        countries=countries,
        override_url=override_url,
        max_workers=max_workers,
        show_progress=show_progress
    )

def main() -> None:
    """Command-line interface entry point."""
    try:
        args = parse_arguments()
        asyncio.run(amain(
            download_dir=args.download_dir,
            countries=args.countries,
            override_url=args.override_url,
            max_workers=args.max_workers,
            show_progress=not args.no_progress
        ))
    except KeyboardInterrupt:
        sys.exit(1)
    except Exception as e:
        print(f"Error: {e}", file=sys.stderr)
        sys.exit(1) 