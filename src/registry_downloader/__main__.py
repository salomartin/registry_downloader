import argparse
import asyncio
import sys
from registry_downloader.runner import run_downloader

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
    return parser.parse_args()

async def main() -> None:
    """Main entry point for the registry downloader."""
    args = parse_arguments()
    await run_downloader(
        download_dir=args.download_dir,
        countries=args.countries,
        override_url=args.override_url
    )

def cli() -> None:
    """Command-line interface entry point."""
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        sys.exit(1)
    except Exception as e:
        print(f"Error: {e}", file=sys.stderr)
        sys.exit(1)

if __name__ == "__main__":
    cli()