"""The entry point for EPI downloader."""
import asyncio
import json
import sys
from argparse import ArgumentParser
from typing import Any

from httpx_cache import AsyncClient, FileCache

from . import config

Metadata = dict[str, dict[str, int]]


def _parse_metadata(metadata: dict[str, Any]) -> Metadata:
    out = {}
    for var in ("model", "measure", "year", "age", "sex"):
        out[var] = {
            str(item["name"]): item[f"{var}_id"] for item in metadata[var].values()
        }

    return out


def dump_to_file(metadata: Metadata) -> None:
    print("Saving metadata.json")
    with open("metadata.json", "w") as file:
        json.dump(metadata, file, indent=4)


async def load_metadata(client: AsyncClient) -> Metadata:
    resp = await client.get("/api/metadata")
    resp.raise_for_status()
    return _parse_metadata(json.loads(resp.content)["data"])


async def main() -> int:
    parser = ArgumentParser(
        prog="epi_downloader", description="A tool to download datasets from IHME"
    )
    parser.add_argument(
        "--dump-metadata",
        dest="dump_metadata",
        action="store_true",
        help="Dump metadata to a series of JSON files",
    )

    if len(sys.argv) <= 1:
        parser.print_usage()
        return 1

    args = parser.parse_args()

    async with AsyncClient(base_url=config.EPI_BASE_URL, cache=FileCache()) as client:
        metadata = await load_metadata(client)

    if args.dump_metadata:
        dump_to_file(metadata)
        return 0

    return 0


if __name__ == "__main__":
    sys.exit(asyncio.run(main()))
