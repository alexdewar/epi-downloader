"""The entry point for EPI downloader."""
import asyncio
import json
import os
import sys
from argparse import ArgumentParser
from pathlib import Path
from typing import Any

import platformdirs
from httpx import AsyncClient

from . import config

Metadata = dict[str, dict[str, int]]

REQUIRED_VARS = ("model", "measure", "year", "age", "sex")
EXAMPLE_CONFIG: dict[str, str] = {
    "model": "Diabetes mellitus",
    "measure": "Prevalence",
    "year": "2015",
    "age": "20-24 years",
    "sex": "Male",
}


class CacheClient:
    def __init__(self, client: AsyncClient, cache_path: Path) -> None:
        self._client = client
        self._cache_path = cache_path

    async def get(self, url: str, file_name: str, *args: Any, **kwargs: Any) -> str:
        file_path = self._cache_path / file_name

        # If it's already cached, then load it
        if file_path.exists():
            with file_path.open() as file:
                return file.read()

        response = await self._client.get(url, *args, **kwargs)
        response.raise_for_status()

        # Don't bother caching empty files
        if response.text:
            os.makedirs(self._cache_path, exist_ok=True)
            with file_path.open("w") as file:
                file.write(response.text)

        return response.text


def _parse_metadata(metadata: dict[str, Any]) -> Metadata:
    out = {}
    for var in REQUIRED_VARS:
        out[var] = {
            str(item["name"]): item[f"{var}_id"] for item in metadata[var].values()
        }

    return out


def write_json(file_name: str, data: Any) -> None:
    print(f"Saving {file_name}")
    with open(file_name, "w") as file:
        json.dump(data, file, indent=4)


async def load_metadata(client: CacheClient) -> Metadata:
    text = await client.get("/api/metadata", "metadata.json")
    return _parse_metadata(json.loads(text)["data"])


async def main() -> int:
    parser = ArgumentParser(
        prog="epi_downloader", description="A tool to download datasets from IHME"
    )
    parser.add_argument(
        "--dump-config",
        dest="dump_config",
        action="store_true",
        help="Dump metadata and example config files",
    )

    if len(sys.argv) <= 1:
        parser.print_usage()
        return 1

    args = parser.parse_args()

    async with AsyncClient(base_url=config.EPI_BASE_URL) as http_client:
        cache_path = platformdirs.user_cache_path(__name__, ensure_exists=True)
        client = CacheClient(http_client, cache_path)
        metadata = await load_metadata(client)

    if args.dump_config:
        write_json("metadata.json", metadata)
        write_json("example_config.json", EXAMPLE_CONFIG)

    return 0


if __name__ == "__main__":
    sys.exit(asyncio.run(main()))
