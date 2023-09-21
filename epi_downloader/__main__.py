"""The entry point for EPI downloader."""
import asyncio
import json
import sys
from argparse import ArgumentParser
from typing import Any

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


async def load_metadata(client: AsyncClient) -> Metadata:
    resp = await client.get("/api/metadata")
    resp.raise_for_status()
    return _parse_metadata(json.loads(resp.content)["data"])


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

    async with AsyncClient(base_url=config.EPI_BASE_URL) as client:
        metadata = await load_metadata(client)

    if args.dump_config:
        write_json("metadata.json", metadata)
        write_json("example_config.json", EXAMPLE_CONFIG)

    return 0


if __name__ == "__main__":
    sys.exit(asyncio.run(main()))
