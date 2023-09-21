"""The entry point for EPI downloader."""
import argparse
import asyncio
import json
import sys
from pathlib import Path
from typing import Any

import platformdirs
from httpx import AsyncClient

APP_NAME = "epi_downloader"
EPI_BASE_URL = "https://vizhub.healthdata.org/epi"
REQUIRED_VARS = ("model", "measure", "year", "age", "sex")
EXAMPLE_CONFIG: dict[str, str] = {
    "model": "Diabetes mellitus",
    "measure": "Prevalence",
    "year": "2015",
    "age": "20-24 years",
    "sex": "Male",
}

Metadata = dict[str, dict[str, int]]
Versions = list[dict[str, Any]]


class CacheClient:
    def __init__(
        self, client: AsyncClient, cache_path: Path, ignore_cache: bool
    ) -> None:
        self._client = client
        self._cache_path = cache_path
        self._ignore_cache = ignore_cache

    async def get(self, url: str, file_name: str, *args: Any, **kwargs: Any) -> str:
        file_path = self._cache_path / file_name

        # If the data has already been cached (and the user hasn't opted out of using
        # the cache) then load it from disk
        if not self._ignore_cache and file_path.exists():
            with file_path.open() as file:
                return file.read()

        # Make HTTP request
        response = await self._client.get(url, *args, **kwargs)
        response.raise_for_status()

        # Cache data on disk
        with file_path.open("w") as file:
            file.write(response.text)

        return response.text


async def load_model_versions(client: CacheClient, model: int) -> Versions:
    # NB: I'm not sure what the step param means, but it seems to be empty for the
    # datasets I've been downloading -- AD
    params = {"model": model, "step": ""}
    text = await client.get(
        "/api/model/versions", f"versions_model{model}.json", params=params
    )
    return list(json.loads(text)["data"].values())


def get_latest_model_version(versions: Versions, measure: int | None) -> int:
    """Get the latest version ID matching the specified measure.

    This assumes that newer datasets have higher version IDs.
    """
    return max(v["version"] for v in versions if v["measure"] == measure)


def get_model_version(versions: Versions, measure: int) -> int:
    """Try to get the best-matching model version for the specified measure.

    The metadata is in a slightly confusing form here. Sometimes there are versions
    available which have measure IDs explicitly specified, in which case we can just
    check whether there is a version available with the desired measure ID. However, at
    other times, there is no measure ID specified (the field is null), but seemingly
    data for the desired measure can still be downloaded. So we first try to get an
    exact match for the measure ID and fall back on using a model version with no
    measure ID.
    """
    try:
        return get_latest_model_version(versions, measure)
    except ValueError:
        return get_latest_model_version(versions, None)


def load_config(config_path: str, metadata: Metadata) -> dict[str, int]:
    with open(config_path) as file:
        config = json.load(file)

    # Convert the names to corresponding integer IDs
    return {key: metadata[str(key)][value] for key, value in config.items()}


async def download_data(
    client: CacheClient, config: dict[str, int], output_path: str
) -> None:
    versions = await load_model_versions(client, config["model"])

    # Get version ID for dataset
    version = get_model_version(versions, config["measure"])
    params: dict[str, Any] = config | {"version": version, "measure": config["measure"]}

    # Keep params in same order so filename is consistent
    params = dict(sorted(params.items()))

    # A unique file name for this set of params
    param_str = "_".join(f"{key}{value}" for key, value in params.items())
    data_file_name = f"data_{param_str}.csv"

    # Extra parameters -- not sure what they mean
    params = params | {
        "type": "final",
        "bundle": "",
        "step": "",
        "crosswalk": "",
        "clinical": False,
        "adjusted": False,
        "location": 1,
        "population": 1,
    }

    # Download CSV file
    text = await client.get(
        "/api/model/results/download", data_file_name, params=params
    )
    if not text:
        raise RuntimeError("No data available for the specified parameters")

    # Save to user-specified path
    print(f"Saving data to {output_path}")
    with open(output_path, "w") as file:
        file.write(text)


def parse_metadata(metadata: dict[str, Any]) -> Metadata:
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
    return parse_metadata(json.loads(text)["data"])


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        prog=APP_NAME, description="A tool to download datasets from IHME"
    )
    group = parser.add_argument_group()
    group.add_argument(
        "--dump-config",
        dest="dump_config",
        action="store_true",
        help="Dump metadata and example config files",
    )
    group.add_argument(
        "-c",
        "--config",
        dest="config_path",
        type=str,
        help="Path to config file",
    )
    group.add_argument(
        "-o",
        "--output",
        dest="output_path",
        type=str,
        help="Path to save CSV data to",
    )
    group.add_argument(
        "--no-cache",
        dest="no_cache",
        action="store_true",
        help=(
            "Redownload data files even if they are in the cache. This is useful for "
            "checking whether new data has become available since the last search."
        ),
    )

    # There might be a smarter way to validate arguments, but I can't figure it out
    try:
        if len(sys.argv) <= 1:
            raise RuntimeError()

        args = parser.parse_args()
        if args.dump_config:
            if args.config_path or args.output_path or args.no_cache:
                raise RuntimeError(
                    "--dump-config cannot be combined with other options"
                )
        elif not (args.config_path and args.output_path):
            raise RuntimeError("Both --config and --output options are required")
    except RuntimeError as ex:
        parser.print_usage()
        print(ex)
        raise SystemExit()

    return args


async def main() -> int:
    args = parse_args()

    async with AsyncClient(base_url=EPI_BASE_URL) as http_client:
        cache_path = platformdirs.user_cache_path(APP_NAME, ensure_exists=True)
        client = CacheClient(http_client, cache_path, args.no_cache)
        metadata = await load_metadata(client)

        if args.dump_config:
            write_json("metadata.json", metadata)
            write_json("example_config.json", EXAMPLE_CONFIG)

        if args.config_path:
            config = load_config(args.config_path, metadata)
            await download_data(client, config, args.output_path)

    return 0


if __name__ == "__main__":
    sys.exit(asyncio.run(main()))
