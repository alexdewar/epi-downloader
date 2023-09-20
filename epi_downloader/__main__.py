"""The entry point for EPI downloader."""
import asyncio
import json
from pprint import pprint
from typing import Any

from httpx_cache import AsyncClient, FileCache

from . import config


async def get_metadata(client: AsyncClient) -> dict[str, Any]:
    resp = await client.get("/api/metadata")
    resp.raise_for_status()
    return json.loads(resp.content)


async def main() -> None:
    async with AsyncClient(base_url=config.EPI_BASE_URL, cache=FileCache()) as client:
        metadata = await get_metadata(client)
        pprint(metadata)


if __name__ == "__main__":
    asyncio.run(main())
