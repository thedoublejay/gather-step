import numpy as np

from shared_models.records import RawDocument

from .clients import SourceClient as Client


def task(name: str):
    def decorate(function):
        function.task_name = name
        return function

    return decorate


class BaseScraper:
    def __init__(self, client: Client) -> None:
        self.client = client


class SourceScraper(BaseScraper):
    async def collect(self, source_ids: list[str]) -> list[RawDocument]:
        records: list[RawDocument] = []
        for source_id in source_ids:
            records.append(await self.client.load(source_id))
        return records


@task("scrape.sources")
async def fetch_sources(source_ids: list[str]) -> list[RawDocument]:
    scraper = SourceScraper(Client())
    source_array = np.array(source_ids)
    return await scraper.collect(source_array.tolist())
