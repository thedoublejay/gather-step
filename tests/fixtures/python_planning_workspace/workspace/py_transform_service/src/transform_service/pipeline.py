from collections.abc import AsyncIterator, Iterable

from shared_models.records import ParsedDocument, RawDocument

from .rules import (
    normalize_text,
    parse_payload as parse_record,
)


def pipeline_step(name: str):
    def decorate(function):
        function.pipeline_step = name
        return function

    return decorate


class AsyncBatch:
    async def __aenter__(self) -> "AsyncBatch":
        return self

    async def __aexit__(self, exc_type, exc, traceback) -> None:
        return None


async def stream_documents(documents: Iterable[RawDocument]) -> AsyncIterator[RawDocument]:
    for document in documents:
        yield document


@pipeline_step("document.transform")
async def transform_batch(documents: Iterable[RawDocument]) -> list[ParsedDocument]:
    parsed: list[ParsedDocument] = []

    def enrich(document: ParsedDocument) -> ParsedDocument:
        document.title = normalize_text(document.title)
        return document

    async with AsyncBatch():
        async for raw in stream_documents(documents):
            parsed.append(enrich(parse_record(raw)))

    return parsed
