from shared_models.records import RawDocument, SourceKind


class SourceClient:
    async def load(self, source_id: str) -> RawDocument:
        return RawDocument(
            source_id=source_id,
            kind=SourceKind.HTML,
            payload={"html": "<article>sample</article>"},
        )
