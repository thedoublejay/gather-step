from shared_models.records import RawDocument, SourceKind
from transform_service.pipeline import transform_batch


def post(path: str):
    def decorate(function):
        function.route_path = path
        return function

    return decorate


class App:
    def post(self, path: str):
        return post(path)


@post("/documents/ingest")
async def ingest_documents(payload: list[dict[str, object]]):
    raw_documents = [
        RawDocument(
            source_id=str(index),
            kind=SourceKind.JSON,
            payload=document,
        )
        for index, document in enumerate(payload)
    ]
    return await transform_batch(raw_documents)


def register_routes(app: App) -> None:
    app.post("/documents/ingest")(ingest_documents)
