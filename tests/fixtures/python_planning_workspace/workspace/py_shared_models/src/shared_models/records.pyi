from typing import Any


class SourceKind:
    HTML: str
    JSON: str
    TEXT: str


class RawDocument:
    source_id: str
    kind: SourceKind
    payload: dict[str, Any]


class ParsedDocument:
    source_id: str
    kind: SourceKind
    title: str
    payload: dict[str, Any]
