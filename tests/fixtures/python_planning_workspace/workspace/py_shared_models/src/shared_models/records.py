from dataclasses import dataclass, field
from enum import StrEnum
from typing import Any, TypeAlias

Payload: TypeAlias = dict[str, Any]


class SourceKind(StrEnum):
    HTML = "html"
    JSON = "json"
    TEXT = "text"


@dataclass(slots=True)
class RawDocument:
    source_id: str
    kind: SourceKind
    payload: Payload = field(default_factory=dict)


@dataclass(slots=True)
class ParsedDocument:
    source_id: str
    kind: SourceKind
    title: str
    payload: Payload = field(default_factory=dict)

    @classmethod
    def from_raw(cls, raw: RawDocument, *, title: str) -> "ParsedDocument":
        return cls(
            source_id=raw.source_id,
            kind=raw.kind,
            title=title,
            payload=raw.payload,
        )
