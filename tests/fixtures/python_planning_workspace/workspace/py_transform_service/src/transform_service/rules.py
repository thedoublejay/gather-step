from shared_models.records import ParsedDocument, RawDocument, SourceKind


def normalize_text(value: str) -> str:
    return " ".join(value.split())


def parse_payload(raw: RawDocument) -> ParsedDocument:
    match raw.kind:
        case SourceKind.HTML:
            title = normalize_text(str(raw.payload.get("html", "")))
        case SourceKind.JSON:
            title = normalize_text(str(raw.payload.get("title", "")))
        case _:
            title = "untitled"
    return ParsedDocument.from_raw(raw, title=title)
