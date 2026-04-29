from typing import Any, TypeAlias

from .services import Runner

Payload: TypeAlias = dict[str, Any]


def route(path: str, *, methods: tuple[str, ...]):
    def decorate(function):
        return function

    return decorate


class TitleHandler(Runner):
    @route("/documents/{source_id}", methods=("POST", "PATCH"))
    async def handle(self, payload: Payload | None) -> str:
        def normalize(value: str) -> str:
            return value.strip()

        match payload:
            case {"title": title}:
                return normalize(str(title))
            case _:
                return normalize("untitled")


def build_title(payload: Payload) -> str:
    def normalize(value: str) -> str:
        return value.casefold()

    return normalize(str(payload.get("title", "")))


def summarize_title(payload: Payload) -> str:
    def normalize(value: str) -> str:
        return value.strip().title()

    return normalize(str(payload.get("title", "")))
