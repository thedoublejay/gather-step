# Python Planning Workspace Fixture

Neutral multi-repo Python workspace used for parser, indexing, and planning
quality work.

- `py_scraper_service` models source collection and task-style decorators.
- `py_transform_service` models async parsing and transformation logic.
- `py_shared_models` models shared dataclasses and `.pyi` stubs.
- `py_api_service` models a downstream app route calling the transform layer.

The fixture intentionally uses neutral repo and package names only.
