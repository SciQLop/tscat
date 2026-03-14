# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project

tscat is a Python library for storing, loading, and filtering time-series events in catalogues. Used in space physics / satellite data domains. SQLite-backed via SQLAlchemy 2.0 with Alembic migrations.

## Build & Test Commands

```bash
uv sync --extra test    # Set up dev environment
uv run pytest           # Run all tests
uv run pytest tests/test_api.py::TestEvent::test_method  # Single test
uv run flake8 tscat tests  # Lint (max-complexity=10, max-line-length=127)
uv build                # Build sdist + wheel
bump2version patch|minor|major  # Version bump (updates pyproject.toml + __init__.py)
```

Build backend is hatchling (`pyproject.toml`). Linting and mypy configured in `setup.cfg`.

## Architecture

Three layers:

1. **Public API** (`tscat/base.py`): Module-level functions (`create_event`, `create_catalogue`, `get_events`, etc.) and wrapper classes `_Event`/`_Catalogue` that wrap ORM entities with validation, change tracking, and dynamic attribute support.

2. **Filtering** (`tscat/filtering.py`): Composable predicate DSL using a visitor pattern. Natural syntax: `filtering.event.author == "value"`, combinable with `&`, `|`, `~`. Predicates are converted to SQLAlchemy queries in the backend via a recursive visitor. Predicates serialize to/from JSON dicts via `to_dict()`/`from_dict()`.

3. **ORM Backend** (`tscat/orm_sqlalchemy/`): Singleton `Backend` with SQLite storage. ORM models in `orm.py` use SA 2.0 patterns (`DeclarativeBase`, `Mapped[T]`, `mapped_column()`). Tags, products, and predicates stored as JSON columns. `Backend(testing=True)` creates in-memory databases for tests.

## Key Patterns

- **Session context manager** for batch operations — entities are flushed on `__exit__`
- **Dynamic attributes** via `**kwargs` on events/catalogues, stored as JSON, accessed through `__getattr__`/`__setattr__`
- **Soft deletes** with `remove()`/`restore()` and optional permanent deletion
- **Transaction control**: `save()`, `discard()`, `has_unsaved_changes()`
- **Import/export**: VOTable (XML) and JSON formats via `import_export.py`
- **Custom SQLite function** `json_array_contains` registered via engine `connect` event listener for tag/product filtering

## Testing Conventions

- Framework: `unittest` with `ddt` (data-driven tests) for parameterized test cases
- Each test resets the backend: `tscat.base._backend = Backend(testing=True)`
- Coverage excludes `test_mypy.py` and `test_perf.py` (see `.coveragerc`)
- CI runs on Python 3.10–3.14 using `astral-sh/setup-uv@v5`

## Validation Rules

- Attribute keys must match `^[A-Za-z][A-Za-z_0-9]*$`
- Tags/products: string lists
- Rating: integer 1–10 or None
- UUIDs validated as v4
