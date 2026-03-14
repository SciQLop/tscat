# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project

tscat is a Python library for storing, loading, and filtering time-series events in catalogues. Used in space physics / satellite data domains. SQLite-backed via SQLAlchemy with Alembic migrations.

## Build & Test Commands

```bash
make test          # Run pytest
make lint          # flake8 tscat tests (max-complexity=10, max-line-length=127)
make coverage      # pytest with coverage report
pytest tests/test_api.py::TestEvent::test_method  # Run a single test
mypy --files='**/*.py' --plugins=sqlalchemy.ext.mypy.plugin  # Type checking
bump2version patch|minor|major  # Version bump (updates pyproject.toml + __init__.py)
```

Build system is Flit (`pyproject.toml`). Linting and mypy configured in `setup.cfg`.

## Architecture

Three layers:

1. **Public API** (`tscat/base.py`): Module-level functions (`create_event`, `create_catalogue`, `get_events`, etc.) and wrapper classes `_Event`/`_Catalogue` that wrap ORM entities with validation, change tracking, and dynamic attribute support.

2. **Filtering** (`tscat/filtering.py`): Composable predicate DSL using a visitor pattern. Natural syntax: `filtering.event.author == "value"`, combinable with `&`, `|`, `~`. Predicates are converted to SQLAlchemy queries in the backend via a recursive visitor.

3. **ORM Backend** (`tscat/orm_sqlalchemy/`): Singleton `_Backend` with SQLite storage. ORM models in `orm.py`. Dynamic attributes stored as JSON columns. `Backend(testing=True)` creates in-memory databases for tests.

## Key Patterns

- **Session context manager** for batch operations — entities are flushed on `__exit__`
- **Dynamic attributes** via `**kwargs` on events/catalogues, stored as JSON, accessed through `__getattr__`/`__setattr__`
- **Soft deletes** with `remove()`/`restore()` and optional permanent deletion
- **Transaction control**: `save()`, `discard()`, `has_unsaved_changes()`
- **Import/export**: VOTable (XML) and JSON formats via `import_export.py`

## Testing Conventions

- Framework: `unittest` with `ddt` (data-driven tests) for parameterized test cases
- Each test resets the backend: `tscat.base._backend = Backend(testing=True)`
- Coverage excludes `test_mypy.py` and `test_perf.py` (see `.coveragerc`)
- CI runs on Python 3.10–3.14

## Validation Rules

- Attribute keys must match `^[A-Za-z][A-Za-z_0-9]*$`
- Tags/products: string lists, no commas allowed
- Rating: integer 1–10 or None
- UUIDs validated as v4
