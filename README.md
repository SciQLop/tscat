# tscat — Time Series Catalogues

![Test Status](https://github.com/SciQLop/tscat/actions/workflows/test_main.yml/badge.svg)
![Coverage Status](https://codecov.io/gh/SciQLop/tscat/branch/main/graph/badge.svg)

tscat is a Python library for managing catalogues of time intervals (events). It is the catalogue backend for [SciQLop](https://github.com/SciQLop/SciQLop) and is designed for space physics workflows — storing event lists such as magnetopause crossings, ICMEs, or any user-defined intervals with arbitrary metadata.

Events are persisted in a local SQLite database. No server required.

## Installation

```bash
pip install tscat
```

## Quick start

```python
from datetime import datetime
from tscat import create_event, create_catalogue, add_events_to_catalogue, save

catalogue = create_catalogue("Bow shock crossings", author="Alice",
                             tags=["MMS", "bow_shock"])

events = [
    create_event(datetime(2023, 1, 1, 10, 0), datetime(2023, 1, 1, 10, 30),
                 author="Alice", tags=["inbound"], Bz_max=12.5),
    create_event(datetime(2023, 1, 3, 14, 0), datetime(2023, 1, 3, 14, 45),
                 author="Alice", tags=["outbound"], Bz_max=8.3),
]

add_events_to_catalogue(catalogue, events)
save()
```

Any keyword argument beyond the fixed fields (`start`, `stop`, `author`, `tags`, `products`, `rating`) becomes a custom attribute stored alongside the event.

## Filtering

Retrieve events matching conditions using a natural Python DSL:

```python
from tscat import get_events
from tscat.filtering import event, In, Field

# Events after a date
get_events(event.start >= datetime(2023, 1, 2))

# Combine predicates with &, |, ~
get_events((event.author == "Alice") & In("inbound", Field("tags")))

# Filter on custom attributes
get_events(event.Bz_max > 10.0)
```

## Dynamic catalogues

A catalogue with a `predicate` automatically includes all matching events:

```python
from tscat import create_catalogue
from tscat.filtering import event

dynamic = create_catalogue(
    "High Bz events", author="Alice",
    predicate=event.Bz_max > 10.0,
)
```

## Import / export

Share catalogues as JSON or VOTable (AMDA-compatible):

```python
from tscat import export_json, import_json, export_votable_str

json_str = export_json(catalogue)
import_json(json_str)  # into another database
```

## Documentation

Full usage guide: [docs/usage.rst](docs/usage.rst)

## Development

This project uses [uv](https://docs.astral.sh/uv/):

```bash
uv sync --extra test
uv run pytest
uv run flake8 tscat tests
```

## License

GNU General Public License v3
