======================
Time Series Catalogues
======================

.. image:: https://github.com/SciQLop/tscat/actions/workflows/test_main.yml/badge.svg
        :target: https://github.com/pboettch/tscat/actions/workflows/test_main.yml

.. image:: https://codecov.io/gh/SciQLop/tscat/branch/main/graph/badge.svg
        :target: https://codecov.io/gh/SciQLop/tscat
        :alt: Coverage Status


A library which stores, loads and filters time-series-events and associates them catalogues and
dynamic catalogues (filter-based).

Usage
=====
You can install the package using pip:
```bash
pip install tscat
```
You can also install the package from source:
```bash
git clone https://github.com/SciQLop/tscat
cd tscat
pip install -e .
```

Examples
========

Let's create a simple catalogue with some events and save it:

```python
from tscat import create_catalogue, create_event, add_events_to_catalogue, save
from datetime import datetime
# Create a catalogue
catalogue = create_catalogue("my_catalogue", author="John Doe", description="A sample catalogue", tags=["example", "sample"])
# Create some events
events = [
    create_event(start=datetime(2023, 1, 1), stop=datetime(2023, 1, 2), author="Alice", tags=["tag1", "tag2"]),
    create_event(start=datetime(2023, 1, 3), stop=datetime(2023, 1, 4), author="Bob", tags=["tag3"], some_custom_attribute="value1"),
]
# Add events to the catalogue
add_events_to_catalogue(catalogue, events)
# Save changes to the catalogue database
save()

```

Let's create a catalogue from a pandas DataFrame:
```python
import pandas as pd
from tscat import create_catalogue, create_event, add_events_to_catalogue, save

# Let's fetch some catalogue on GitHub
df = pd.read_pickle("https://helioforecast.space/static/sync/icmecat/HELIO4CAST_ICMECAT_v23_pandas.p")[0]

# Create events from the DataFrame
events = [
    create_event(
        start=row['mo_start_time'],
        stop=row['mo_end_time'],
        author="Christian Möstl",
        tags=["HELIO4CAST", "ICMECAT"],
        **{k: v for k, v in row.items() if k not in ['mo_start_time', 'mo_end_time']}
    )
    for _, row in df.iterrows()
]

# Create a catalogue from the DataFrame
catalogue = create_catalogue("icmecat", author="Christian Möstl", description="HELIO4CAST ICMECAT catalogue", tags=["HELIO4CAST", "ICMECAT"], events=events)

# save the catalogue
save()

Features
========
- Store and manage time-series events in catalogues
- Filter events using dynamic catalogue queries
- Load and export catalogue data
- Associate events with metadata and attributes
- Time-based filtering and search capabilities
- Python API for programmatic access



* Free software: GNU General Public License v3
* Documentation: https://tscat.readthedocs.io.
