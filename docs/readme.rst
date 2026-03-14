==============================
tscat — Time Series Catalogues
==============================

tscat is a Python library for managing catalogues of time intervals (events). It is the catalogue backend for `SciQLop <https://github.com/SciQLop/SciQLop>`_ and is designed for space physics workflows — storing event lists such as magnetopause crossings, ICMEs, or any user-defined intervals with arbitrary metadata.

Events are persisted in a local SQLite database. No server required.

Features
--------

- **Events with metadata**: store time intervals with tags, products, ratings, and arbitrary custom attributes
- **Catalogues**: group events into named collections
- **Dynamic catalogues**: define a filter predicate and the catalogue auto-populates with matching events
- **Filtering DSL**: query events and catalogues with a natural Python syntax (``event.Bz_min < -10``)
- **Import / export**: share catalogues as JSON or VOTable (AMDA-compatible)
- **Soft delete / restore**: non-destructive removal with undo support
- **Batch operations**: ``Session`` context manager for efficient bulk inserts

Quick start
-----------

.. code-block:: python

    from datetime import datetime
    from tscat import create_event, create_catalogue, add_events_to_catalogue, save

    cat = create_catalogue("Bow shock crossings", author="Alice", tags=["MMS"])

    events = [
        create_event(datetime(2023, 1, 1, 10, 0), datetime(2023, 1, 1, 10, 30),
                     author="Alice", tags=["inbound"], Bz_max=12.5),
        create_event(datetime(2023, 1, 3, 14, 0), datetime(2023, 1, 3, 14, 45),
                     author="Alice", tags=["outbound"], Bz_max=8.3),
    ]

    add_events_to_catalogue(cat, events)
    save()

See the :doc:`usage` guide for filtering, dynamic catalogues, import/export, and more.
