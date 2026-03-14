=====
Usage
=====

tscat stores **events** (time intervals with metadata) in **catalogues**, persisted in a local SQLite database. This guide covers the full API with examples oriented toward space physics workflows.

Creating events
===============

An event has a start time, stop time, author, and optional metadata:

.. code-block:: python

    from datetime import datetime
    from tscat import create_event, save

    e = create_event(
        start=datetime(2023, 1, 15, 8, 30),
        stop=datetime(2023, 1, 15, 9, 15),
        author="Alice",
        tags=["MMS", "magnetopause"],
        products=["MMS1_FGM", "MMS1_FPI"],
        rating=8,
        # any extra keyword becomes a custom attribute
        Bz_min=-15.3,
        crossing_type="inbound",
    )
    save()

Fixed fields
------------

Every event has these built-in fields:

==========  ================  ==========================================
Field       Type              Notes
==========  ================  ==========================================
``start``   ``datetime``      Must be before ``stop``
``stop``    ``datetime``      Must be after ``start``
``author``  ``str``           Who created the event
``uuid``    ``str``           Auto-generated UUIDv4, or user-supplied
``tags``    ``list[str]``     Free-form labels
``products`` ``list[str]``    Data products associated with the event
``rating``  ``int`` or None   1–10 scale
==========  ================  ==========================================

Custom attributes
-----------------

Any keyword argument whose name matches ``[A-Za-z][A-Za-z_0-9]*`` is stored as a custom attribute. You can read, modify, and delete them like regular Python attributes:

.. code-block:: python

    print(e.Bz_min)        # -15.3
    e.Bz_min = -20.0       # update (auto-persisted)
    del e.crossing_type    # remove attribute
    save()


Creating catalogues
===================

A catalogue groups events under a name:

.. code-block:: python

    from tscat import create_catalogue, add_events_to_catalogue, save

    cat = create_catalogue(
        "Magnetopause crossings 2023",
        author="Alice",
        tags=["MMS", "magnetopause"],
        mission="MMS",  # custom attribute on the catalogue itself
    )

    # add events at creation time or later
    add_events_to_catalogue(cat, [e1, e2, e3])
    save()

You can also pass ``events=`` directly to ``create_catalogue``:

.. code-block:: python

    cat = create_catalogue("My catalogue", author="Alice", events=[e1, e2])
    save()


Retrieving data
===============

.. code-block:: python

    from tscat import get_catalogues, get_events, get_catalogue

    # all catalogues
    catalogues = get_catalogues()

    # a specific catalogue by name or UUID
    cat = get_catalogue(name="Magnetopause crossings 2023")

    # all events (no filter)
    all_events = get_events()

    # events in a catalogue (returns a tuple: events, query_info)
    events, info = get_events(cat)


Filtering events
================

The ``tscat.filtering`` module provides a composable predicate DSL. Import the ``event`` (or ``events``) token and build predicates with standard Python operators:

.. code-block:: python

    from tscat import get_events
    from tscat.filtering import event, In, Field

    # by time range
    jan_events = get_events(
        (event.start >= datetime(2023, 1, 1)) &
        (event.stop <= datetime(2023, 2, 1))
    )

    # by author
    get_events(event.author == "Alice")

    # by tag membership (tags and products are lists — use In)
    get_events(In("MMS", Field("tags")))

    # by custom attribute
    get_events(event.Bz_min < -10.0)

    # regex match on string fields
    get_events(event.author.matches("Al.*"))

    # negate with ~
    get_events(~(event.author == "Bob"))

    # combine with & (and) and | (or)
    get_events(
        In("magnetopause", Field("tags")) &
        (event.rating >= 7) &
        (event.Bz_min < -10.0)
    )

Checking attribute existence
----------------------------

For custom attributes that may not be present on all events:

.. code-block:: python

    from tscat.filtering import Attribute

    get_events(Attribute("Bz_min").exists())

Filtering catalogues
--------------------

The same DSL works for catalogues via the ``catalogue`` token:

.. code-block:: python

    from tscat import get_catalogues
    from tscat.filtering import catalogue, In, Field

    get_catalogues(catalogue.author == "Alice")
    get_catalogues(In("MMS", Field("tags")))
    get_catalogues(catalogue.name.matches("Magneto.*"))


Dynamic catalogues
==================

A catalogue with a ``predicate`` is dynamic — it automatically includes all events matching the filter, in addition to any manually assigned events:

.. code-block:: python

    from tscat import create_catalogue, get_events, save
    from tscat.filtering import event, In, Field

    dynamic_cat = create_catalogue(
        "High-quality MMS events",
        author="Alice",
        predicate=In("MMS", Field("tags")) & (event.rating >= 8),
    )
    save()

    # get_events returns matching events even if none were explicitly added
    events, info = get_events(dynamic_cat)

    # info[i].assigned tells you whether each event was manually added
    # or matched via the predicate
    for ev, qi in zip(events, info):
        print(ev.start, "assigned" if qi.assigned else "filtered")

You can also request only assigned or only filtered events:

.. code-block:: python

    assigned_events, _ = get_events(dynamic_cat, assigned_only=True)
    filtered_events, _ = get_events(dynamic_cat, filtered_only=True)


Batch operations with Session
=============================

When creating many events at once, use ``Session`` to batch database writes:

.. code-block:: python

    from tscat import Session, save

    with Session() as s:
        for row in my_data:
            s.create_event(
                start=row["start"],
                stop=row["stop"],
                author="pipeline",
                tags=["auto"],
            )
    save()

All entities created within a ``Session`` are flushed together on exit.


Importing from a pandas DataFrame
==================================

.. code-block:: python

    import pandas as pd
    from tscat import create_catalogue, Session, save

    df = pd.read_csv("events.csv", parse_dates=["start", "stop"])

    with Session() as s:
        events = [
            s.create_event(
                start=row["start"],
                stop=row["stop"],
                author="import",
                tags=["imported"],
            )
            for _, row in df.iterrows()
        ]
        cat = s.create_catalogue("Imported events", author="import")
        s.add_events_to_catalogue(cat, events)
    save()


Import and export
=================

tscat supports JSON and VOTable (AMDA-compatible) formats for sharing catalogues.

JSON
----

.. code-block:: python

    from tscat import export_json, import_json, save

    # export
    json_str = export_json(catalogue)

    # import (into the current database)
    imported_cats = import_json(json_str)
    save()

    # file-based
    from tscat import import_json_file
    imported_cats = import_json_file("catalogue.json")
    save()

VOTable
-------

.. code-block:: python

    from tscat import export_votable_str, import_votable_file, save

    # export to XML string
    xml_str = export_votable_str(catalogue)

    # import from file
    imported_cats = import_votable_file("catalogue.xml")
    save()

Imports are idempotent: re-importing the same data (matched by UUID) is a no-op. If an existing event has different values, a ``ValueError`` is raised.


Soft delete and restore
=======================

Events and catalogues support soft deletion:

.. code-block:: python

    from tscat import get_events, save

    event.remove()           # soft delete
    save()

    event.restore()          # undo soft delete
    save()

    event.remove(permanently=True)  # permanent, irreversible
    save()

To list removed items, pass ``removed_items=True``:

.. code-block:: python

    removed = get_events(removed_items=True)


Transaction control
===================

Changes are buffered until you call ``save()``. You can discard uncommitted changes or check for pending modifications:

.. code-block:: python

    from tscat import save, discard, has_unsaved_changes

    # make some changes ...
    if has_unsaved_changes():
        save()      # commit to database
        # or
        discard()   # rollback all pending changes


Discovering tags
================

To get all tags currently used across events and catalogues:

.. code-block:: python

    from tscat import existing_tags

    all_tags = existing_tags()  # returns a set of strings
