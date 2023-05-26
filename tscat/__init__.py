"""Top-level package for Time Series Catalogues."""

__author__ = """Patrick Boettcher"""
__email__ = 'p@yai.se'
__version__ = '0.2.0'

from .base import create_event, create_catalogue, \
    add_events_to_catalogue, remove_events_from_catalogue, \
    save, discard, has_unsaved_changes, \
    get_catalogues, get_events, \
    _Catalogue, _Event, Session

from .import_export import export_votable, export_json, \
    import_json, import_votable, \
    __canonicalize_json_import as canonicalize_json_import, \
    __import_canonicalized_dict as import_canonicalized_dict
