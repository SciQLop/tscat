"""Top-level package for Time Series Catalogues."""

__author__ = """Patrick Boettcher"""
__email__ = 'p@yai.se'
__version__ = '0.2.0'

from .filtering import Predicate, UUID as UUIDFilter

import json
import re
from typing import Dict, List, Union, Tuple, Iterable, Any, TYPE_CHECKING
from uuid import uuid4, UUID

from . import orm_sqlalchemy

if TYPE_CHECKING:
    from .orm_sqlalchemy.orm import Event, Catalogue

import datetime as dt

_valid_key = re.compile(r'^[A-Za-z][A-Za-z_0-9]*$')

_backend = None


def backend() -> orm_sqlalchemy.Backend:
    global _backend
    if not _backend:  # pragma: no cover
        _backend = orm_sqlalchemy.Backend()  # during tests this line should never be called - this it's uncovered
    return _backend


def _listify(v) -> Union[List, Tuple]:
    if type(v) in [list, tuple]:
        return v
    else:
        return [v]


def _verify_attribute_names(kwargs: Dict) -> Dict:
    for k in kwargs.keys():
        if not _valid_key.match(k):
            raise ValueError('Invalid key-name for event-meta-data in kwargs:', k)
    return kwargs


class Session:
    def __init__(self):
        self.entities: List[Union['Event', 'Catalogue']] = []

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_value, exc_tb):
        backend().add_and_flush(self.entities)

    def create_event(self, *args: Any, **kwargs: Any) -> '_Event':
        e = _Event(*args, **kwargs)
        self.entities.append(e._backend_entity)
        return e

    def create_catalogue(self, *args: Any, **kwargs: Any) -> '_Catalogue':
        c = _Catalogue(*args, **kwargs)
        self.entities.append(c._backend_entity)
        return c

    @staticmethod
    def add_events_to_catalogue(catalogue: '_Catalogue', events: Union['_Event', List['_Event']]):
        backend().add_events_to_catalogue(catalogue._backend_entity,
                                          [event._backend_entity for event in _listify(events)])

    @staticmethod
    def remove_events_from_catalogue(catalogue: '_Catalogue', events: Union['_Event', List['_Event']]):
        backend().remove_events_from_catalogue(catalogue._backend_entity,
                                               [event._backend_entity for event in _listify(events)])


class _BackendBasedEntity:
    def representation(self, name: str) -> str:
        fix = ', '.join(k + '=' + str(v) for k, v in self.fixed_attributes().items())
        kv = ', '.join(k + '=' + str(v) for k, v in self.variable_attributes().items())
        return f'{name}({fix}) attributes({kv})'

    def dump(self) -> dict:
        ret = self.variable_attributes()
        for k, v in self.__dict__.items():
            if k in self._fixed_keys:
                ret[k] = v
        return ret

    def variable_attributes(self) -> dict:
        ret = {}
        for k, v in self.__dict__.items():
            if k in self._fixed_keys:
                continue
            if _valid_key.match(k):
                ret[k] = v
        return ret

    def fixed_attributes(self) -> dict:
        return {k: self.__dict__[k] for k in self._fixed_keys}

    def __getattr__(self, name):
        if name == '_backend_entity' and name not in self.__dict__:
            raise ValueError("You are attempting an operation on an invalid object, " +
                             "this may be because your object has been deleted, please try creating or " +
                             "getting a new one.")
        return super(_BackendBasedEntity, self).__getattr__(name)

    def __setattr__(self, key, value):
        super(_BackendBasedEntity, self).__setattr__(key, value)

        if key != '_in_ctor' and not self._in_ctor:
            if key in self._fixed_keys:
                backend().update_field(self._backend_entity, key, value)
            elif _valid_key.match(key):
                backend().update_attribute(self._backend_entity, key, value)

    def __delattr__(self, key):
        super(_BackendBasedEntity, self).__delattr__(key)

        # only allow deletion of attributes
        if key in self._fixed_keys:
            raise IndexError('Fixed keys cannot be deleted.')
        if _valid_key.match(key):
            backend().delete_attribute(self._backend_entity, key)

    def __eq__(self, o):
        if sorted(filter(_valid_key.match, self.__dict__.keys())) != \
            sorted(filter(_valid_key.match, o.__dict__.keys())):
            return False

        for k in sorted(filter(_valid_key.match, self.__dict__.keys())):
            if self.__dict__[k] != o.__dict__[k]:
                return False
        return True

    def remove(self, permanently: bool = False) -> None:
        backend().remove(self._backend_entity, permanently=permanently)
        if permanently:
            del self._backend_entity

    def is_removed(self) -> bool:
        return backend().is_removed(self._backend_entity)

    def restore(self) -> None:
        backend().restore(self._backend_entity)


class _Event(_BackendBasedEntity):
    _fixed_keys = ['start', 'stop', 'author', 'uuid', 'tags', 'products']

    def __init__(self, start: dt.datetime, stop: dt.datetime,
                 author: str,
                 uuid: str = None,
                 tags: Iterable[str] = [],
                 products: Iterable[str] = [],
                 _insert: bool = True,
                 **kwargs):
        self._in_ctor = True
        super().__init__()

        self.start = start
        self.stop = stop
        self.author = author
        self.tags = list(tags)
        self.products = list(products)

        if not uuid:
            self.uuid = str(uuid4())
        else:
            self.uuid = uuid

        _verify_attribute_names(kwargs)
        self.__dict__.update(kwargs)

        if _insert:
            self._backend_entity = backend().add_event({
                'start': self.start,
                'stop': self.stop,
                'author': self.author,
                'uuid': self.uuid,
                'tags': self.tags,
                'products': self.products,
                'attributes': kwargs,
            })

        self._in_ctor = False

    def __setattr__(self, key, value):
        if key == 'uuid':
            UUID(value, version=4)  # throws an exception if not valid
        elif key == 'start' and hasattr(self, 'stop'):
            if value > self.stop:
                raise ValueError("start date has to be before stop date")
        elif key == 'stop' and hasattr(self, 'start'):
            if value < self.start:
                raise ValueError("stop date has to be after start date")
        elif key in ['tags', 'products']:
            if any(type(v) != str for v in value):
                raise ValueError("a tag has to be a string")

        super(_Event, self).__setattr__(key, value)

    def __repr__(self):
        return self.representation('Event')


class _Catalogue(_BackendBasedEntity):
    _fixed_keys = ['name', 'author', 'uuid', 'tags', 'predicate']

    def __init__(self, name: str, author: str,
                 uuid: str = None,
                 tags: Iterable[str] = [],
                 predicate: Predicate = None,
                 _insert: bool = True,
                 **kwargs):
        self._in_ctor = True

        super().__init__()

        self.name = name
        self.author = author

        if not uuid:
            self.uuid = str(uuid4())
        else:
            self.uuid = uuid

        self.tags = list(tags)
        self.predicate = predicate

        _verify_attribute_names(kwargs)
        self.__dict__.update(kwargs)

        if _insert:
            self._backend_entity = backend().add_catalogue({
                'name': self.name,
                'author': self.author,
                'uuid': self.uuid,
                'tags': self.tags,
                'predicate': self.predicate,
                'attributes': kwargs,
            })

        self._in_ctor = False

    def is_dynamic(self):
        return self.predicate is not None

    def __setattr__(self, key, value):
        if key == 'uuid':
            UUID(value, version=4)  # throws an exception if not valid
        elif key == 'name':
            if not value:
                raise ValueError('Catalogue name cannot be emtpy.')
        elif key == 'tags':
            if any(type(v) != str for v in value):
                raise ValueError("a tag has to be a string")

        super(_Catalogue, self).__setattr__(key, value)

    def __repr__(self):
        return self.representation('Catalogue')


def create_event(*args, **kwargs) -> _Event:
    with Session() as s:
        return s.create_event(*args, **kwargs)


def create_catalogue(*args, events: List[_Event] = [], **kwargs) -> _Catalogue:
    with Session() as s:
        c = s.create_catalogue(*args, **kwargs)
        s.add_events_to_catalogue(c, events)
        return c


def add_events_to_catalogue(catalogue: _Catalogue, events: List[_Event]) -> None:
    with Session() as s:
        s.add_events_to_catalogue(catalogue, events)


def remove_events_from_catalogue(catalogue: _Catalogue, events: List[_Event]) -> None:
    with Session() as s:
        s.remove_events_from_catalogue(catalogue, events)


def get_catalogues(base: Union[Predicate, _Event, None] = None, removed_items: bool = False) -> List[_Catalogue]:
    base_dict: Dict[str, Union[Predicate, 'Event', None, bool]]

    if isinstance(base, Predicate):
        base_dict = {'predicate': base}
    elif isinstance(base, _Event):
        base_dict = {'entity': base._backend_entity}
    else:
        base_dict = {}

    base_dict.update({'removed': removed_items})

    catalogues = []
    for cat in backend().get_catalogues(base_dict):
        c = _Catalogue(cat['name'], cat['author'], cat['uuid'], cat['tags'], cat['predicate'],
                       _insert=False, **cat['attributes'])
        c._backend_entity = cat['entity']
        catalogues += [c]
    return catalogues


def get_events(base: Union[Predicate, _Catalogue, None] = None, removed_items: bool = False) -> List[_Event]:
    base_dict: Dict[str, Union[Predicate, 'Catalogue', None, bool]]
    if isinstance(base, Predicate):
        base_dict = {'predicate': base}
    elif isinstance(base, _Catalogue):
        base_dict = {'entity': base._backend_entity,
                     'predicate': base.predicate}
    else:
        base_dict = {}

    base_dict.update({'removed': removed_items})

    events = []
    for ev in backend().get_events(base_dict):
        e = _Event(ev['start'], ev['stop'], ev['author'], ev['uuid'], ev['tags'], ev['products'],
                   **ev['attributes'], _insert=False)
        e._backend_entity = ev['entity']
        events.append(e)
    return events


def save() -> None:
    backend().commit()


def discard() -> None:
    backend().rollback()


def has_unsaved_changes() -> bool:
    return backend().has_unsaved_changes()


def export_json(catalogue: _Catalogue) -> str:
    events = get_events(catalogue)

    events_uuids = [event.uuid for event in events]

    catalogue_dump = catalogue.dump()
    catalogue_dump.update({"events": events_uuids})
    export_dict = {
        'catalogues': [catalogue_dump],
        'events': [event.dump() for event in events],
    }
    return json.dumps(export_dict, default=str)


def canonicalize_json_import(jsons: str) -> dict:
    import_dict = json.loads(jsons)

    # check events and catalogues for existing entities
    # if existing and identical - remove from import-dict
    # if existing and not identical raise
    # if not existing import


    uuids = [event['uuid'] for event in import_dict['events']]
    events = backend().get_events_by_uuid_list(uuids)

    for event in import_dict['events'][:]:
        if event['uuid'] not in events:
            continue

        check_event = events[event['uuid']]
        del check_event['entity']
        check_event.update(check_event['attributes'])
        del check_event['attributes']

        check_event['start'] = str(check_event['start'])
        check_event['stop'] = str(check_event['stop'])

        if check_event != event:
            raise ValueError(f'Import: event with UUID {event["uuid"]} already exists in database, ' +
                             'but with different values.')
        import_dict['events'].remove(event)

    for catalogue in import_dict['catalogues'][:]:
        check_catalogue = get_catalogues(UUIDFilter(catalogue['uuid']))
        if len(check_catalogue) != 0:
            events_uuids = [event.uuid for event in get_events(check_catalogue[0])]
            catalogue_dump = check_catalogue[0].dump()
            catalogue_dump.update({'events': events_uuids})

            if catalogue_dump != catalogue:
                raise ValueError(f'Import: catalogue with UUID {catalogue["uuid"]} already exists in database, ' +
                                 'but with different values.')
            import_dict['catalogues'].remove(catalogue)

    return import_dict


def import_canonicalized_dict(import_dict: dict) -> None:
    event_of_uuid = {}

    # import all new events
    with Session() as s:
        for event in import_dict['events']:
            event['start'] = dt.datetime.fromisoformat(event['start'])
            event['stop'] = dt.datetime.fromisoformat(event['stop'])
            event_of_uuid[event['uuid']] = s.create_event(**event)

        for catalogue_dict in import_dict['catalogues']:
            catalogue_events = [event_of_uuid[uuid] if uuid in event_of_uuid
                                else get_events(UUIDFilter(uuid))[0]
                                for uuid in catalogue_dict['events']]
            del catalogue_dict['events']

            catalogue = s.create_catalogue(**catalogue_dict)
            s.add_events_to_catalogue(catalogue, catalogue_events)


def import_json(jsons: str) -> None:
    import_dict = canonicalize_json_import(jsons)
    import_canonicalized_dict(import_dict)
