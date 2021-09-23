"""Top-level package for Time Series Catalogues."""

__author__ = """Patrick Boettcher"""
__email__ = 'p@yai.se'
__version__ = '0.0.0'

from .filtering import Predicate

import re
from typing import Dict, List, Union, Tuple
from typeguard import typechecked
from uuid import uuid4, UUID

from . import orm_sqlalchemy

import datetime as dt

_valid_key = re.compile(r'^[A-Za-z][A-Za-z_0-9]*$')

_backend = None


def backend():
    global _backend
    if not _backend:
        _backend = orm_sqlalchemy.Backend()  # during tests this line should never be called - this it's uncovered
    return _backend


@typechecked
def _listify(v) -> Union[List, Tuple]:
    if type(v) in [list, tuple]:
        return v
    else:
        return [v]


@typechecked
def _verify_attribute_names(kwargs: Dict) -> Dict:
    for k in kwargs.keys():
        if not _valid_key.match(k):
            raise ValueError('Invalid key-name for event-meta-data in kwargs:', k)
    return kwargs


@typechecked
class _BackendBasedEntity:
    def representation(self, name: str) -> str:
        fix = ', '.join(k + '=' + str(self.__dict__[k]) for k in self._fixed_keys)
        kv = ', '.join(k + '=' + str(v) for k, v in self.variable_attributes_as_dict().items())
        return f'{name}({fix}) attributes({kv})'

    def variable_attributes_as_dict(self) -> Dict:
        ret = {}
        for k, v in self.__dict__.items():
            if k in self._fixed_keys:
                continue
            if _valid_key.match(k):
                ret[k] = v
        return ret

    def __setattr__(self, key, value):
        super(_BackendBasedEntity, self).__setattr__(key, value)

        if key != '_in_ctor' and not self._in_ctor:
            # TODO use backend().add
            if key in self._fixed_keys:
                setattr(self._backend_entity, key, value)
            elif _valid_key.match(key):
                self._backend_entity[key] = value

    def __delattr__(self, key):
        super(_BackendBasedEntity, self).__delattr__(key)

        # only allow deletion of attributes
        if key in self._fixed_keys:
            raise IndexError('Fixed keys cannot be deleted.')
        if _valid_key.match(key):
            # backend().del
            del self._backend_entity[key]

    def __eq__(self, o):
        if sorted(filter(_valid_key.match, self.__dict__.keys())) != \
            sorted(filter(_valid_key.match, o.__dict__.keys())):
            return False

        for k in sorted(filter(_valid_key.match, self.__dict__.keys())):
            if self.__dict__[k] != o.__dict__[k]:
                return False
        return True


@typechecked
class Event(_BackendBasedEntity):
    _fixed_keys = ['start', 'stop', 'author', 'uuid']

    def __init__(self, start: dt.datetime, stop: dt.datetime,
                 author: str,
                 uuid: str = None,
                 _insert: bool = True,
                 **kwargs):
        self._in_ctor = True
        super().__init__()

        self.start = start
        self.stop = stop
        self.author = author

        if not uuid:
            self.uuid = str(uuid4())
        else:
            self.uuid = uuid

        _verify_attribute_names(kwargs)
        self.__dict__.update(kwargs)

        if _insert:
            self._backend_entity = backend().add_or_update_event({
                'start': self.start,
                'stop': self.stop,
                'author': self.author,
                'uuid': self.uuid,
                'attributes': kwargs,
            })

        self._in_ctor = False

    def __setattr__(self, key, value):
        if key == 'uuid':
            UUID(value, version=4)  # throws an exception if not valid
        elif key == 'start' and hasattr(self, 'stop'):
            if value >= self.stop:
                raise ValueError("start-datetime has to be younger than end-datetime")
        elif key == 'stop' and hasattr(self, 'start'):
            if value <= self.start:
                raise ValueError("stop-datetime has to be younger than end-datetime")

        super(Event, self).__setattr__(key, value)

    def __repr__(self):
        return self.representation('Event')


@typechecked
class Catalogue(_BackendBasedEntity):
    _fixed_keys = ['name', 'author', 'predicate']

    def __init__(self, name: str, author: str,
                 predicate: Predicate = None,
                 events: List[Event] = None,
                 _insert: bool = True,
                 **kwargs):
        self._in_ctor = True

        super().__init__()

        self.name = name
        self.author = author
        self.predicate = predicate

        _verify_attribute_names(kwargs)
        self.__dict__.update(kwargs)

        if _insert:
            self._backend_entity = backend().add_or_update_catalogue({
                'name': self.name,
                'author': self.author,
                'predicate': self.predicate,
                'attributes': kwargs,
            })

            if events:
                self.add_events(events)

        self._in_ctor = False

    def add_events(self, events: Union[Event, List[Event]]):
        backend().add_events_to_catalogue(self._backend_entity,
                                          [event._backend_entity for event in _listify(events)])

    def remove_events(self, events: Union[Event, List[Event]]):
        backend().remove_events_from_catalogue(self._backend_entity,
                                               [event._backend_entity for event in _listify(events)])

    def is_dynamic(self):
        return self.predicate is not None

    def __setattr__(self, key, value):
        if key == 'name':
            if not value:
                raise ValueError('Catalogue name cannot be emtpy.')

        super(Catalogue, self).__setattr__(key, value)

    def __repr__(self):
        return self.representation('Catalogue')


@typechecked
def get_catalogues(base: Union[Predicate, Event, None] = None) -> List[Catalogue]:
    if isinstance(base, Predicate):
        base = {'predicate': base}
    elif isinstance(base, Event):
        base = {'entity': base._backend_entity}
    else:
        base = {}

    catalogues = []
    for cat in backend().get_catalogues(base):
        c = Catalogue(cat['name'], cat['author'], cat['predicate'],
                      None, **cat['attributes'], _insert=False)
        c._backend_entity = cat['entity']
        catalogues += [c]
    return catalogues


@typechecked
def get_events(base: Union[Predicate, Catalogue, None] = None) -> List[Event]:
    if isinstance(base, Predicate):
        base = {'predicate': base}
    elif isinstance(base, Catalogue):
        base = {'entity': base._backend_entity,
                'predicate': base.predicate}
    else:
        base = {}

    events = []
    for ev in backend().get_events(base):
        e = Event(ev['start'], ev['stop'], ev['author'], ev['uuid'], **ev['attributes'], _insert=False)
        e._backend_entity = ev['entity']
        events.append(e)
    return events


@typechecked
def save() -> None:
    backend().commit()


@typechecked
def discard() -> None:
    backend().rollback()


@typechecked
def has_unsaved_changes() -> bool:
    return backend().has_unsaved_changes()
