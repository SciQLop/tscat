import datetime as dt
import re
from dataclasses import dataclass
from typing import Any, Dict, Iterable, List, Optional, TYPE_CHECKING, Tuple, Union
from uuid import UUID, uuid4

from . import orm_sqlalchemy
from .filtering import Predicate

if TYPE_CHECKING:
    from .orm_sqlalchemy.orm import Event, Catalogue

_valid_key = re.compile(r'^[A-Za-z][A-Za-z_0-9]*$')

_backend = None


def backend() -> orm_sqlalchemy.Backend:
    global _backend
    if not _backend:  # pragma: no cover
        _backend = orm_sqlalchemy.Backend()  # during tests this line should never be called - this it's uncovered
    return _backend


def _listify(v) -> Union[List, Tuple]:
    if isinstance(v, (list, tuple)):
        return v
    else:
        return [v]


def _verify_attribute_names(kwargs: Dict) -> Dict:
    for k in kwargs.keys():
        if not _valid_key.match(k):
            raise ValueError('Invalid key-name for event-meta-data in kwargs:', k)
    return kwargs


class Session:
    def __init__(self) -> None:
        self.entities: List[Union['Event', 'Catalogue']] = []

    def __enter__(self) -> 'Session':
        return self

    def __exit__(self, exc_type, exc_value, exc_tb) -> None:
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
    def __init__(self):
        self._removed = False

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
        self._removed = True

        backend().remove(self._backend_entity, permanently=permanently)
        if permanently:
            del self._backend_entity

    def restore(self) -> None:
        backend().restore(self._backend_entity)

    def is_removed(self) -> bool:
        return self._removed


class _Event(_BackendBasedEntity):
    _fixed_keys = ['start', 'stop', 'author', 'uuid', 'tags', 'products', 'rating']

    def __init__(self, start: dt.datetime, stop: dt.datetime,
                 author: str,
                 uuid: Optional[str] = None,
                 tags: Iterable[str] = [],
                 products: Iterable[str] = [],
                 rating: Optional[int] = None,
                 _insert: bool = True,
                 **kwargs):
        self._in_ctor = True
        super().__init__()

        self.start = start
        self.stop = stop
        self.author = author
        self.tags = list(tags)
        self.products = list(products)
        self.rating = rating

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
                'rating': self.rating,
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
            if any(not isinstance(v, str) for v in value):
                raise ValueError("a tag has to be a string")
            if any(',' in v for v in value):
                raise ValueError("a string-list value shall not contain a comma")
        elif key == 'rating':
            if value is not None:
                if not isinstance(value, int):
                    raise ValueError("rating has to be an integer value")
                if value < 1 or value > 10:
                    raise ValueError("rating has to be between 1 and 10")

        super(_Event, self).__setattr__(key, value)

    def __repr__(self):
        return self.representation('Event')


class _Catalogue(_BackendBasedEntity):
    _fixed_keys = ['name', 'author', 'uuid', 'tags', 'predicate']

    def __init__(self, name: str, author: str,
                 uuid: Optional[str] = None,
                 tags: Iterable[str] = [],
                 predicate: Optional[Predicate] = None,
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
            if any(not isinstance(v, str) for v in value):
                raise ValueError("a tag has to be a string")
            if any(',' in v for v in value):
                raise ValueError("a string-list value shall not contain a comma")

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


def add_events_to_catalogue(catalogue: _Catalogue, events: Union[_Event, List[_Event]]) -> None:
    with Session() as s:
        s.add_events_to_catalogue(catalogue, events)


def remove_events_from_catalogue(catalogue: _Catalogue, events: Union[_Event, List[_Event]]) -> None:
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
        c._removed = removed_items
        catalogues += [c]
    return catalogues


def __backend_to_event(ev: Dict, removed_item: bool) -> _Event:
    e = _Event(ev['start'], ev['stop'], ev['author'], ev['uuid'], ev['tags'], ev['products'], ev['rating'],
               **ev['attributes'], _insert=False)
    e._removed = removed_item
    e._backend_entity = ev['entity']
    return e


def _get_events_from_predicate_or_none(base: Union[Predicate, None], removed_items: bool) -> List[_Event]:
    base_dict: Dict = {'removed': removed_items}
    if isinstance(base, Predicate):
        base_dict.update({'predicate': base})
    events = []
    for ev in backend().get_events(base_dict):
        events.append(__backend_to_event(ev, removed_items))
    return events


@dataclass
class EventQueryInformation:
    assigned: bool


def _get_events_from_catalogue(base: _Catalogue, removed_items: bool, assigned_only: bool, filtered_only: bool) \
    -> Tuple[List[_Event], List[EventQueryInformation]]:
    base_dict = {'entity': base._backend_entity,
                 'predicate': base.predicate,
                 'removed': removed_items}
    if assigned_only:
        del base_dict['predicate']
    if filtered_only:
        del base_dict['entity']

    events = []
    query_info = []
    for ev in backend().get_events(base_dict):
        events.append(__backend_to_event(ev, removed_items))
        query_info.append(EventQueryInformation(assigned=ev['is_assigned']))

    return events, query_info


# MultipleDispatch
def get_events(base: Union[Predicate, _Catalogue, None] = None,
               removed_items: bool = False,
               **kwargs: Any) -> Optional[Union[List[_Event], Tuple[List[_Event], List[EventQueryInformation]]]]:
    """ Get events from the database.
        If base is a Predicate, all events matching the predicate are returned.
        If base is a Catalogue, all events in the catalogue are returned.
        If base is None, all events are returned.
        If removed_items is True, also removed events are returned.
        If assigned_only is True, only events that are assigned to a catalogue are returned.
        If filtered_only is True, only events that are filtered by a catalogue are returned.

        Parameters
        ----------
        base: Predicate, Catalogue or None
            The base for the query (see above)
        removed_items: bool
            If True, also removed events are returned
        assigned_only: bool
            If True, only events that have been added (assigned) to a catalogue are returned. If a predicate is given
            this parameter is ignored.
        filtered_only: bool
            If True, only events that are filtered (matching the predicate) by the predicate of a
            catalogue are returned. If a predicate is given this parameter is ignored.
    """
    if base is None or isinstance(base, Predicate):
        return _get_events_from_predicate_or_none(base, removed_items)
    elif isinstance(base, _Catalogue):
        return _get_events_from_catalogue(base, removed_items,
                                          assigned_only=kwargs.get('assigned_only', False),
                                          filtered_only=kwargs.get('filtered_only', False))
    else:
        raise ValueError('base has to be a Predicate, Catalogue or None')  # pragma: no cover


def save() -> None:
    backend().commit()


def discard() -> None:
    backend().rollback()


def has_unsaved_changes() -> bool:
    return backend().has_unsaved_changes()

### Generic Import/Export
