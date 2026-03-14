import datetime as dt
import re
from dataclasses import dataclass
from typing import Any, Dict, Iterable, List, Optional, TYPE_CHECKING, Tuple, Union, Set
from uuid import UUID, uuid4

from . import orm_sqlalchemy
from .filtering import Predicate

if TYPE_CHECKING:
    from .orm_sqlalchemy.orm import Event, Catalogue

_valid_key = re.compile(r'^[A-Za-z][A-Za-z_0-9]*$')

_backend = None


class _LazyBackendEntity:
    """Defers ORM entity loading until a write operation needs it."""
    __slots__ = ('_row_id', '_entity')

    def __init__(self, row_id: int):
        self._row_id = row_id
        self._entity = None

    def _resolve(self):
        if self._entity is None:
            from .orm_sqlalchemy.orm import Event
            self._entity = backend().session.get(Event, self._row_id)
        return self._entity

    def __getattr__(self, name):
        return getattr(self._resolve(), name)

    def __setattr__(self, name, value):
        if name in ('_row_id', '_entity'):
            object.__setattr__(self, name, value)
        else:
            setattr(self._resolve(), name, value)


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

    def _track(self, entity) -> None:
        self.entities.append(entity)

    def create_event(self, *args: Any, **kwargs: Any) -> '_Event':
        e = _Event(*args, **kwargs)
        self._track(e._backend_entity)
        return e

    def create_catalogue(self, *args: Any, **kwargs: Any) -> '_Catalogue':
        c = _Catalogue(*args, **kwargs)
        self._track(c._backend_entity)
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
        ret = {}
        for k, v in self.__dict__.items():
            if k in self._fixed_keys or _valid_key.match(k):
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
        self_keys = sorted(k for k in self.__dict__ if _valid_key.match(k))
        o_keys = sorted(k for k in o.__dict__ if _valid_key.match(k))
        if self_keys != o_keys:
            return False
        return all(self.__dict__[k] == o.__dict__[k] for k in self_keys)

    def remove(self, permanently: bool = False) -> None:
        self._removed = True

        backend().remove(self._backend_entity, permanently=permanently)
        if permanently:
            del self._backend_entity

    def restore(self) -> None:
        self._removed = False
        backend().restore(self._backend_entity)

    def is_removed(self) -> bool:
        return self._removed


class _Event(_BackendBasedEntity):
    _fixed_keys = ['start', 'stop', 'author', 'uuid', 'tags', 'products', 'rating']

    def __init__(self, start: dt.datetime, stop: dt.datetime,
                 author: str,
                 uuid: Optional[str] = None,
                 tags: Optional[Iterable[str]] = None,
                 products: Optional[Iterable[str]] = None,
                 rating: Optional[int] = None,
                 _insert: bool = True,
                 **kwargs):
        _set = object.__setattr__
        _set(self, '_in_ctor', True)
        _set(self, '_removed', False)

        if start > stop:
            raise ValueError("start date has to be before stop date")
        _set(self, 'start', start)
        _set(self, 'stop', stop)
        _set(self, 'author', author)

        tags_list = list(tags) if tags else []
        if any(not isinstance(v, str) for v in tags_list):
            raise ValueError("a tag has to be a string")
        _set(self, 'tags', tags_list)

        products_list = list(products) if products else []
        if any(not isinstance(v, str) for v in products_list):
            raise ValueError("a product has to be a string")
        _set(self, 'products', products_list)

        if rating is not None:
            if not isinstance(rating, int):
                raise ValueError("rating has to be an integer value")
            if rating < 1 or rating > 10:
                raise ValueError("rating has to be between 1 and 10")
        _set(self, 'rating', rating)

        if not uuid:
            _set(self, 'uuid', str(uuid4()))
        else:
            UUID(uuid, version=4)
            _set(self, 'uuid', uuid)

        _verify_attribute_names(kwargs)
        self.__dict__.update(kwargs)

        if _insert:
            _set(self, '_backend_entity', backend().add_event({
                'start': self.start,
                'stop': self.stop,
                'author': self.author,
                'uuid': self.uuid,
                'tags': self.tags,
                'products': self.products,
                'rating': self.rating,
                'attributes': kwargs,
            }))

        _set(self, '_in_ctor', False)

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
        elif key == 'rating':
            if value is not None:
                if not isinstance(value, int):
                    raise ValueError("rating has to be an integer value")
                if value < 1 or value > 10:
                    raise ValueError("rating has to be between 1 and 10")

        super(_Event, self).__setattr__(key, value)

    @classmethod
    def _from_db(cls, entity, removed: bool = False) -> '_Event':
        _set = object.__setattr__
        obj = object.__new__(cls)
        _set(obj, '_in_ctor', True)
        _set(obj, '_removed', removed)
        _set(obj, 'start', entity.start)
        _set(obj, 'stop', entity.stop)
        _set(obj, 'author', entity.author)
        _set(obj, 'uuid', entity.uuid)
        _set(obj, 'tags', entity.tags)
        _set(obj, 'products', entity.products)
        _set(obj, 'rating', entity.rating)
        for k, v in (entity.attributes or {}).items():
            _set(obj, k, v)
        _set(obj, '_backend_entity', entity)
        _set(obj, '_in_ctor', False)
        return obj

    @classmethod
    def _from_row(cls, row, removed: bool = False) -> '_Event':
        obj = object.__new__(cls)
        d = {
            '_in_ctor': True,
            '_removed': removed,
            'start': row[1],
            'stop': row[2],
            'author': row[3],
            'uuid': row[4],
            'tags': row[5],
            'products': row[6],
            'rating': row[7],
        }
        attrs = row[8]
        if attrs:
            d.update(attrs)
        d['_backend_entity'] = _LazyBackendEntity(row[0])
        d['_in_ctor'] = False
        object.__setattr__(obj, '__dict__', d)
        return obj

    def __repr__(self):
        return self.representation('Event')


class _Catalogue(_BackendBasedEntity):
    _fixed_keys = ['name', 'author', 'uuid', 'tags', 'predicate']

    def __init__(self, name: str, author: str,
                 uuid: Optional[str] = None,
                 tags: Optional[Iterable[str]] = None,
                 predicate: Optional[Predicate] = None,
                 _insert: bool = True,
                 **kwargs):
        _set = object.__setattr__
        _set(self, '_in_ctor', True)
        _set(self, '_removed', False)

        if not name:
            raise ValueError('Catalogue name cannot be empty.')
        _set(self, 'name', name)
        _set(self, 'author', author)

        if not uuid:
            _set(self, 'uuid', str(uuid4()))
        else:
            UUID(uuid, version=4)
            _set(self, 'uuid', uuid)

        tags_list = list(tags) if tags else []
        if any(not isinstance(v, str) for v in tags_list):
            raise ValueError("a tag has to be a string")
        _set(self, 'tags', tags_list)
        _set(self, 'predicate', predicate)

        _verify_attribute_names(kwargs)
        self.__dict__.update(kwargs)

        if _insert:
            _set(self, '_backend_entity', backend().add_catalogue({
                'name': self.name,
                'author': self.author,
                'uuid': self.uuid,
                'tags': self.tags,
                'predicate': self.predicate,
                'attributes': kwargs,
            }))

        _set(self, '_in_ctor', False)

    def is_dynamic(self):
        return self.predicate is not None

    def __setattr__(self, key, value):
        if key == 'uuid':
            UUID(value, version=4)  # throws an exception if not valid
        elif key == 'name':
            if not value:
                raise ValueError('Catalogue name cannot be empty.')
        elif key == 'tags':
            if any(not isinstance(v, str) for v in value):
                raise ValueError("a tag has to be a string")

        super(_Catalogue, self).__setattr__(key, value)

    @classmethod
    def _from_db(cls, entity, removed: bool = False) -> '_Catalogue':
        _set = object.__setattr__
        obj = object.__new__(cls)
        _set(obj, '_in_ctor', True)
        _set(obj, '_removed', removed)
        _set(obj, 'name', entity.name)
        _set(obj, 'author', entity.author)
        _set(obj, 'uuid', entity.uuid)
        _set(obj, 'tags', entity.tags)
        _set(obj, 'predicate', Predicate.from_dict(entity.predicate) if entity.predicate else None)
        for k, v in (entity.attributes or {}).items():
            _set(obj, k, v)
        _set(obj, '_backend_entity', entity)
        _set(obj, '_in_ctor', False)
        return obj

    def __repr__(self):
        return self.representation('Catalogue')


def create_event(start: dt.datetime, stop: dt.datetime,
                 author: str,
                 uuid: Optional[str] = None,
                 tags: Optional[Iterable[str]] = None,
                 products: Optional[Iterable[str]] = None,
                 rating: Optional[int] = None, **kwargs) -> _Event:
    """Create a new event in the database.

    Parameters
    ----------
    start: datetime
        The start time of the event.
    stop: datetime
        The stop time of the event.
    author: str
        The author of the event.
    uuid: str, optional
        A unique identifier for the event. If not provided, a new UUID will be generated.
    tags: Iterable[str], optional
        A list of tags associated with the event.
    products: Iterable[str], optional
        A list of products associated with the event.
    rating: int, optional
        A rating for the event, between 1 and 10. If not provided, no rating is set.
    kwargs: dict, optional
        Additional attributes for the event. Keys must be valid Python identifiers.
    Returns
    -------
    _Event
        The created event object.
    Raises
    ------
    ValueError
        If the start date is after the stop date, or if the rating is not between 1 and 10.
        If any tag or product contains a comma or is not a string.
        If any key in kwargs is not a valid Python identifier.

    See also
    --------
    create_catalogue: For creating catalogues that can contain events.
    """
    with Session() as s:
        return s.create_event(
            start=start,
            stop=stop,
            author=author,
            uuid=uuid,
            tags=tags or [],
            products=products or [],
            rating=rating,
            **kwargs
        )


def create_catalogue(name: str, author: str,
                     uuid: Optional[str] = None,
                     tags: Optional[Iterable[str]] = None,
                     predicate: Optional[Predicate] = None, events: Optional[List[_Event]] = None,
                     **kwargs) -> _Catalogue:
    """Create a new catalogue in the database with optional events.

    Parameters
    ----------
    name: str
        The name of the catalogue.
    author: str
        The author of the catalogue.
    uuid: str, optional
        A unique identifier for the catalogue. If not provided, a new UUID will be generated.
    tags: Iterable[str], optional
        A list of tags associated with the catalogue.
    predicate: Predicate, optional
        A predicate to filter events in the catalogue. If not provided, the catalogue is static.
    events: List[_Event], optional
        A list of events to be added to the catalogue upon creation.
    kwargs: dict, optional
        Additional attributes for the catalogue. Keys must be valid Python identifiers.
    Returns
    -------
    _Catalogue
        The created catalogue object.
    Raises
    ------
    ValueError
        If the name is empty, or if any tag contains a comma or is not a string.
        If any key in kwargs is not a valid Python identifier.

    See also
    --------
    create_event: For creating events that can be added to the catalogue.
    """
    with Session() as s:
        c = s.create_catalogue(
            name=name,
            author=author,
            uuid=uuid,
            tags=tags or [],
            predicate=predicate,
            **kwargs)
        if events:
            s.add_events_to_catalogue(c, events)
        return c


def add_events_to_catalogue(catalogue: Union[_Catalogue, str], events: Union[_Event, List[_Event]]) -> None:
    """Add events to an existing catalogue.
    Parameters
    ----------
    catalogue: _Catalogue or str
        The catalogue to which events will be added. If a string is provided, it is treated as the catalogue UUID.
    events: Union[_Event, List[_Event]]
        A single event or a list of events to be added to the catalogue.

    See also
    --------
    remove_events_from_catalogue: For removing events from a catalogue.
    """

    with Session() as s:
        if isinstance(catalogue, str):
            catalogue = next(filter(lambda c: c.uuid == catalogue, get_catalogues()))
        s.add_events_to_catalogue(catalogue, events)


def remove_events_from_catalogue(catalogue: Union[_Catalogue, str], events: Union[_Event, List[_Event]]) -> None:
    """Remove events from an existing catalogue.
    Parameters
    ----------
    catalogue: _Catalogue or str
        The catalogue from which events will be removed. If a string is provided, it is treated as the catalogue UUID.
    events: Union[_Event, List[_Event]]
        A single event or a list of events to be removed from the catalogue.

    See also
    --------
    add_events_to_catalogue: For adding events to a catalogue.
    """
    with Session() as s:
        if isinstance(catalogue, str):
            c = get_catalogue(uuid=catalogue)
            if not c:
                raise ValueError(f"Catalogue with UUID '{catalogue}' not found.")
            catalogue = c
        s.remove_events_from_catalogue(catalogue, events)


def get_catalogue(uuid: Optional[str] = None, name: Optional[str] = None, predicate: Optional[Predicate] = None) -> \
    Optional[_Catalogue]:
    """Get a catalogue by its UUID or name or using a predicate. If more than one catalogue matches the criteria, only the first one is returned.

    Parameters
    ----------
    uuid: str, optional
        The UUID of the catalogue to retrieve.
    name: str, optional
        The name of the catalogue to retrieve.
    predicate: Predicate, optional
        A predicate to filter catalogues. If provided, it overrides the uuid and name parameters.

    Raises
    ------
    ValueError
        If more than one of uuid, name, or predicate is provided.

    Returns
    -------
    _Catalogue or None
        The catalogue object if found, otherwise None.
    """
    if sum(v is not None for v in (uuid, name, predicate)) != 1:
        raise ValueError("Exactly one of uuid, name or predicate must be provided.")
    base_dict: Dict[str, Any] = {}
    if uuid:
        base_dict['uuid'] = uuid
    elif name:
        base_dict['name'] = name
    elif predicate:
        base_dict['predicate'] = predicate

    base_dict['removed'] = False  # only get non-removed catalogues

    cats = backend().get_catalogues(base_dict)
    if cats:
        return _Catalogue._from_db(cats[0], removed=False)
    return None


def get_catalogues(base: Union[Predicate, _Event, None] = None, removed_items: bool = False) -> List[_Catalogue]:
    base_dict: Dict[str, Union[Predicate, 'Event', None, bool]]
    """Get all catalogues from the database.
    If base is a Predicate, all catalogues matching the predicate are returned.
    If base is an Event, all catalogues containing the event are returned.
    If base is None, all catalogues are returned.
    If removed_items is True, also removed catalogues are returned.

    Parameters
    ----------
    base: Predicate, Event or None
        The base for the query (see above)
    removed_items: bool
        If True, also removed catalogues are returned.
    Returns
    -------
    List[_Catalogue]
        A list of catalogue objects.
    """

    if isinstance(base, Predicate):
        base_dict = {'predicate': base}
    elif isinstance(base, _Event):
        base_dict = {'entity': base._backend_entity}
    else:
        base_dict = {}

    base_dict.update({'removed': removed_items})

    return [_Catalogue._from_db(cat, removed=removed_items)
            for cat in backend().get_catalogues(base_dict)]


def existing_tags() -> Set[str]:
    """Get all existing tags from both events and catalogues."""
    return backend().get_existing_tags()


def _get_events_from_predicate_or_none(base: Union[Predicate, None], removed_items: bool) -> List[_Event]:
    base_dict: Dict = {'removed': removed_items}
    if isinstance(base, Predicate):
        base_dict.update({'predicate': base})
    return [_Event._from_row(row, removed=removed_items) for row in backend().get_events_raw(base_dict)]


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
    for e, is_assigned in backend().get_events(base_dict):
        events.append(_Event._from_db(e, removed=removed_items))
        query_info.append(EventQueryInformation(assigned=is_assigned))

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
