"""Top-level package for Time Series Catalogues."""

__author__ = """Patrick Boettcher"""
__email__ = 'p@yai.se'
__version__ = '0.2.0'

from dataclasses import dataclass, field
import itertools
import json
import os
import re
from typing import Dict, List, Union, Tuple, Iterable, Any, Optional, TYPE_CHECKING, Type, Callable
from uuid import uuid4, UUID

from .filtering import Predicate, UUID as UUIDFilter
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
                 uuid: Optional[str] = None,
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
            if any(',' in v for v in value):
                raise ValueError("a string-list value shall not contain a comma")

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
            if any(type(v) != str for v in value):
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
        catalogues += [c]
    return catalogues


def get_events(base: Union[Predicate, _Catalogue, None] = None,
               removed_items: bool = False,
               assigned_only: bool = False) -> List[_Event]:
    base_dict: Dict[str, Union[Predicate, 'Catalogue', None, bool]]
    if isinstance(base, Predicate):
        base_dict = {'predicate': base}
    elif isinstance(base, _Catalogue):
        base_dict = {'entity': base._backend_entity}
        if not assigned_only:
            base_dict['predicate'] = base.predicate
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


### Generic Import/Export

@dataclass
class __CanonicalizedTSCatData:
    '''
    A class representing the serialization of catalogues and theirs events. Used for exporting and importing
    Basically the structure is two lists of catalogues and events dumped to their dict-form.

    Additionally, catalogues have an element 'events' which is a string-list of uuid of events associated to
    them.
    '''
    catalogues: List[Dict[str, Any]] = field(default_factory=list)  # list of dumped catalogues
    events: Dict[str, Dict[str, Any]] = field(default_factory=dict)  # dict of str(UUID): dumped event

    class DumpCatalogue:
        '''
        Session class for preparing the export of catalogues and events.
        '''

        def __init__(self, catalogue: _Catalogue, data: '__CanonicalizedTSCatData') -> None:
            self._data = data
            self._cat_dump = catalogue.dump()
            self._events_uuids: List[str] = []

        def __enter__(self) -> '__CanonicalizedTSCatData.DumpCatalogue':
            return self

        def __exit__(self, exc_type, exc_value, exc_tb) -> None:
            self._cat_dump.update({"events": self._events_uuids})
            self._data.catalogues.append(self._cat_dump)

        def add_events(self, events: List[_Event]) -> None:
            for event in events:
                self._events_uuids.append(event.uuid)
                self._data.events[event.uuid] = event.dump()

    def to_dict(self) -> Dict[str, List[Dict]]:
        return {
            'catalogues': self.catalogues,
            'events': [event for event in self.events.values()],
        }


def __canonicalize_from_dict(data: Dict[str, Any]) -> __CanonicalizedTSCatData:
    # check events and catalogues for existing entities
    # if existing and identical - remove from import-dict
    # if existing and not identical raise
    # if not existing import

    uuids = [event['uuid'] for event in data['events']]
    events = backend().get_events_by_uuid_list(uuids)

    for event in data['events'][:]:
        if event['uuid'] not in events:
            continue

        # to compare the existing event, the to-be-imported event is transformed to an in event from the backend
        check_event = events[event['uuid']]
        del check_event['entity']
        check_event.update(check_event['attributes'])
        del check_event['attributes']

        check_event['start'] = dt.datetime.isoformat(check_event['start'])
        check_event['stop'] = dt.datetime.isoformat(check_event['stop'])

        if check_event != event:
            raise ValueError(f'Import: event with UUID {event["uuid"]} already exists in database, ' +
                             'but with different values.')
        data['events'].remove(event)

    for catalogue in data['catalogues'][:]:
        check_catalogue = get_catalogues(UUIDFilter(catalogue['uuid']))
        if len(check_catalogue) != 0:
            events_uuids = [event.uuid for event in get_events(check_catalogue[0])]
            catalogue_dump = check_catalogue[0].dump()

            # convert the existing catalogue so that it can be compared with the to-be-imported one
            events_uuids.sort()
            catalogue_dump.update({'events': events_uuids})
            if catalogue_dump['predicate']:
                catalogue_dump['predicate'] = str(catalogue_dump['predicate'])

            catalogue['events'].sort()
            if catalogue_dump != catalogue:
                raise ValueError(f'Import: catalogue with UUID {catalogue["uuid"]} already exists in database, ' +
                                 'but with different values.')
            data['catalogues'].remove(catalogue)

    return __CanonicalizedTSCatData(data['catalogues'], {e['uuid']: e for e in data['events']})


def __import_canonicalized_dict(data: __CanonicalizedTSCatData) -> List[_Catalogue]:
    event_of_uuid = {}
    catalogues: List[_Catalogue] = []

    # import all new events
    with Session() as s:
        for event in data.events.values():
            event['start'] = dt.datetime.fromisoformat(event['start'])
            event['stop'] = dt.datetime.fromisoformat(event['stop'])
            event_of_uuid[event['uuid']] = s.create_event(**event)

        for catalogue_dict in data.catalogues:
            catalogue_events = [event_of_uuid[uuid] if uuid in event_of_uuid
                                else get_events(UUIDFilter(uuid))[0]
                                for uuid in catalogue_dict['events']]
            del catalogue_dict['events']

            catalogue = s.create_catalogue(**catalogue_dict)
            s.add_events_to_catalogue(catalogue, catalogue_events)
            catalogues.append(catalogue)

    return catalogues


### JSON
class __LocalEncoder(json.JSONEncoder):
    def default(self, obj):
        if isinstance(obj, dt.datetime):
            return dt.datetime.isoformat(obj)
        else:
            return str(obj)

        return json.JSONEncoder.default(self, obj)


def export_json(catalogues: Union[List[_Catalogue], _Catalogue]) -> str:
    data = __CanonicalizedTSCatData()

    for catalogue in _listify(catalogues):
        with __CanonicalizedTSCatData.DumpCatalogue(catalogue, data) as catalogue_data:
            catalogue_data.add_events(get_events(catalogue))

    return json.dumps(data.to_dict(), cls=__LocalEncoder)


def __canonicalize_json_import(jsons: str) -> __CanonicalizedTSCatData:
    import_dict = json.loads(jsons)
    return __canonicalize_from_dict(import_dict)


def import_json(jsons: str) -> List[_Catalogue]:
    import_dict = __canonicalize_json_import(jsons)
    return __import_canonicalized_dict(import_dict)


### VOTable (AMDA compatible)

from astropy.io.votable import parse
from astropy.io.votable.tree import VOTableFile, Resource, Table, \
    Field as VOField


@dataclass
class __VOTableTSCatField:
    python_type: Type
    attr: Dict[str, str]
    convert_vot: Callable[[Any], Any]
    convert_tscat: Callable[[Any], Any]
    tscat_name: Optional[str] = None

    def name(self, field: VOField) -> str:
        if self.tscat_name:
            return self.tscat_name
        return field.name

    def match(self, field: VOField) -> bool:
        for k, v in self.attr.items():
            if field.__getattribute__(k) != v:
                return False

        return True

    def make_vot_field(self, table: Table, name: str) -> VOField:
        if 'name' not in self.attr:
            return VOField(table, name=name, **self.attr)
        else:
            return VOField(table, **self.attr)


class __VOTableTSCatFieldSpecialDateTime(__VOTableTSCatField):
    def __init__(self, attrs: Dict[str, str], tscat_name: Optional[str] = None) -> None:
        attrs.update({'datatype': "char", 'xtype': "dateTime", 'utype': "", 'arraysize': "*"})
        super().__init__(dt.datetime, attrs, dt.datetime.isoformat, str, tscat_name)


votable_tscat_fields = [
    __VOTableTSCatFieldSpecialDateTime({'name': "Start Time", 'ID': "TimeIntervalStart", 'ucd': "time.start"}, 'start'),
    __VOTableTSCatFieldSpecialDateTime({'name': "Stop Time", 'ID': "TimeIntervalStop", 'ucd': "time.end"}, 'stop'),
    __VOTableTSCatFieldSpecialDateTime({}),
    __VOTableTSCatField(int, {'datatype': 'long'}, int, int),
    __VOTableTSCatField(float, {'datatype': 'double'}, float, float),
    __VOTableTSCatField(bool, {'datatype': 'boolean'}, bool, bool),
    __VOTableTSCatField(list, {'datatype': "char", 'arraysize': "*", 'utype': 'json'}, json.dumps, json.loads),
    __VOTableTSCatField(list, {'datatype': "char", 'arraysize': "*", 'name': 'products'}, json.dumps, json.loads,
                        'products'),
    __VOTableTSCatField(list, {'datatype': "char", 'arraysize': "*", 'name': 'tags'}, json.dumps, json.loads, 'tags'),
    __VOTableTSCatField(str, {'datatype': "char", 'arraysize': "*"}, str, str),  # last item, catch all strings
]


def __vo_table_field_from(arg: Union[Type, str]) -> __VOTableTSCatField:
    vtf: Optional[__VOTableTSCatField] = None
    if isinstance(arg, str):
        for vtf in votable_tscat_fields:
            if vtf.tscat_name == arg:
                break
    else:
        for vtf in votable_tscat_fields:
            if vtf.tscat_name is None and vtf.python_type == arg:
                break

    assert vtf is not None

    return vtf


def export_votable(catalogues: Union[List[_Catalogue], _Catalogue]) -> VOTableFile:
    votable = VOTableFile()

    resource = Resource()
    votable.resources.append(resource)

    for catalogue in _listify(catalogues):
        table = Table(votable, name=catalogue.name.replace(' ', '_'))
        resource.tables.append(table)

        # define standard fields
        attributes = [
            ('start', __vo_table_field_from('start')),
            ('stop', __vo_table_field_from('stop')),
            ('author', __vo_table_field_from(str)),
            ('uuid', __vo_table_field_from(str)),
            ('tags', __vo_table_field_from(list)),
            ('products', __vo_table_field_from(list)),
        ]

        events = get_events(catalogue)
        # set of all attributes of any event
        var_attrs = set(itertools.chain.from_iterable(event.variable_attributes().keys() for event in events))
        # set of all attributes of all events
        var_attrs_intersect = set.intersection(*[set(event.variable_attributes().keys()) for event in events])

        # for the moment raise an error if there are attributes not present in every event
        if var_attrs != var_attrs_intersect:
            raise ValueError('Export VOTable: not all attributes are present in all events (' +
                             str(var_attrs - var_attrs_intersect))

        for attr in sorted(var_attrs):
            # now check that the value-type of all values of the to be exported attributes is identical
            attrs_value_types = list(set(type(event.__dict__[attr]) for event in events))
            if len(attrs_value_types) != 1:
                raise ValueError('Export: VOTable: not all value-types are ' +
                                 f'identical for all events for attribute {attr}')

            attributes.append((attr, __vo_table_field_from(attrs_value_types[0])))

        table.fields.extend([vtf.make_vot_field(votable, name) for name, vtf in attributes])

        table.create_arrays(len(events))
        for i, event in enumerate(events):
            c = tuple(vtf.convert_vot(event.__dict__[k]) if k in event.__dict__ else '' for k, vtf in attributes)
            table.array[i] = c

    return votable


def import_votable(filename: str) -> List[_Catalogue]:
    votable = parse(filename)

    name = os.path.basename(filename)

    ddict: Dict[str, List[Dict[str, Any]]] = {
        'catalogues': [],
        'events': [],
    }

    for i, table in enumerate(votable.iter_tables()):
        required_field_names: List[str] = ['Start Time', 'Stop Time']
        fields_vs_index: Dict[Tuple[int, str], __VOTableTSCatField] = {}

        for j, field in enumerate(table.fields):
            if field.name in required_field_names:
                required_field_names.remove(field.name)

            # match field-event-signature to get converters/name
            for vtf in votable_tscat_fields:
                if vtf.match(field):
                    fields_vs_index[(j, vtf.name(field))] = vtf
                    break
            else:
                raise ValueError(
                    f'VOTable import: cannot import field: {field.ID}, {field.name}, {field.datatype},' +
                    f'{field.xtype}')

        if len(required_field_names) > 0:
            raise ValueError(f'VOTable import: required fields are missing for table {name}_{i}')

        catalogue = {'name': f'{name}_{i} - imported',
                     'author': 'VOTable Import',
                     'events': [],
                     'uuid': str(uuid4())}

        has_author_field = any(f[1] == 'author' for f in fields_vs_index.keys())
        has_uuid_field = any(f[1] == 'uuid' for f in fields_vs_index.keys())

        for l in table.array:
            event = {}
            if not has_author_field:
                event['author'] = 'VOTImport'

            if not has_uuid_field:
                event['uuid'] = str(uuid4())

            for (index, name), vtf in fields_vs_index.items():
                event[name] = vtf.convert_tscat(l[index])

            if not any(event['uuid'] == e['uuid'] for e in ddict['events']):
                ddict['events'].append(event)

            catalogue['events'].append(event['uuid'])  # type: ignore

        ddict['catalogues'].append(catalogue)

    data = __canonicalize_from_dict(ddict)
    return __import_canonicalized_dict(data)
