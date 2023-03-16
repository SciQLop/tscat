from dataclasses import dataclass, field
import datetime as dt
import itertools
import json
import os
from typing import Dict, List, Union, Tuple, Any, Optional, Type, Callable
from uuid import uuid4

from .base import get_catalogues, get_events, _Catalogue, _Event, backend, Session, _listify
from .filtering import UUID


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
        check_catalogue = get_catalogues(UUID(catalogue['uuid']))
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
                                else get_events(UUID(uuid))[0]
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

    catalogues_list = _listify(catalogues)

    if len(catalogues_list) == 1:
        votable.description = f'Contact:{catalogues_list[0].author};Name:{catalogues_list[0].name};'

    resource = Resource()
    votable.resources.append(resource)

    for catalogue in catalogues_list:
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


def import_votable(filename: str, only_first_table: bool = True) -> List[_Catalogue]:
    votable = parse(filename)

    author = 'VOTable Import'
    name = os.path.basename(filename)

    if votable.description:
        for line in str(votable.description).split(';'):
            line = line.strip()

            values = line.split(':', 1)
            if len(values) != 2:
                continue
            property, value = values
            value = value.strip()
            if property == 'Contact':
                author = value
            elif property == 'Name':
                name = value

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

        if only_first_table or len(votable.iter_tables()) == 1:
            this_name = name
        else:
            this_name = f'{name}_{i}'

        catalogue = {'name': this_name,
                     'author': author,
                     'events': [],
                     'uuid': str(uuid4())}

        has_author_field = any(f[1] == 'author' for f in fields_vs_index.keys())
        has_uuid_field = any(f[1] == 'uuid' for f in fields_vs_index.keys())

        for l in table.array:
            event = {}
            if not has_author_field:
                event['author'] = author

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
