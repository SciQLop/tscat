from . import orm

from ..filtering import Predicate, Comparison, Field, Attribute, All, Any, Match, Has, Not, In, InCatalogue, \
    PredicateRecursionError, CatalogueFilterError

import pickle
import datetime as dt
import os
import orjson
from appdirs import user_data_dir

from typing import Union, List, Dict, Type, Set
from typing_extensions import Literal

from sqlalchemy import create_engine, and_, or_, not_, event, func, cast, String
from sqlalchemy.orm import Session, Query

from operator import __eq__, __ne__, __ge__, __gt__, __le__, __lt__


def _serialize_json(obj):
    return orjson.dumps(obj, option=orjson.OPT_NAIVE_UTC).decode('utf-8')


def _deserialize_json(obj):
    return orjson.loads(obj)


class PredicateVisitor:
    def __init__(self, orm_class: Union[Type[orm.Event], Type[orm.Catalogue]]):
        self.visited_predicates: Set[int] = set()
        self._orm_class = orm_class

    def _visit_literal(self, operand: Union[str, int, bool, float, dt.datetime]):
        return operand

    def _visit_comparison(self, comp: Comparison):
        rhs = self._visit_literal(comp._rhs)

        op_map = {
            '==': __eq__,
            '!=': __ne__,
            '<': __lt__,
            '<=': __le__,
            '>': __gt__,
            '>=': __ge__,
        }
        if isinstance(comp._lhs, Field):
            lhs = getattr(self._orm_class, comp._lhs.value)
            return op_map[comp._op](lhs, rhs)

        elif isinstance(comp._lhs, Attribute):
            return and_(
                self._orm_class.attributes[comp._lhs.value] != 'null',
                op_map[comp._op](self._orm_class.attributes[comp._lhs.value], _serialize_json(rhs))
            )

    def _visit_all(self, all_: All):
        return and_(self.visit_predicate(pred) for pred in all_._predicates)

    def _visit_any(self, any_: Any):
        return or_(self.visit_predicate(pred) for pred in any_._predicates)

    def _visit_not(self, not__: Not):
        return not_(self.visit_predicate(not__._operand))

    def _visit_in(self, in_: In):  # cast lists and json to string and regex-match
        if isinstance(in_._rhs, Field):
            f = cast(getattr(self._orm_class, in_._rhs.value), String)
            v = in_._lhs
            # ugly, sqlite's Regex seems not to support (?:,|^)-regexes
            return or_(f == v,
                       f.regexp_match(f'^{v},'),
                       f.regexp_match(f',{v},'),
                       f.regexp_match(f',{v}$'))
        elif isinstance(in_._rhs, Attribute):
            return cast(self._orm_class.attributes[in_._rhs.value], String).regexp_match(f'"{in_._lhs}"')

    def _visit_in_catalogue(self, in_catalogue: InCatalogue):
        if self._orm_class == orm.Catalogue:
            raise CatalogueFilterError("Cannot filter catalogues with a in-catalogue-predicate.")

        if in_catalogue.catalogue is None:
            return ~getattr(self._orm_class, "catalogues").any()
        elif in_catalogue.catalogue.predicate is not None:
            return or_(getattr(self._orm_class, "catalogues").any(id=in_catalogue.catalogue._backend_entity.id),
                       self.visit_predicate(in_catalogue.catalogue.predicate))
        else:
            return getattr(self._orm_class, "catalogues").any(id=in_catalogue.catalogue._backend_entity.id)

    def _visit_has(self, has_: Has):
        return self._orm_class.attributes[has_._operand.value] != 'null'

    def _visit_match(self, match_: Match):
        if isinstance(match_._lhs, Field):
            lhs = getattr(self._orm_class, match_._lhs.value)
            return lhs.regexp_match(match_._rhs)

        elif isinstance(match_._lhs, Attribute):
            lhs = self._orm_class.attributes[match_._lhs.value]
            # SQLAlechmy always add JSON_QUOTE around JSON-fields, to regex-match we substr away the quotes
            return func.substr(lhs, 2, func.length(lhs) - 2).regexp_match(match_._rhs)

    def visit_predicate(self, pred: Predicate):
        if id(pred) in self.visited_predicates:
            raise PredicateRecursionError('Recursion detected in ', pred)

        self.visited_predicates.add(id(pred))

        if isinstance(pred, Comparison):
            return self._visit_comparison(pred)
        elif isinstance(pred, All):
            return self._visit_all(pred)
        elif isinstance(pred, Any):
            return self._visit_any(pred)
        elif isinstance(pred, Not):
            return self._visit_not(pred)
        elif isinstance(pred, Has):
            return self._visit_has(pred)
        elif isinstance(pred, Match):
            return self._visit_match(pred)
        elif isinstance(pred, In):
            return self._visit_in(pred)
        elif isinstance(pred, InCatalogue):
            return self._visit_in_catalogue(pred)


class Backend:
    def __init__(self, testing=False):
        if testing:
            url = 'sqlite://'  # memory database
        else:  # pragma: no cover
            db_file_path = user_data_dir('tscat')
            if not os.path.exists(db_file_path):
                os.makedirs(db_file_path)
            url = f'sqlite:///{db_file_path}/backend.sqlite'

        # self.engine = create_engine(url, echo=True,
        self.engine = create_engine(url,
                                    json_serializer=_serialize_json,
                                    json_deserializer=_deserialize_json)

        # use BEGIN EXCLUSIVE to lock database exclusively to one process
        @event.listens_for(self.engine, "begin")
        def do_begin(conn):
            conn.exec_driver_sql("BEGIN EXCLUSIVE")

        orm.Base.metadata.create_all(self.engine)

        self.session = Session(bind=self.engine, autoflush=True)

    def _specialiced_serialization(self, key, value):
        if key == "predicate":
            return pickle.dumps(value, protocol=3)

    def add_catalogue(self, catalogue: Dict) -> orm.Catalogue:
        serialized_predicate = self._specialiced_serialization('predicate', catalogue['predicate']) \
            if catalogue['predicate'] is not None else None

        entity = orm.Catalogue(catalogue['name'],
                               catalogue['author'],
                               catalogue['uuid'],
                               catalogue['tags'],
                               serialized_predicate,
                               catalogue['attributes'])

        return entity

    def add_event(self, event: Dict) -> orm.Event:
        return orm.Event(event['start'],
                         event['stop'],
                         event['author'],
                         event['uuid'],
                         event['tags'],
                         event['products'],
                         event['attributes'])

    def add_events_to_catalogue(self, catalogue: orm.Catalogue, events: List[orm.Event]) -> None:
        for e in events:
            if e in catalogue.events:
                raise ValueError('Event is already in catalogue.')
        catalogue.events.extend(events)

    def remove_events_from_catalogue(self, catalogue: orm.Catalogue, events: List[orm.Event]) -> None:
        for e in events:
            catalogue.events.remove(e)

    def update_field(self, entity: Union[orm.Event, orm.Catalogue], key: str, value) -> None:
        if key in ['predicate']:
            value = self._specialiced_serialization(key, value)
        setattr(entity, key, value)

    @staticmethod
    def update_attribute(entity: Union[orm.Event, orm.Catalogue], key: str, value) -> None:
        entity.attributes[key] = value

    @staticmethod
    def delete_attribute(entity: Union[orm.Event, orm.Catalogue], key: str) -> None:
        del entity.attributes[key]

    def _create_query(self, base: Dict,
                      orm_class: Union[Type[orm.Event], Type[orm.Catalogue]],
                      field: Union[Literal['events'], Literal['catalogues']],
                      removed: bool = False) -> Query:
        f = None

        if base.get('predicate', None) is not None:
            f = PredicateVisitor(orm_class).visit_predicate(base['predicate'])

        if base.get('entity') is not None:
            entity_filter = getattr(orm_class, field).any(id=base['entity'].id)
            if f is not None:
                f = or_(f, entity_filter)
            else:
                f = entity_filter

        if f is None:
            f = getattr(orm_class, 'removed') == removed
        else:
            f = and_(getattr(orm_class, 'removed') == removed, f)

        q = self.session.query(orm_class)
        if f is not None:
            q = q.filter(f)
        return q

    def get_catalogues(self, base: Dict = {}) -> List[Dict]:
        catalogues = []
        for c in self._create_query(base, orm.Catalogue, 'events', removed=base['removed']):
            catalogue = {"name": c.name,
                         "author": c.author,
                         "uuid": c.uuid,
                         "tags": c.tags,
                         "predicate": pickle.loads(c.predicate) if c.predicate else None,
                         "attributes": c.attributes,
                         "entity": c}
            catalogues.append(catalogue)

        return catalogues

    def get_events(self, base: Dict = {}) -> List[Dict]:
        events = []
        for e in self._create_query(base, orm.Event, 'catalogues', removed=base['removed']):
            event = {
                "start": e.start,
                "stop": e.stop,
                "author": e.author,
                "uuid": e.uuid,
                "tags": e.tags,
                "products": e.products,
                "attributes": e.attributes,
                "entity": e}
            events.append(event)

        return events

    def get_events_by_uuid_list(self, uuids: List[str]) -> Dict[str, Dict]:
        d = {}
        for e in self.session.query(orm.Event).filter(orm.Event.uuid.in_(uuids)).all():
            d[e.uuid] = {
                "start": e.start,
                "stop": e.stop,
                "author": e.author,
                "uuid": e.uuid,
                "tags": e.tags,
                "products": e.products,
                "attributes": e.attributes,
                "entity": e}

        return d

    def add_and_flush(self, entity_list: List[Union[orm.Event, orm.Catalogue]]):
        self.session.add_all(entity_list)
        self.session.flush()

    def commit(self):
        self.session.commit()

    def rollback(self):
        self.session.rollback()

    def has_unsaved_changes(self) -> bool:
        return self.session.in_transaction()

    def remove(self, entity: Union[orm.Catalogue, orm.Event], permanently: bool = False) -> None:
        if permanently:
            self.session.delete(entity)
        else:
            entity.removed = True

    @staticmethod
    def restore(entity: Union[orm.Catalogue, orm.Event]) -> None:
        entity.removed = False

    @staticmethod
    def is_removed(entity: Union[orm.Catalogue, orm.Event]) -> bool:
        return entity.removed
