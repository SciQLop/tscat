from . import orm

from ..filtering import Predicate, Comparison, Field, Attribute, All, Any, Match, Has, Not, In

import pickle
import datetime as dt
import os
from appdirs import user_data_dir

from typing import Union, List, Dict, TypeVar
from typeguard import typechecked

from sqlalchemy import create_engine, and_, or_, not_, event
from sqlalchemy.orm import Session, Query

from operator import __eq__, __ne__, __ge__, __gt__, __le__, __lt__


@typechecked
class PredicateVisitor:
    def __init__(self, orm_class: Union[TypeVar(orm.Event), TypeVar(orm.Catalogue)]):
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
            return self._orm_class.attributes.any(
                and_(self._orm_class._attribute_class.key == comp._lhs.value,
                     op_map[comp._op](self._orm_class._attribute_class.value, rhs)))

    def _visit_all(self, all_: All):
        return and_(self.visit_predicate(pred) for pred in all_._predicates)

    def _visit_any(self, any_: Any):
        return or_(self.visit_predicate(pred) for pred in any_._predicates)

    def _visit_not(self, not__: Not):
        return not_(self.visit_predicate(not__._operand))

    def _visit_in(self, in_: In):
        if isinstance(in_._rhs, Field):
            rhs = getattr(self._orm_class, in_._rhs.value)
            return rhs.any(name=in_._lhs)  # name and product's field are called `name`

        elif isinstance(in_._rhs, Attribute):
            return self._orm_class.attributes.any(
                and_(self._orm_class._attribute_class.key == in_._rhs.value,
                     self._orm_class._attribute_class.value.contains([in_._lhs]))
            )

    def _visit_has(self, has_: Has):
        return self._orm_class.attributes.any(
            and_(self._orm_class._attribute_class.key == has_._operand.value,
                 self._orm_class._attribute_class.value is not None))

    def _visit_match(self, match_: Match):
        if isinstance(match_._lhs, Field):
            lhs = getattr(self._orm_class, match_._lhs.value)
            return lhs.regexp_match(match_._rhs)

        elif isinstance(match_._lhs, Attribute):
            return self._orm_class.attributes.any(
                and_(self._orm_class._attribute_class.key == match_._lhs.value,
                     self._orm_class._attribute_class.value.regexp_match(match_._rhs)))

    def visit_predicate(self, pred: Predicate):
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


@typechecked
class Backend:
    def __init__(self, testing=False):
        if testing:
            url = 'sqlite://'  # memory database
        else:  # pragma: no cover
            db_file_path = user_data_dir('tscat')
            if not os.path.exists(db_file_path):
                os.makedirs(db_file_path)
            url = f'sqlite:///{db_file_path}/backend.sqlite'

        # self.engine = create_engine(url, echo=True)
        self.engine = create_engine(url)

        # use BEGIN EXCLUSIVE to lock database exclusively to one process
        @event.listens_for(self.engine, "begin")
        def do_begin(conn):
            conn.exec_driver_sql("BEGIN EXCLUSIVE")

        orm.Base.metadata.create_all(self.engine)

        self.session = Session(bind=self.engine, autoflush=True)

    def _get_or_create(self, model, **kwargs):
        instance = self.session.query(model).filter_by(**kwargs).one_or_none()
        if instance:
            return instance
        else:
            instance = model(**kwargs)
            self.session.add(instance)
            return instance

    def add_or_update_catalogue(self, catalogue: Dict) -> orm.Catalogue:
        entity = catalogue.get("_entity", None)

        serialized_predicate = pickle.dumps(catalogue['predicate'], protocol=3) \
            if catalogue['predicate'] is not None else None

        tags = [self._get_or_create(orm.Tag, name=tag) for tag in catalogue['tags']]
        if entity:  # update
            entity.name = catalogue['name']
            entity.author = catalogue['author']
            entity.uuid = catalogue['uuid']
            entity.tags = tags
            entity.predicate = serialized_predicate
        else:
            entity = orm.Catalogue(catalogue['name'], catalogue['author'], catalogue['uuid'],
                                   tags, serialized_predicate)

        # need to use []-operator because of proxy-class in sqlalchemy - update() on __dict__ does not work
        for k, v in catalogue['attributes'].items():
            entity[k] = v

        self.session.add(entity)
        self.session.flush()

        return entity

    def add_or_update_event(self, event: Dict) -> orm.Event:
        entity = event.get("_entity", None)

        tags = [self._get_or_create(orm.Tag, name=tag) for tag in event['tags']]
        products = [self._get_or_create(orm.EventProduct, name=product) for product in event['products']]
        if entity:  # update
            entity.start = event['start']
            entity.stop = event['stop']
            entity.author = event['author']
            entity.tags = tags
            entity.products = products
            entity.uuid = event['uuid']
        else:  # insert
            entity = orm.Event(event['start'], event['stop'], event['author'], event['uuid'], tags, products)

        # need to use []-operator because of proxy-class in sqlalchemy - update() on __dict__ does not work
        for k, v in event['attributes'].items():
            entity[k] = v

        self.session.add(entity)
        self.session.flush()

        return entity

    def add_events_to_catalogue(self, catalogue: orm.Catalogue, events: List[orm.Event]) -> None:
        for e in events:
            if e in catalogue.events:
                raise ValueError('Event is already in catalogue.')
            catalogue.events.append(e)
        self.session.flush()  # need flush - autoflush seems not work for n-to-m-relations

    def remove_events_from_catalogue(self, catalogue: orm.Catalogue, events: List[orm.Event]) -> None:
        for e in events:
            catalogue.events.remove(e)
        self.session.flush()

    def _create_query(self, base: Dict,
                      orm_class: Union[TypeVar(orm.Event), TypeVar(orm.Catalogue)],
                      field: ['events', 'catalogues'],
                      removed: bool = False) -> Query:
        f = None

        if base.get('predicate', None) is not None:
            f = PredicateVisitor(orm_class).visit_predicate(base['predicate'])

        if base.get('entity') is not None:  # event
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
            attr = {v.key: v.value for _, v in c.attributes.items()}
            catalogue = {"name": c.name,
                         "author": c.author,
                         "uuid": c.uuid,
                         "tags": [tag.name for tag in c.tags],
                         "predicate": pickle.loads(c.predicate) if c.predicate else None,
                         "attributes": attr,
                         "entity": c}
            catalogues.append(catalogue)

        return catalogues

    def get_events(self, base: Dict = {}) -> List[Dict]:
        events = []
        for e in self._create_query(base, orm.Event, 'catalogues', removed=base['removed']):
            attr = {v.key: v.value for _, v in e.attributes.items()}
            event = {
                "start": e.start,
                "stop": e.stop,
                "author": e.author,
                "uuid": e.uuid,
                "tags": [tag.name for tag in e.tags],
                "products": [product.name for product in e.products],
                "attributes": attr,
                "entity": e}
            events.append(event)

        return events

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
