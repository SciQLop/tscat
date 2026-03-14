from . import orm

from ..filtering import Predicate, Comparison, Field, Attribute, All, Any, Match, Has, Not, In, InCatalogue, \
    PredicateRecursionError, CatalogueFilterError

import datetime as dt
import os
from shutil import copyfile
from tempfile import mkdtemp
import orjson
from appdirs import user_data_dir

from typing import Union, List, Dict, Type, Set
from typing_extensions import Literal

from sqlalchemy import create_engine, and_, or_, not_, event, func, select
from sqlalchemy.orm import Session, sessionmaker

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
        return and_(self.visit_predicate(pred) for pred in all_._predicates) # type: ignore

    def _visit_any(self, any_: Any):
        return or_(self.visit_predicate(pred) for pred in any_._predicates) # type: ignore

    def _visit_not(self, not__: Not):
        return not_(self.visit_predicate(not__._operand))

    def _visit_in(self, in_: In):
        if isinstance(in_._rhs, Field):
            col = getattr(self._orm_class, in_._rhs.value)
            return func.json_array_contains(col, in_._lhs) == True  # noqa: E712
        elif isinstance(in_._rhs, Attribute):
            return func.json_array_contains(
                self._orm_class.attributes[in_._rhs.value], in_._lhs
            ) == True  # noqa: E712

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
    def __init__(self, testing: Union[bool, str] = False):
        in_memory = False
        if testing is True:
            sqlite_filename = ""
            in_memory = True
        elif isinstance(testing, str):
            sqlite_filename = self._copy_to_tmp(testing)
        else:  # pragma: no cover
            db_file_path = user_data_dir('tscat')
            if not os.path.exists(db_file_path):
                os.makedirs(db_file_path)
            sqlite_filename = f'{db_file_path}/backend.sqlite'

        # self.engine = create_engine(url, echo=True,
        self.engine = create_engine(f'sqlite:///{sqlite_filename}',
                                    json_serializer=_serialize_json,
                                    json_deserializer=_deserialize_json)

        @event.listens_for(self.engine, "connect")
        def _register_sqlite_functions(dbapi_conn, connection_record):
            def _json_array_contains(json_str, value):
                try:
                    arr = orjson.loads(json_str) if isinstance(json_str, str) else json_str
                    return value in arr
                except (orjson.JSONDecodeError, TypeError):
                    return False
            dbapi_conn.create_function("json_array_contains", 2, _json_array_contains)

        # use BEGIN EXCLUSIVE to lock database exclusively to one process
        @event.listens_for(self.engine, "begin")
        def do_begin(conn):
            conn.exec_driver_sql("BEGIN EXCLUSIVE")

        if in_memory:
            import sqlite3
            source = sqlite3.connect("")
            raw_conn = self.engine.raw_connection()
            source.backup(raw_conn.driver_connection, pages=-1)  # type: ignore[arg-type]

        from alembic.config import Config
        from alembic import command

        alembic_cfg = Config(os.path.join(os.path.dirname(__file__), 'alembic.ini'))
        alembic_cfg.set_main_option("script_location", os.path.join(os.path.dirname(__file__), 'migrations'))
        alembic_cfg.set_main_option("sqlalchemy.url", f'sqlite:///{sqlite_filename}')

        if not in_memory:
            from alembic.script import ScriptDirectory
            from alembic.migration import MigrationContext
            script = ScriptDirectory.from_config(alembic_cfg)
            head = script.get_current_head()
            with self.engine.connect() as conn:
                current = MigrationContext.configure(conn).get_current_revision()
            if current != head:
                command.upgrade(alembic_cfg, "head")
        else:
            command.upgrade(alembic_cfg, "head")

        orm.Base.metadata.create_all(self.engine)

        self._session_factory = sessionmaker(bind=self.engine, autoflush=False)
        self.session = self._session_factory()

    def _copy_to_tmp(self, source_file) -> str:
        # temp dir lives as long as the object
        self._tmp_dir = mkdtemp()
        destination_file = os.path.join(self._tmp_dir, os.path.basename(source_file))
        copyfile(source_file, destination_file)
        return destination_file

    def close(self):
        self.session.close()
        self.engine.dispose()

    def add_catalogue(self, catalogue: Dict) -> orm.Catalogue:
        serialized_predicate = catalogue['predicate'].to_dict() \
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
                         event['rating'],
                         event['attributes'])

    def add_events_to_catalogue(self, catalogue: orm.Catalogue, events: List[orm.Event]) -> None:
        existing_ids = {e.id for e in catalogue.events}
        for e in events:
            if e.id in existing_ids:
                raise ValueError('Event is already in catalogue.')
        catalogue.events.extend(events)

    def remove_events_from_catalogue(self, catalogue: orm.Catalogue, events: List[orm.Event]) -> None:
        existing = {id(e) for e in catalogue.events}
        for e in events:
            if id(e) not in existing:
                raise ValueError('Event is not in catalogue.')
        to_remove = {id(e) for e in events}
        catalogue.events = [e for e in catalogue.events if id(e) not in to_remove]

    def update_field(self, entity: Union[orm.Event, orm.Catalogue], key: str, value) -> None:
        if key == 'predicate' and value is not None:
            value = value.to_dict()
        setattr(entity, key, value)

    @staticmethod
    def update_attribute(entity: Union[orm.Event, orm.Catalogue], key: str, value) -> None:
        if entity.attributes is None:
            entity.attributes = {}
        entity.attributes[key] = value

    @staticmethod
    def delete_attribute(entity: Union[orm.Event, orm.Catalogue], key: str) -> None:
        if entity.attributes is not None:
            del entity.attributes[key]

    def _create_query(self, base: Dict,
                      orm_class: Union[Type[orm.Event], Type[orm.Catalogue]],
                      field: Union[Literal['events'], Literal['catalogues']],
                      removed: bool = False):
        f = None

        if base.get('predicate', None) is not None:
            f = PredicateVisitor(orm_class).visit_predicate(base['predicate'])

        entity_filter = None
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

        if entity_filter is not None:
            stmt = select(orm_class, entity_filter).filter(f)
        else:
            stmt = select(orm_class).filter(f)
        return self.session.execute(stmt), entity_filter is not None

    def get_catalogues(self, base: Dict = {}) -> List[orm.Catalogue]:
        self.session.flush()
        rows, has_filter = self._create_query(base, orm.Catalogue, 'events', removed=base['removed'])
        return [row[0] for row in rows]

    def get_events(self, base: Dict = {}) -> List[tuple]:
        self.session.flush()
        rows, has_filter = self._create_query(base, orm.Event, 'catalogues', removed=base['removed'])
        if has_filter:
            return [(row[0], bool(row[1])) for row in rows]
        return [(row[0], False) for row in rows]

    def get_events_raw(self, base: Dict) -> list:
        """Fast path: returns raw tuples (id, start, stop, author, uuid, tags, products, rating, attributes).
        Bypasses ORM hydration for bulk reads."""
        self.session.flush()

        events_t = orm.Event.__table__
        cols = [events_t.c.id, events_t.c.start, events_t.c.stop, events_t.c.author,
                events_t.c.uuid, events_t.c.tags, events_t.c.products, events_t.c.rating,
                events_t.c.attributes]

        removed = base.get('removed', False)
        predicate = base.get('predicate', None)

        filters = [events_t.c.removed == removed]
        if predicate is not None:
            filters.append(PredicateVisitor(orm.Event).visit_predicate(predicate))

        stmt = select(*cols).where(and_(*filters))
        return list(self.session.connection().execute(stmt).fetchall())

    def get_events_by_uuid_list(self, uuids: List[str]) -> Dict[str, orm.Event]:
        self.session.flush()
        # SQLite limits bind parameters per query (~999), so chunk the list
        result: Dict[str, orm.Event] = {}
        chunk_size = 900
        for i in range(0, len(uuids), chunk_size):
            chunk = uuids[i:i + chunk_size]
            stmt = select(orm.Event).filter(orm.Event.uuid.in_(chunk))
            result.update({e.uuid: e for e in self.session.scalars(stmt).all()})
        return result

    def get_existing_tags(self) -> Set[str]:
        self.session.flush()
        import sqlalchemy as sa
        result = set()
        for (tag,) in self.session.execute(
            sa.text("SELECT DISTINCT j.value FROM events, json_each(events.tags) AS j")
        ).fetchall():
            result.add(tag)
        for (tag,) in self.session.execute(
            sa.text("SELECT DISTINCT j.value FROM catalogues, json_each(catalogues.tags) AS j")
        ).fetchall():
            result.add(tag)
        return result

    def add_and_flush(self, entity_list: List[Union[orm.Event, orm.Catalogue]]):
        self.session.add_all(entity_list)
        self.session.flush()

    def commit(self):
        self.session.commit()

    def rollback(self):
        self.session.rollback()
        self.session.expire_all()

    def has_unsaved_changes(self) -> bool:
        return self.session.in_transaction()

    def remove(self, entity: Union[orm.Catalogue, orm.Event], permanently: bool = False) -> None:
        if permanently:
            self.session.delete(entity)
            self.session.flush()
        else:
            entity.removed = True

    @staticmethod
    def restore(entity: Union[orm.Catalogue, orm.Event]) -> None:
        entity.removed = False
