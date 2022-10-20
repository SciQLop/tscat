from sqlalchemy import Column, Integer, DateTime, ForeignKey, UnicodeText, Boolean, Table, String, \
    LargeBinary, Index, JSON
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.ext.mutable import MutableDict
from sqlalchemy.orm import relationship
from sqlalchemy_utils import ScalarListType  # type: ignore

from typing import List, Dict, Any

Base = declarative_base()

event_in_catalogue_association_table = \
    Table('event_in_catalogue', Base.metadata,
          Column('event_id', Integer, ForeignKey('events.id')),
          Column('catalogue_id', Integer, ForeignKey('catalogues.id')))

event_in_catalogue_association_index = Index(
    'e_in_c_index',
    event_in_catalogue_association_table.c.event_id,
    event_in_catalogue_association_table.c.catalogue_id,
    unique=True)


class Event(Base):
    __tablename__ = 'events'

    id = Column(Integer, primary_key=True, autoincrement=True)

    uuid = Column(String(36), index=True, nullable=False, unique=True)

    start = Column(DateTime, nullable=False)
    stop = Column(DateTime, nullable=False)
    author = Column(UnicodeText, nullable=False)

    tags: List[str] = Column(ScalarListType(str), default=[], info={"type": (list, "string_list")})
    products: List[str] = Column(ScalarListType(str), default=[], info={"type": (list, "string_list")})

    removed: bool = Column(Boolean, default=False, nullable=False)

    attributes: Dict[str, Any] = Column(MutableDict.as_mutable(JSON))

    def __init__(self, start, stop, author, uuid, tags, products, attributes):
        self.start = start
        self.stop = stop
        self.author = author
        self.uuid = uuid
        self.tags = tags
        self.products = products
        self.attributes = attributes

    def __repr__(self):  # pragma: no cover
        return f'Event({self.id}: {self.start}, {self.stop}, {self.author}), {self.removed}, meta=' + self._proxied.__repr__()


class Catalogue(Base):
    __tablename__ = 'catalogues'

    id = Column(Integer, primary_key=True, autoincrement=True)

    uuid = Column(String(36), index=True, nullable=False, unique=True)

    name = Column(UnicodeText, nullable=False)
    author = Column(UnicodeText, nullable=False)
    predicate = Column(LargeBinary, nullable=True)

    tags: List[str] = Column(ScalarListType(str), default=[], info={"type": (list, "string_list")})

    removed: bool = Column(Boolean, default=False, nullable=False)

    attributes: Dict[str, Any] = Column(MutableDict.as_mutable(JSON))

    events: List[Event] = relationship("Event",
                                       backref="catalogues",
                                       secondary=event_in_catalogue_association_table)

    def __init__(self, name: str, author: str, uuid: str, tags: List[str], predicate: bytes, attributes: Dict):
        self.name = name
        self.author = author
        self.uuid = uuid
        self.tags = tags
        self.predicate = predicate
        self.attributes = attributes

    def __repr__(self):  # pragma: no cover
        return f'Catalogue({self.id}: {self.name}, {self.author}, {self.removed}), attrs=' + self._proxied.__repr__()
