import datetime as dt

from sqlalchemy import ForeignKey, Table, Column, Integer, Index, String, JSON, func
from sqlalchemy.ext.mutable import MutableDict
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column, relationship

from typing import List, Dict, Any, Optional


class Base(DeclarativeBase):
    pass


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

    id: Mapped[int] = mapped_column(primary_key=True, autoincrement=True)

    uuid: Mapped[str] = mapped_column(String(36), index=True, unique=True)

    start: Mapped[dt.datetime] = mapped_column()
    stop: Mapped[dt.datetime] = mapped_column()
    author: Mapped[str] = mapped_column()

    tags: Mapped[Optional[List[str]]] = mapped_column(JSON, default=list)
    products: Mapped[Optional[List[str]]] = mapped_column(JSON, default=list)
    rating: Mapped[Optional[int]] = mapped_column(default=None)

    removed: Mapped[bool] = mapped_column(default=False)

    attributes: Mapped[Optional[Dict[str, Any]]] = mapped_column(MutableDict.as_mutable(JSON))

    def __init__(self, start, stop, author, uuid, tags, products, rating, attributes):
        self.start = start
        self.stop = stop
        self.author = author
        self.uuid = uuid
        self.tags = tags
        self.products = products
        self.rating = rating
        self.attributes = attributes

    def __repr__(self):  # pragma: no cover
        return f'Event({self.id}: {self.start}, {self.stop}, {self.author}), {self.removed}, meta=' + self.attributes.__repr__()


class Catalogue(Base):
    __tablename__ = 'catalogues'

    id: Mapped[int] = mapped_column(primary_key=True, autoincrement=True)

    uuid: Mapped[str] = mapped_column(String(36), index=True, unique=True)

    name: Mapped[str] = mapped_column()
    author: Mapped[str] = mapped_column()
    predicate: Mapped[Optional[dict]] = mapped_column(JSON, default=None)

    tags: Mapped[Optional[List[str]]] = mapped_column(JSON, default=list)

    removed: Mapped[bool] = mapped_column(default=False)

    attributes: Mapped[Optional[Dict[str, Any]]] = mapped_column(MutableDict.as_mutable(JSON))

    events: Mapped[List[Event]] = relationship("Event",
                                               backref="catalogues",
                                               secondary=event_in_catalogue_association_table)

    def __init__(self, name: str, author: str, uuid: str, tags: List[str], predicate: Optional[dict], attributes: Dict):
        self.name = name
        self.author = author
        self.uuid = uuid
        self.tags = tags
        self.predicate = predicate
        self.attributes = attributes

    def __repr__(self):  # pragma: no cover
        return f'Catalogue({self.id}: {self.name}, {self.author}, {self.removed}), attrs=' + self.attributes.__repr__()
