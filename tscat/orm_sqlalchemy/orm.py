from sqlalchemy import Column, Integer, DateTime, ForeignKey, Unicode, UnicodeText, Boolean, Table, String, \
    LargeBinary, Float, Index
from sqlalchemy import event, literal_column
from sqlalchemy.orm import declarative_base, relationship
from sqlalchemy.orm.collections import attribute_mapped_collection
from sqlalchemy.orm.interfaces import PropComparator
from sqlalchemy.ext.associationproxy import association_proxy
from sqlalchemy.ext.hybrid import hybrid_property
from sqlalchemy_utils import ScalarListType

from typing import List

import datetime as dt

import json

Base = declarative_base()


class ProxiedDictMixin(object):
    """Adds obj[key] access to a mapped class.

    This class basically proxies dictionary access to an attribute
    called ``_proxied``.  The class which inherits this class
    should have an attribute called ``_proxied`` which points to a dictionary.
    """

    def __len__(self):
        return len(self._proxied)

    def __iter__(self):
        return iter(self._proxied)

    def __getitem__(self, key):
        return self._proxied[key]

    def __contains__(self, key):
        return key in self._proxied

    def __setitem__(self, key, value):
        self._proxied[key] = value

    def __delitem__(self, key):
        del self._proxied[key]

    def __repr__(self):  # pragma: no cover
        return json.dumps({k: v for k, v in self.proxied.items() if not k.startswith('_')}, indent=1)


class PolymorphicVerticalProperty(object):
    """A key/value pair with polymorphic value storage.

    The class which is mapped should indicate typing information
    within the "info" dictionary of mapped Column objects; see
    the AnimalFact mapping below for an example.

    """

    def __init__(self, key, value=None):
        self.key = key
        self.value = value

    @hybrid_property
    def value(self):
        fieldname, discriminator = self.type_map[self.type]
        return getattr(self, fieldname)

    @value.setter
    def value(self, value):
        try:
            py_type = type(value)
            fieldname, discriminator = self.type_map[py_type]
            self.type = discriminator
            setattr(self, fieldname, value)
        except KeyError:
            raise TypeError(f'Unsupported type for SQLAlchemy hybrid-property used {type(self.type)}')

    # value deletion is done with del, not sure how this method can be called
    # @value.deleter
    # def value(self):
    #     self._set_value(None)

    @value.comparator
    class value(PropComparator):
        def __init__(self, cls):
            self.cls = cls

        def _fieldname(self, py_type):
            return self.cls.type_map[py_type][0]

        # TODO, see whether the type-name from type_map should be used for and and_-condition
        # TODO, check whether we need to cast?!

        def __eq__(self, other):
            fieldname = self._fieldname(type(other))
            return literal_column(fieldname) == other

        def __ne__(self, other):
            fieldname = self._fieldname(type(other))
            return literal_column(fieldname) != other

        def __lt__(self, other):
            fieldname = self._fieldname(type(other))
            return literal_column(fieldname) < other

        def __gt__(self, other):
            fieldname = self._fieldname(type(other))
            return literal_column(fieldname) > other

        def __le__(self, other):
            fieldname = self._fieldname(type(other))
            return literal_column(fieldname) <= other

        def __ge__(self, other):
            fieldname = self._fieldname(type(other))
            return literal_column(fieldname) >= other

        def regexp_match(self, pattern, flags=None):
            return literal_column('char_value').regexp_match(pattern, flags)

        def contains(self, other, **kwargs):
            fieldname = self._fieldname(type(other))

            if type(other) == list:
                pattern = r'(,|^)' + other[0] + r'(,|$)'
                return literal_column(fieldname).regexp_match(pattern)

    def __repr__(self):  # pragma: no cover
        return f"<{self.__class__.__name__} {self.key}={self.value}>"


@event.listens_for(
    PolymorphicVerticalProperty, "mapper_configured", propagate=True
)
def on_new_class(mapper, cls_):
    """Look for Column objects with type info in them, and work up
    a lookup table."""

    info_dict = {
        type(None): (None, "none"),
        "none": (None, "none")
    }

    for k, col in mapper.c.items():
        if "type" in col.info:
            python_type, discriminator = col.info["type"]
            info_dict[python_type] = \
                info_dict[discriminator] = (k, discriminator)
    cls_.type_map = info_dict


event_in_catalogue_association_table = \
    Table('event_in_catalogue', Base.metadata,
          Column('event_id', Integer, ForeignKey('events.id')),
          Column('catalogue_id', Integer, ForeignKey('catalogues.id')))

event_in_catalogue_association_index = Index(
    'e_in_c_index',
    event_in_catalogue_association_table.c.event_id,
    event_in_catalogue_association_table.c.catalogue_id,
    unique=True)


class EventAttributes(PolymorphicVerticalProperty, Base):
    """Meta-data (key-value-store) for an event."""

    __tablename__ = "events_attributes"

    event_id = Column(ForeignKey("events.id"), primary_key=True)
    key = Column(Unicode(64), primary_key=True)
    type = Column(Unicode(16), nullable=False)

    int_value = Column(Integer, info={"type": (int, "integer")})
    char_value = Column(UnicodeText, info={"type": (str, "string")})
    boolean_value = Column(Boolean, info={"type": (bool, "boolean")})
    datetime_value = Column(DateTime, info={"type": (dt.datetime, "datetime")}, nullable=True)
    float_value = Column(Float, info={"type": (float, "float")})
    string_list_value = Column(ScalarListType(str), default=[], info={"type": (list, "string_list")})


class Tag(Base):
    __tablename__ = 'tags'

    id = Column(Integer, primary_key=True, autoincrement=True)
    name = Column(UnicodeText, nullable=False, unique=True)

    def __init__(self, name: str):
        self.name = name


tag_in_event_association_table = \
    Table('tag_in_event', Base.metadata,
          Column('tags_id', Integer, ForeignKey('tags.id')),
          Column('events_id', Integer, ForeignKey('events.id')))


class EventProduct(Base):
    __tablename__ = 'event_products'

    id = Column(Integer, primary_key=True, autoincrement=True)
    name = Column(UnicodeText, nullable=False, unique=True)

    def __init__(self, name: str):
        self.name = name


product_in_event_association_table = \
    Table('product_in_event', Base.metadata,
          Column('event_products_id', Integer, ForeignKey('event_products.id')),
          Column('events_id', Integer, ForeignKey('events.id')))


class Event(ProxiedDictMixin, Base):
    __tablename__ = 'events'

    id = Column(Integer, primary_key=True, autoincrement=True)

    uuid = Column(String(36), index=True, nullable=False)

    start = Column(DateTime, nullable=False)
    stop = Column(DateTime, nullable=False)
    author = Column(UnicodeText, nullable=False)

    tags = relationship("Tag", backref="events", secondary=tag_in_event_association_table)
    products = relationship("EventProduct", backref="events", secondary=product_in_event_association_table)

    removed = Column(Boolean, default=False)

    attributes = relationship(
        "EventAttributes",
        collection_class=attribute_mapped_collection("key"),
        cascade="all, delete-orphan"
    )

    _proxied = association_proxy(
        "attributes",
        "value",
        creator=lambda key, value: EventAttributes(key=key, value=value),
    )

    _attribute_class = EventAttributes

    def __init__(self, start, stop, author, uuid, tags, products):
        self.start = start
        self.stop = stop
        self.author = author
        self.uuid = uuid
        self.tags = tags
        self.products = products

    def __repr__(self):  # pragma: no cover
        return f'Event({self.id}: {self.start}, {self.stop}, {self.author}), {self.removed}, meta=' + self._proxied.__repr__()


class CatalogueAttributes(PolymorphicVerticalProperty, Base):
    """Meta-data (key-value-store) for a catalogue."""

    __tablename__ = "catalogues_attributes"

    event_id = Column(ForeignKey("catalogues.id"), primary_key=True)
    key = Column(Unicode(64), primary_key=True)
    type = Column(Unicode(16))

    int_value = Column(Integer, info={"type": (int, "integer")})
    char_value = Column(UnicodeText, info={"type": (str, "string")})
    boolean_value = Column(Boolean, info={"type": (bool, "boolean")})
    datetime_value = Column(DateTime, info={"type": (dt.datetime, "datetime")})
    float_value = Column(Float, info={"type": (float, "float")})
    string_list_value = Column(ScalarListType(str), default=[], info={"type": (list, "string_list")})


tag_in_cataloguet_association_table = \
    Table('tag_in_catalogue', Base.metadata,
          Column('tags_id', Integer, ForeignKey('tags.id')),
          Column('catalogues_id', Integer, ForeignKey('catalogues.id')))


class Catalogue(ProxiedDictMixin, Base):
    __tablename__ = 'catalogues'

    id = Column(Integer, primary_key=True, autoincrement=True)

    uuid = Column(String(36), index=True, nullable=False)

    name = Column(UnicodeText, nullable=False)
    author = Column(UnicodeText, nullable=False)
    predicate = Column(LargeBinary, nullable=True)

    tags = relationship("Tag", backref="catalogues", secondary=tag_in_cataloguet_association_table)

    removed = Column(Boolean, default=False)

    attributes = relationship(
        "CatalogueAttributes",
        collection_class=attribute_mapped_collection("key"),
        cascade="all, delete-orphan"
    )

    events = relationship("Event",
                          backref="catalogues",
                          secondary=event_in_catalogue_association_table)

    _proxied = association_proxy(
        "attributes",
        "value",
        creator=lambda key, value: CatalogueAttributes(key=key, value=value),
    )

    _attribute_class = CatalogueAttributes

    def __init__(self, name: str, author: str, uuid: str, tags: List[str], predicate: bytes):
        self.name = name
        self.author = author
        self.uuid = uuid
        self.tags = tags
        self.predicate = predicate

    def __repr__(self):  # pragma: no cover
        return f'Catalogue({self.id}: {self.name}, {self.author}, {self.removed}), attrs=' + self._proxied.__repr__()
