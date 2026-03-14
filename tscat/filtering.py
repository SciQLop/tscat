import datetime as dt

from typing import Union, TYPE_CHECKING
from typing_extensions import Literal
import uuid

if TYPE_CHECKING:
    from . import _Catalogue

MemberValueType = Union[str, int, float, dt.datetime, bool]

class _Member:
    def __init__(self, name: str):
        self.value = name

    def __eq__(self, other: MemberValueType) -> 'Predicate': # type: ignore[override]
        return Comparison('==', self, other)

    def __ne__(self, other) -> 'Predicate': # type: ignore[override]
        return Comparison('!=', self, other)

    def __gt__(self, other) -> 'Predicate':
        return Comparison('>', self, other)

    def __lt__(self, other) -> 'Predicate':
        return Comparison('<', self, other)

    def __ge__(self, other) -> 'Predicate':
        return Comparison('>=', self, other)

    def __le__(self, other) -> 'Predicate':
        return Comparison('<=', self, other)

    def matches(self, value: str) -> 'Predicate':
        return Match(self, value)



class Field(_Member):
    def __init__(self, name: str):
        super().__init__(name)

    def __repr__(self):
        return f"Field('{self.value}')"

    def to_dict(self):
        return {"type": "Field", "name": self.value}


class Attribute(_Member):
    def __init__(self, name: str):
        super().__init__(name)

    def __repr__(self):
        return f"Attribute('{self.value}')"

    def to_dict(self):
        return {"type": "Attribute", "name": self.value}

    def exists(self) -> 'Predicate':
        return Has(self)


def _member_from_dict(d: dict) -> _Member:
    return Field(d['name']) if d['type'] == 'Field' else Attribute(d['name'])

class Predicate:
    def __eq__(self, o):
        return repr(self) == repr(o)

    def __and__(self, other):
        if isinstance(other, Predicate):
            return All(self, other)
        elif isinstance(other, (list, tuple)) and all(isinstance(item, Predicate) for item in other):
            return All(self, *other)
        else:
            raise TypeError(f"Cannot combine {type(self).__name__} with {type(other).__name__}")

    def __or__(self, other):
        if isinstance(other, Predicate):
            return Any(self, other)
        elif isinstance(other, (list, tuple)) and all(isinstance(item, Predicate) for item in other):
            return Any(self, *other)
        else:
            raise TypeError(f"Cannot combine {type(self).__name__} with {type(other).__name__}")

    def __invert__(self):
        return Not(self)

    def to_dict(self) -> dict:
        raise NotImplementedError

    @staticmethod
    def from_dict(d: dict) -> 'Predicate':
        _type_map = {
            'Comparison': Comparison,
            'Match': Match,
            'Not': Not,
            'Has': Has,
            'All': All,
            'Any': Any,
            'In': In,
            'UUID': UUID,
            'InCatalogue': InCatalogue,
        }
        return _type_map[d['type']]._from_dict(d)  # type: ignore[attr-defined]


class Comparison(Predicate):
    def __init__(self,
                 op: Union[Literal['>'], Literal['>='],
                 Literal['<'], Literal['<='],
                 Literal['=='], Literal['!=']],
                 lhs: _Member,
                 rhs: MemberValueType):
        self._op = op
        self._lhs = lhs
        self._rhs = rhs

    def __repr__(self):
        return f"Comparison('{self._op}', {self._lhs}, {repr(self._rhs)})"

    def to_dict(self):
        d = {"type": "Comparison", "op": self._op, "lhs": self._lhs.to_dict(), "rhs": self._rhs}
        if isinstance(self._rhs, dt.datetime):
            d["rhs"] = self._rhs.isoformat()
            d["rhs_type"] = "datetime"
        return d

    @classmethod
    def _from_dict(cls, d):
        lhs = _member_from_dict(d['lhs'])
        rhs = d['rhs']
        if d.get('rhs_type') == 'datetime':
            rhs = dt.datetime.fromisoformat(rhs)
        return cls(d['op'], lhs, rhs)


class Match(Predicate):
    def __init__(self,
                 lhs: _Member,
                 rhs: str):  # regex
        self._lhs = lhs
        self._rhs = rhs

    def __repr__(self):
        return f"Match({self._lhs}, {repr(self._rhs)})"

    def to_dict(self):
        return {"type": "Match", "lhs": self._lhs.to_dict(), "rhs": self._rhs}

    @classmethod
    def _from_dict(cls, d):
        return cls(_member_from_dict(d['lhs']), d['rhs'])


class Not(Predicate):
    def __init__(self, operand: "Predicate"):
        self._operand = operand

    def __repr__(self):
        return f"Not({self._operand})"

    def to_dict(self):
        return {"type": "Not", "operand": self._operand.to_dict()}

    @classmethod
    def _from_dict(cls, d):
        return cls(Predicate.from_dict(d['operand']))


class Has(Predicate):
    def __init__(self, operand: Attribute):
        self._operand = operand

    def __repr__(self):
        return f"Has({self._operand})"

    def to_dict(self):
        return {"type": "Has", "operand": self._operand.to_dict()}

    @classmethod
    def _from_dict(cls, d):
        return cls(Attribute(d['operand']['name']))


class All(Predicate):
    def __init__(self, *args: Predicate):
        self._predicates = args

    def __repr__(self):
        return "All({})".format(', '.join(repr(p) for p in self._predicates))

    def to_dict(self):
        return {"type": "All", "predicates": [p.to_dict() for p in self._predicates]}

    @classmethod
    def _from_dict(cls, d):
        return cls(*[Predicate.from_dict(p) for p in d['predicates']])


class Any(Predicate):
    def __init__(self, *args: Predicate):
        self._predicates = args

    def __repr__(self):
        return "Any({})".format(', '.join(repr(p) for p in self._predicates))

    def to_dict(self):
        return {"type": "Any", "predicates": [p.to_dict() for p in self._predicates]}

    @classmethod
    def _from_dict(cls, d):
        return cls(*[Predicate.from_dict(p) for p in d['predicates']])


class In(Predicate):
    def __init__(self, lhs: str, rhs: _Member):
        self._lhs = lhs
        self._rhs = rhs

    def __repr__(self):
        return f"In('{self._lhs}', {repr(self._rhs)})"

    def to_dict(self):
        return {"type": "In", "lhs": self._lhs, "rhs": self._rhs.to_dict()}

    @classmethod
    def _from_dict(cls, d):
        return cls(d['lhs'], _member_from_dict(d['rhs']))


class UUID(Comparison):
    def __init__(self, uuid_: str):
        uuid.UUID(uuid_, version=4)
        super().__init__('==', Field('uuid'), uuid_)

    def to_dict(self):
        return {"type": "UUID", "uuid": self._rhs}

    @classmethod
    def _from_dict(cls, d):
        return cls(d['uuid'])


class InCatalogue(Predicate):
    def __init__(self, catalogue: '_Catalogue'):
        self.catalogue = catalogue

    def __repr__(self):
        return f"InCatalogue({self.catalogue})"

    def to_dict(self):
        return {"type": "InCatalogue",
                "catalogue_uuid": self.catalogue.uuid if self.catalogue is not None else None}

    @classmethod
    def _from_dict(cls, d):
        from . import get_catalogue
        cat_uuid = d['catalogue_uuid']
        if cat_uuid is None:
            return cls(None)
        cat = get_catalogue(uuid=cat_uuid)
        if cat is None:
            raise ValueError(f"InCatalogue references unknown catalogue UUID: {cat_uuid}")
        return cls(cat)


class PredicateRecursionError(Exception):
    def __init__(self, message: str, predicate: Predicate):
        super().__init__(message)

        self.predicate = predicate


class CatalogueFilterError(Exception):
    def __init__(self, message: str):
        super().__init__(message)



class _CatalogueToken:
    def __init__(self):
        pass

    def __getattr__(self, item) -> _Member:
        if item in ('name', 'author', 'uuid', 'tags', 'predicate', 'attributes'):
            return Field(item)
        return Attribute(item)


class _EventsToken:
    def __init__(self):
        pass

    def __contains__(self, item: str) -> Predicate:
        if item in ('start', 'stop', 'author', 'tags', 'products', 'rating', 'uuid'):
            raise ValueError(f"'{item}' is always present in events, therefore cannot be used in 'in' checks.")
        return Attribute(item).exists()

    def __getattr__(self, item) -> _Member:
        if item in ('start', 'stop', 'author', 'tags', 'products', 'rating', 'uuid'):
            return Field(item)
        return Attribute(item)


# tokens to create predicates from Python code
catalogue = _CatalogueToken()
events = _EventsToken()
event = events
