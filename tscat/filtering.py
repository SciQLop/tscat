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


class Attribute(_Member):
    def __init__(self, name: str):
        super().__init__(name)

    def __repr__(self):
        return f"Attribute('{self.value}')"

    def exists(self) -> 'Predicate':
        return Has(self)

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


class Match(Predicate):
    def __init__(self,
                 lhs: _Member,
                 rhs: str):  # regex
        self._lhs = lhs
        self._rhs = rhs

    def __repr__(self):
        return f"Match({self._lhs}, {repr(self._rhs)})"


class Not(Predicate):
    def __init__(self, operand: "Predicate"):
        self._operand = operand

    def __repr__(self):
        return f"Not({self._operand})"


class Has(Predicate):
    def __init__(self, operand: Attribute):
        self._operand = operand

    def __repr__(self):
        return f"Has({self._operand})"


class All(Predicate):
    def __init__(self, *args: Predicate):
        self._predicates = args

    def __repr__(self):
        return "All({})".format(', '.join(repr(p) for p in self._predicates))


class Any(Predicate):
    def __init__(self, *args: Predicate):
        self._predicates = args

    def __repr__(self):
        return "Any({})".format(', '.join(repr(p) for p in self._predicates))


class In(Predicate):
    def __init__(self, lhs: str, rhs: _Member):
        self._lhs = lhs
        self._rhs = rhs

    def __repr__(self):
        return f"In('{self._lhs}', {repr(self._rhs)})"


class UUID(Comparison):
    def __init__(self, uuid_: str):
        uuid.UUID(uuid_, version=4)
        super().__init__('==', Field('uuid'), uuid_)


class InCatalogue(Predicate):
    def __init__(self, catalogue: '_Catalogue'):
        self.catalogue = catalogue

    def __repr__(self):
        return f"InCatalogue({self.catalogue})"


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
