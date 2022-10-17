import datetime as dt

from typing import Union, TYPE_CHECKING
from typing_extensions import Literal
import uuid

if TYPE_CHECKING:
    from . import _Catalogue


class Field:
    def __init__(self, name: str):
        self.value = name

    def __repr__(self):
        return f"Field('{self.value}')"


class Attribute:
    def __init__(self, name: str):
        self.value = name

    def __repr__(self):
        return f"Attribute('{self.value}')"


class Predicate:
    def __eq__(self, o):
        return repr(self) == repr(o)


class Comparison(Predicate):
    def __init__(self,
                 op: Union[Literal['>'], Literal['>='],
                           Literal['<'], Literal['<='],
                           Literal['=='], Literal['!=']],
                 lhs: Union[Field, Attribute],
                 rhs: Union[str, int, float, dt.datetime, bool]):
        self._op = op
        self._lhs = lhs
        self._rhs = rhs

    def __repr__(self):
        return f"Comparison('{self._op}', {self._lhs}, {repr(self._rhs)})"


class Match(Predicate):
    def __init__(self,
                 lhs: Union[Field, Attribute],
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
    def __init__(self, lhs: str, rhs: Union[Field, Attribute]):
        self._lhs = lhs
        self._rhs = rhs

    def __repr__(self):
        return f"In('{self._lhs}', {repr(self._rhs)})"


class UUID(Comparison):
    def __init__(self, uuid_: str):
        uuid.UUID(uuid_, version=4)
        super().__init__('==', Field('uuid'), uuid_)


class InCatalogue(Predicate):
    def __init__(self, catalogue: Union['_Catalogue', None] = None):
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
