import datetime as dt

from typing import Union
from typing_extensions import Literal
from typeguard import typechecked, typeguard_ignore
import uuid


@typechecked
class Field:
    def __init__(self, name: str):
        self.value = name

    def __repr__(self):
        return f"Field('{self.value}')"


@typechecked
class Attribute:
    def __init__(self, name: str):
        self.value = name

    def __repr__(self):
        return f"Attribute('{self.value}')"


@typechecked
class Predicate:
    def __eq__(self, o):
        return repr(self) == repr(o)


@typechecked
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


@typechecked
class Match(Predicate):
    def __init__(self,
                 lhs: Union[Field, Attribute],
                 rhs: str):  # regex
        self._lhs = lhs
        self._rhs = rhs

    def __repr__(self):
        return f"Match({self._lhs}, {repr(self._rhs)})"


@typechecked
class Not(Predicate):
    def __init__(self, operand: "Predicate"):
        self._operand = operand

    def __repr__(self):
        return f"Not({self._operand})"


@typechecked
class Has(Predicate):
    def __init__(self, operand: Attribute):
        self._operand = operand

    def __repr__(self):
        return f"Has({self._operand})"


@typechecked
class All(Predicate):
    def __init__(self, *args: Predicate):
        self._predicates = args

    def __repr__(self):
        return "All({})".format(', '.join(repr(p) for p in self._predicates))


@typechecked
class Any(Predicate):
    def __init__(self, *args: Predicate):
        self._predicates = args

    def __repr__(self):
        return "Any({})".format(', '.join(repr(p) for p in self._predicates))


@typechecked
class In(Predicate):
    def __init__(self, lhs: str, rhs: Union[Field, Attribute]):
        self._lhs = lhs
        self._rhs = rhs

    def __repr__(self):
        return f"In('{self._lhs}', {repr(self._rhs)})"


@typechecked
class UUID(Comparison):
    def __init__(self, uuid_: str):
        uuid.UUID(uuid_, version=4)
        super().__init__('==', Field('uuid'), uuid_)


@typechecked
class InCatalogue(Predicate):
    @typeguard_ignore
    def __init__(self, catalogue: Union['Catalogue', None] = None):
        # poor man's type-check, "Catalogue" does not work as forward declaration with typeguard
        if catalogue is not None and \
                f"{catalogue.__class__.__module__}.{catalogue.__class__.__name__}" != 'tscat.Catalogue':
            raise TypeError('Expected None or Catalogue.')
        self.catalogue = catalogue

    def __repr__(self):
        return f"InCatalogue({self.catalogue})"


@typechecked
class PredicateRecursionError(Exception):
    def __init__(self, message: str, predicate: Predicate):
        super().__init__(message)

        self.predicate = predicate


@typechecked
class CatalogueFilterError(Exception):
    def __init__(self, message: str):
        super().__init__(message)
