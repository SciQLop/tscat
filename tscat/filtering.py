import datetime as dt
from typing import Union, Literal

from typeguard import typechecked


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
    pass


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
