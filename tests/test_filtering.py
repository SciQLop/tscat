#!/usr/bin/env python

import unittest
from ddt import ddt, data, unpack

import tscat.orm_sqlalchemy
from tscat import Event, get_events, Catalogue, get_catalogues
from tscat.filtering import Predicate, Comparison, Field, Attribute, Has, Match, Not, All, Any, In

import datetime as dt

dates = [
    dt.datetime.now(),
    dt.datetime.now() + dt.timedelta(days=1),
    dt.datetime.now() + dt.timedelta(days=2)
]
events = []
catalogues = []


@ddt
class TestFilterRepr(unittest.TestCase):

    @data(
        (Field('fieldName'), "Field('fieldName')"),
        (Attribute('attrName'), "Attribute('attrName')"),
        (Has(Attribute('attrName')), "Has(Attribute('attrName'))"),
        (Comparison('<=', Field('fieldName'), 'value'), "Comparison('<=', Field('fieldName'), 'value')"),
        (Match(Field('fieldName'), r'^mat[ch]{2}\n$'), "Match(Field('fieldName'), '^mat[ch]{2}\\\\n$')"),
        (Not(Comparison('<=', Field('fieldName'), 'value')), "Not(Comparison('<=', Field('fieldName'), 'value'))"),
        (Any(Comparison('<=', Field('fieldName'), 'value'), Match(Field('fieldName'), r'^mat[ch]{2}\n$')),
         "Any(Comparison('<=', Field('fieldName'), 'value'), Match(Field('fieldName'), '^mat[ch]{2}\\\\n$'))"),
        (All(Comparison('<=', Field('fieldName'), 'value'), Match(Field('fieldName'), r'^mat[ch]{2}\n$')),
         "All(Comparison('<=', Field('fieldName'), 'value'), Match(Field('fieldName'), '^mat[ch]{2}\\\\n$'))"),
        (In("Value", Field("FieldName")), "In('Value', Field('FieldName'))")
    )
    @unpack
    def test_predicate_repr(self, pred: Predicate, expected: str) -> None:
        self.assertEqual(f'{pred}', expected)


@ddt
class TestEventFiltering(unittest.TestCase):
    @classmethod
    def setUpClass(cls) -> None:
        tscat._backend = tscat.orm_sqlalchemy.Backend(testing=True)

        global events
        events = [
            Event(dates[0], dates[1], "Patrick", a=1, b=12, f=30, s='Hello'),
            Event(dates[1], dates[2], "Alexis", a=1, b=11, g=30, s='World'),
            Event(dates[0], dates[2], "Nicolas", a=1, b=10, h=30, s='Goodbye!'),
        ]

    @data(
        ('==', Field('author'), 'Patrick', [0]),
        ('!=', Field('author'), 'Patrick', [1, 2]),
        ('<', Field('author'), 'Patrick', [1, 2]),
        ('>', Field('author'), 'Patrick', []),
        ('<=', Field('author'), 'Patrick', [0, 1, 2]),
        ('>=', Field('author'), 'Patrick', [0]),

        ('==', Field('start'), dates[0], [0, 2]),
        ('>', Field('start'), dates[0], [1]),
        ('<', Field('stop'), dates[2], [0]),
        ('==', Field('stop'), dates[2], [1, 2]),

        ('==', Attribute('a'), 1, [0, 1, 2]),
        ('==', Attribute('a'), 0, []),
        ('!=', Attribute('a'), 1, []),
        ('<', Attribute('a'), 1, []),
        ('<=', Attribute('a'), 1, [0, 1, 2]),
        ('>', Attribute('a'), 1, []),
        ('>=', Attribute('a'), 1, [0, 1, 2]),

        ('==', Attribute('b'), 10, [2]),
        ('==', Attribute('b'), 11, [1]),
        ('==', Attribute('b'), 12, [0]),
        ('!=', Attribute('b'), 10, [0, 1]),
        ('!=', Attribute('b'), 11, [0, 2]),
        ('!=', Attribute('b'), 12, [1, 2]),
        ('<', Attribute('b'), 12, [1, 2]),
        ('<', Attribute('b'), 11, [2]),
        ('<', Attribute('b'), 10, []),
        ('<=', Attribute('b'), 12, [0, 1, 2]),
        ('<=', Attribute('b'), 11, [1, 2]),
        ('<=', Attribute('b'), 10, [2]),
        ('>', Attribute('b'), 12, []),
        ('>', Attribute('b'), 11, [0]),
        ('>', Attribute('b'), 10, [0, 1]),
        ('>=', Attribute('b'), 12, [0]),
        ('>=', Attribute('b'), 11, [0, 1]),
        ('>=', Attribute('b'), 10, [0, 1, 2]),

        ('==', Attribute('f'), 30, [0]),
        ('!=', Attribute('f'), 30, []),
        ('==', Attribute('g'), 30, [1]),
        ('!=', Attribute('g'), 30, []),
        ('==', Attribute('h'), 30, [2]),
        ('!=', Attribute('h'), 30, []),

        ('==', Attribute('s'), 'Hello', [0]),
        ('==', Attribute('s'), 'World', [1]),
        ('==', Attribute('s'), 'Goodbye!', [2]),
    )
    @unpack
    def test_comparison(self, op, lhs, rhs, idx):
        event_list = get_events(Comparison(op, lhs, rhs))
        self.assertListEqual(event_list, [events[i] for i in idx])

    @data(
        ('a', [0, 1, 2]),
        ('b', [0, 1, 2]),
        ('s', [0, 1, 2]),
        ('f', [0]),
        ('g', [1]),
        ('h', [2]),
        ('u', [])
    )
    @unpack
    def test_has_attribute(self, attr, idx):
        event_list = get_events(Has(Attribute(attr)))
        self.assertListEqual(event_list, [events[i] for i in idx])

    @data(
        (Field('author'), r'a', [0, 2]),
        (Field('author'), r'A', [1]),
        (Field('author'), r's$', [1, 2]),
        (Field('author'), r'^[AN]{1}.*s$', [1, 2]),
        (Attribute('s'), r'!$', [2]),
        (Attribute('s'), r'^G', [2]),
    )
    @unpack
    def test_match(self, field_or_attr, pattern, idx):
        event_list = get_events(Match(field_or_attr, pattern))
        self.assertListEqual(event_list, [events[i] for i in idx])

    @data(
        (All(Match(Field('author'), r'a'), Has(Attribute('h'))), [2]),
        (Any(Match(Field('author'), r'a'), Has(Attribute('h'))), [0, 2]),
        (All(Match(Field('author'), r'a'), Has(Attribute('g'))), []),
        (All(Match(Field('author'), r'a'), Not(Has(Attribute('g')))), [0, 2]),
        (Any(Match(Field('author'), r'a'), Has(Attribute('g'))), [0, 1, 2]),
    )
    @unpack
    def test_logical_combinations(self, pred, idx):
        event_list = get_events(All(pred))
        self.assertListEqual(event_list, [events[i] for i in idx])


@ddt
class TestStringListAttributes(unittest.TestCase):
    @classmethod
    def setUpClass(cls) -> None:
        tscat._backend = tscat.orm_sqlalchemy.Backend(testing=True)

        global events
        events = [
            Event(dates[0], dates[2], "Patrick", tags=["tag1", "tag2"], sl=["name", "tagAA"]),
            Event(dates[0], dates[2], "Someone", tags=["tag2", "tag3"], products=["prd1", "prd2"],
                  sl=["tag1", "tagA", "name"]),
            Event(dates[0], dates[2], "Person", sl=["tagc", "taga"]),
        ]

    def test_(self):
        event_list = get_events(In('name', Attribute('sl')))
        self.assertListEqual(event_list, events[0:2])

        event_list = get_events(In('tagA', Attribute('sl')))
        self.assertListEqual(event_list, [events[1]])

        event_list = get_events(In('tagAA', Attribute('sl')))
        self.assertListEqual(event_list, [events[0]])

        event_list = get_events(In('tag1', Attribute('sl')))
        self.assertListEqual(event_list, [events[1]])

        event_list = get_events(Any(In('tag1', Attribute('sl')),
                                    In('tagc', Attribute('sl'))))
        self.assertListEqual(event_list, [events[1], events[2]])

        event_list = get_events(Any(In('t', Attribute('sl'))))
        self.assertListEqual(event_list, [])

        # fields
        event_list = get_events(In('t', Field("tags")))
        self.assertListEqual(event_list, [])

        event_list = get_events(In('tag2', Field("tags")))
        self.assertListEqual(event_list, events[0:2])

        event_list = get_events(In('prd1', Field("products")))
        self.assertListEqual(event_list, [events[1]])

        event_list = get_events(All(
            In('prd1', Field("products")),
            In('prd2', Field("products"))))
        self.assertListEqual(event_list, [events[1]])


@ddt
class TestUsageExceptions(unittest.TestCase):

    @data(
        lambda: Field(123),
        lambda: Attribute(123),

        lambda: Comparison('!!', Field('fieldName'), 'value'),
        lambda: Comparison('<=', 123, 'value'),

        lambda: Match(123, r'^mat[ch]{2}\n$'),
        lambda: Match(Field('fieldName'), 123),

        lambda: Not(123),
        lambda: All(123),
        lambda: Any(123),

        lambda: Has(123),
        lambda: Has(Field('name')),
        lambda: In(123, Field('fieldName')),
        lambda: In("asd", 123),
    )
    def test_invalid_type_for_ctors_args(self, func):
        with self.assertRaises(TypeError):
            func()


@ddt
class TestCatalogueFiltering(unittest.TestCase):
    @classmethod
    def setUpClass(cls) -> None:
        tscat._backend = tscat.orm_sqlalchemy.Backend(testing=True)

        global catalogues
        catalogues = [
            Catalogue('Catalogue A', "Patrick", a=1, b=12, f=30, s='Hello'),
            Catalogue('Catalogue B', "Alexis", a=1, b=11, g=30, s='World'),
            Catalogue('Catalogue C', "Nicolas", a=1, b=10, h=30, s='Goodbye!'),
        ]

    @data(
        ('==', Field('author'), 'Patrick', [0]),
        ('!=', Field('author'), 'Patrick', [1, 2]),
        ('<', Field('author'), 'Patrick', [1, 2]),
        ('>', Field('author'), 'Patrick', []),
        ('<=', Field('author'), 'Patrick', [0, 1, 2]),
        ('>=', Field('author'), 'Patrick', [0]),

        ('==', Attribute('a'), 1, [0, 1, 2]),
        ('==', Attribute('a'), 0, []),
        ('!=', Attribute('a'), 1, []),
        ('<', Attribute('a'), 1, []),
        ('<=', Attribute('a'), 1, [0, 1, 2]),
        ('>', Attribute('a'), 1, []),
        ('>=', Attribute('a'), 1, [0, 1, 2]),

        ('==', Attribute('b'), 10, [2]),
        ('==', Attribute('b'), 11, [1]),
        ('==', Attribute('b'), 12, [0]),
        ('!=', Attribute('b'), 10, [0, 1]),
        ('!=', Attribute('b'), 11, [0, 2]),
        ('!=', Attribute('b'), 12, [1, 2]),
        ('<', Attribute('b'), 12, [1, 2]),
        ('<', Attribute('b'), 11, [2]),
        ('<', Attribute('b'), 10, []),
        ('<=', Attribute('b'), 12, [0, 1, 2]),
        ('<=', Attribute('b'), 11, [1, 2]),
        ('<=', Attribute('b'), 10, [2]),
        ('>', Attribute('b'), 12, []),
        ('>', Attribute('b'), 11, [0]),
        ('>', Attribute('b'), 10, [0, 1]),
        ('>=', Attribute('b'), 12, [0]),
        ('>=', Attribute('b'), 11, [0, 1]),
        ('>=', Attribute('b'), 10, [0, 1, 2]),

        ('==', Attribute('f'), 30, [0]),
        ('!=', Attribute('f'), 30, []),
        ('==', Attribute('g'), 30, [1]),
        ('!=', Attribute('g'), 30, []),
        ('==', Attribute('h'), 30, [2]),
        ('!=', Attribute('h'), 30, []),

        ('==', Attribute('s'), 'Hello', [0]),
        ('==', Attribute('s'), 'World', [1]),
        ('==', Attribute('s'), 'Goodbye!', [2]),
    )
    @unpack
    def test_comparison(self, op, lhs, rhs, idx):
        catalogue_list = get_catalogues(Comparison(op, lhs, rhs))
        self.assertListEqual(catalogue_list, [catalogues[i] for i in idx])

    @data(
        ('a', [0, 1, 2]),
        ('b', [0, 1, 2]),
        ('s', [0, 1, 2]),
        ('f', [0]),
        ('g', [1]),
        ('h', [2]),
        ('u', [])
    )
    @unpack
    def test_has_attribute(self, attr, idx):
        catalogue_list = get_catalogues(Has(Attribute(attr)))
        self.assertListEqual(catalogue_list, [catalogues[i] for i in idx])

    @data(
        (Field('author'), r'a', [0, 2]),
        (Field('author'), r'A', [1]),
        (Field('author'), r's$', [1, 2]),
        (Field('author'), r'^[AN]{1}.*s$', [1, 2]),
        (Attribute('s'), r'!$', [2]),
        (Attribute('s'), r'^G', [2]),
    )
    @unpack
    def test_match(self, field_or_attr, pattern, idx):
        catalogue_list = get_catalogues(Match(field_or_attr, pattern))
        self.assertListEqual(catalogue_list, [catalogues[i] for i in idx])

    @data(
        (All(Match(Field('author'), r'a'), Has(Attribute('h'))), [2]),
        (Any(Match(Field('author'), r'a'), Has(Attribute('h'))), [0, 2]),
        (All(Match(Field('author'), r'a'), Has(Attribute('g'))), []),
        (All(Match(Field('author'), r'a'), Not(Has(Attribute('g')))), [0, 2]),
        (Any(Match(Field('author'), r'a'), Has(Attribute('g'))), [0, 1, 2]),
    )
    @unpack
    def test_logical_combinations(self, pred, idx):
        catalogue_list = get_catalogues(All(pred))
        self.assertListEqual(catalogue_list, [catalogues[i] for i in idx])
