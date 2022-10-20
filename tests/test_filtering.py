import unittest
from ddt import ddt, data, unpack  # type: ignore
from typing import List

import tscat.orm_sqlalchemy
import tscat
from tscat import create_event, create_catalogue, add_events_to_catalogue, save, get_events, get_catalogues, _Event, \
    _Catalogue
from tscat.filtering import Predicate, Comparison, Field, Attribute, Has, Match, Not, All, Any, In, UUID, \
    InCatalogue, PredicateRecursionError, CatalogueFilterError

import datetime as dt

dates = [
    dt.datetime.now(),
    dt.datetime.now() + dt.timedelta(days=1),
    dt.datetime.now() + dt.timedelta(days=2)
]
events = []
catalogues = []

# initialize the backend to testing before anything is done on the datebase
tscat._backend = tscat.orm_sqlalchemy.Backend(testing=True)


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
        (In("Value", Field("FieldName")), "In('Value', Field('FieldName'))"),
        (InCatalogue(create_catalogue('Name', 'Author', uuid='957d65ae-f278-48f5-aab1-8cf50efeadef')),
         "InCatalogue(Catalogue(name=Name, author=Author, uuid=957d65ae-f278-48f5-aab1-8cf50efeadef, tags=[], predicate=None) attributes())")
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
            create_event(dates[0], dates[1], "Patrick", a=1, b=12, f=30, s='Hello'),
            create_event(dates[1], dates[2], "Alexis", a=1, b=11, g=30, s='World'),
            create_event(dates[0], dates[2], "Nicolas", a=1, b=10, h=30, s='Goodbye!'),
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
            create_event(dates[0], dates[2], "Patrick", tags=["tag1", "tag2"], sl=["name", "tagAA"]),
            create_event(dates[0], dates[2], "Someone", tags=["tag2", "tag3"], products=["prd1", "prd2"],
                         sl=["tag1", "tagA", "name"]),
            create_event(dates[0], dates[2], "Person", sl=["tagc", "taga"]),
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
class TestCatalogueFiltering(unittest.TestCase):
    @classmethod
    def setUpClass(cls) -> None:
        tscat._backend = tscat.orm_sqlalchemy.Backend(testing=True)

        global catalogues
        catalogues = [
            create_catalogue('Catalogue A', "Patrick", a=1, b=12, f=30, s='Hello'),
            create_catalogue('Catalogue B', "Alexis", a=1, b=11, g=30, s='World'),
            create_catalogue('Catalogue C', "Nicolas", a=1, b=10, h=30, s='Goodbye!'),
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


@ddt
class TestUUIDFiltering(unittest.TestCase):
    uuid1: str
    uuid2: str
    events: List[_Event]
    catalogues: List[_Catalogue]

    @classmethod
    def setUp(self) -> None:
        tscat._backend = tscat.orm_sqlalchemy.Backend(testing=True)

        self.uuid1 = 'aa1b3598-babf-4317-9b54-4d7be254121e'
        self.uuid2 = 'aa1b3598-babf-4317-9b54-4d7be254121f'

        self.events = [
            create_event(dt.datetime.now(), dt.datetime.now() + dt.timedelta(days=1), "Patrick", self.uuid1),
            create_event(dt.datetime.now(), dt.datetime.now() + dt.timedelta(days=3), "Alexis", self.uuid2),
        ]

        self.catalogues = [
            create_catalogue('Catalogue A', "Patrick", self.uuid1),
            create_catalogue('Catalogue B', "Alexis", self.uuid2),
        ]

        save()

    def test_get_event_from_uuid(self):
        e = get_events(Comparison("==", Field('uuid'), self.uuid1))
        self.assertEqual(len(e), 1)
        self.assertEqual(self.events[0], e[0])

        e = get_events(UUID(self.uuid2))
        self.assertEqual(len(e), 1)
        self.assertEqual(self.events[1], e[0])

    def test_get_catalogues_from_uuid(self):
        c = get_catalogues(Comparison("==", Field('uuid'), self.uuid1))
        self.assertEqual(len(c), 1)
        self.assertEqual(self.catalogues[0], c[0])

        c = get_catalogues(UUID(self.uuid2))
        self.assertEqual(len(c), 1)
        self.assertEqual(self.catalogues[1], c[0])


class TestEventFilteringOnCatalogues(unittest.TestCase):
    def setUp(self) -> None:
        tscat._backend = tscat.orm_sqlalchemy.Backend(testing=True)
        self.c = create_catalogue('Catalogue A', "Alexis")
        self.d = create_catalogue('Catalogue B', "Patrick")

        self.events = [
            create_event(dates[0], dates[1], "Patrick"),
            create_event(dates[1], dates[2], "Alexis"),
            create_event(dates[0], dates[2], "Nicolas"),
            create_event(dates[0], dates[2], "Toto"),
        ]
        add_events_to_catalogue(self.c, [self.events[1], self.events[3]])
        add_events_to_catalogue(self.d, [self.events[2], self.events[3]])

    def test_get_events_which_are_in_no_catalogue(self):
        self.assertListEqual(get_events(InCatalogue(None)), [self.events[0]])

    def test_get_events_of_one_catalogue_using_in_catalogue_predicate(self):
        self.assertListEqual(get_events(InCatalogue(self.c)), [self.events[1], self.events[3]])

    def test_get_events_present_in_multiples_catalogues_using_in_catalogue_predicate(self):
        self.assertListEqual(get_events(All(InCatalogue(self.c), InCatalogue(self.d))), [self.events[3]])

    def test_get_events_present_in_any_of_the_catalogues_using_in_catalogue_predicate(self):
        self.assertListEqual(get_events(Any(InCatalogue(self.c), InCatalogue(self.d))), self.events[1:])

    def test_get_events_using_in_catalogue_with_a_dynamic_catalogue(self):
        n = create_catalogue('DynCatalogue', "Patrick", predicate=Comparison('==', Field("author"), 'Patrick'))
        self.assertListEqual(get_events(n), self.events[0:1])

    def test_get_events_using_in_catalogue_with_a_dynamic_catalogue_which_has_a_static_event(self):
        n = create_catalogue('DynCatalogue', "Patrick", predicate=Comparison('==', Field("author"), 'Patrick'))
        add_events_to_catalogue(n, self.events[1])
        self.assertListEqual(get_events(n), self.events[0:2])

    def test_raise_predicate_recursion_when_referencing_self(self):
        a = create_catalogue("TestCatalogue", "Patrick")
        a.predicate = InCatalogue(a)
        with self.assertRaises(PredicateRecursionError):
            get_events(a)

    def test_raise_predicate_recursion_on_some_deeper_recursion(self):
        a = create_catalogue("TestCatalogue", "Patrick")
        b = create_catalogue("TestCatalogue", "Patrick", predicate=InCatalogue(a))
        a.predicate = InCatalogue(b)
        with self.assertRaises(PredicateRecursionError):
            get_events(a)

    def test_raise_if_get_catalogue_is_used_with_in_catalogue(self):
        with self.assertRaises(CatalogueFilterError):
            get_catalogues(InCatalogue(self.c))
