#!/usr/bin/env python

import unittest
from ddt import ddt, data, unpack

import tscat.orm_sqlalchemy
from tscat import Event, Catalogue, get_events, save, discard, get_catalogues
from tscat.filtering import Comparison, Field

import datetime as dt
import re


@ddt
class TestCatalogue(unittest.TestCase):
    def setUp(self) -> None:
        tscat._backend = tscat.orm_sqlalchemy.Backend(testing=True)  # create a memory-database for tests

        self.events = [
            Event(dt.datetime.now(), dt.datetime.now() + dt.timedelta(days=1), "Patrick"),
            Event(dt.datetime.now(), dt.datetime.now() + dt.timedelta(days=2), "Patrick"),
            Event(dt.datetime.now(), dt.datetime.now() + dt.timedelta(days=3), "Patrick"),
        ]

        save()

    @data(
        ("Catalogue Name", "", None, {}),
        ("Catalogue Name", "Patrick", None, {}),
        ("Catalogue Name", "Patrick", "3c0bee4b-d38f-46e7-94d5-8a762a61bbf2", {}),
        ("Catalogue Name", "Patrick", None, {'field': 2}),
        ("Catalogue Name", "Patrick", None, {'field_': 2}),
        ("Catalogue Name", "Patrick", None, {'field_with_underscores': 2}),
        ("Catalogue Name", "Patrick", None, {'field': 2.0}),
        ("Catalogue Name", "Patrick", None, {'field': "2"}),
        ("Catalogue Name", "Patrick", None, {'field': True}),
        ("Catalogue Name", "Patrick", None, {'field': dt.datetime.now()}),
        ("Catalogue Name", "Patrick", None, {'field': 2}),
        ("Catalogue Name", "Patrick", None, {'field': 2, 'Field': 3}),
        ("Catalogue Name", "Patrick", None, {'field': 2, 'field2': 3.14, 'field3': "str", 'field4': True, 'field5': dt.datetime.now()}),
        ("Catalogue Name", "Patrick", "3c0bee4b-d38f-46e7-94d5-8a762a61bbf2", {'field': 2, 'Field': 3}, ['tag1', '#tag2']),
        ("Catalogue Name", "Patrick", None, {}, ['', '\'as']),
    )
    @unpack
    def test_constructor_various_combinations_all_ok(self, name, author, uuid, attrs, tags=[]):
        e = Catalogue(name, author, uuid, tags, **attrs)

        self.assertEqual(e.name, name)
        self.assertEqual(e.author, author)

        for k, v in attrs.items():
            self.assertEqual(e.__getattribute__(k), v)

        attr_repr = ', '.join(f'{k}={v}' for k, v in attrs.items())

        tags = re.escape(str(tags))
        self.assertRegex(f'{e}',
                         r'^Catalogue\(name=' + name + r', author=' + author +
                         r', uuid=[0-9a-f-]{36}, tags=' + tags +
                         r', predicate=None\) attributes\(' + attr_repr + r'\)$')

    @data(
        ("", "", None, {}),
        ("", "", 'invalid_uuid', {}),
        ("Catalogue Name", "", None, {"_invalid": 2}),
        ("Catalogue Name", "", None, {"'invalid'": 2}),
        ("Catalogue Name", "", None, {"invalid'": 2}),
        ("Catalogue Name", "", None, {'"invalid"': 2}),
        ("Catalogue Name", "", None, {"\nvalid": 2}),
        ("Catalogue Name", "", None, {"nvalid\\\'": 2}),
        ("Catalogue Name", "", None, {}, [123, "test"]),
        ("Catalogue Name", "", None, {}, [dict(), "test"]),
    )
    @unpack
    def test_constructor_various_combinations_value_errorl(self, name, author, uuid, attrs, tags=[]):
        with self.assertRaises(ValueError):
            assert Catalogue(name, author, uuid, tags, **attrs)

    def test_unequal_catalogues(self):
        a, b = Catalogue("Catalogue Name1", "Patrick"), Catalogue("Catalogue Name2", "Patrick")
        self.assertNotEqual(a, b)

        a, b = Catalogue("Catalogue Name", "Patrick", attr1=20), Catalogue("Catalogue Name", "Patrick", attr1=10)
        self.assertNotEqual(a, b)

        a, b = Catalogue("Catalogue Name", "Patrick", attr1=20), Catalogue("Catalogue Name", "Patrick", attr2=20)
        self.assertNotEqual(a, b)

    def test_constructor_with_dynamic_attribute_manual_access(self):
        dt_val = dt.datetime.now()
        c = Catalogue("Catalogue Name", "Patrick",
                      field_int=100, field_float=1.234, field_str="string-test", field_bool=True, field_dt=dt_val)

        self.assertEqual(c.name, "Catalogue Name")
        self.assertEqual(c.author, "Patrick")

        self.assertEqual(c.field_int, 100)
        self.assertEqual(c.field_float, 1.234)
        self.assertEqual(c.field_str, "string-test")
        self.assertEqual(c.field_bool, True)
        self.assertEqual(c.field_dt, dt_val)

    def test_add_and_get_empty_catalogues(self):
        catalogues = [Catalogue("Catalogue Name1", "Patrick"), Catalogue("Catalogue Name2", "Patrick")]
        cat_list = get_catalogues()
        self.assertListEqual(catalogues, cat_list)

    def test_add_and_get_empty_catalogues_discard_and_save(self):
        Catalogue("Catalogue Name1", "Patrick")
        Catalogue("Catalogue Name2", "Patrick")

        discard()

        cat_list = get_catalogues()
        self.assertListEqual([], cat_list)

        c = Catalogue("Catalogue Name2", "Patrick")

        cat_list = get_catalogues()
        self.assertListEqual([c], cat_list)

        save()

        cat_list = get_catalogues()
        self.assertListEqual([c], cat_list)

        c2 = Catalogue("Catalogue Name2", "Patrick")

        cat_list = get_catalogues()
        self.assertListEqual([c, c2], cat_list)

        discard()

        cat_list = get_catalogues()
        self.assertListEqual([c], cat_list)

    def test_add_events_to_catalogue_constructor(self):
        c = Catalogue("Catalogue Name", "Patrick", events=self.events)

        event_list = get_events(c)
        self.assertListEqual(event_list, self.events)

        c.remove_events(self.events[0])

        event_list = get_events(c)
        self.assertListEqual(event_list, self.events[1:])

    def test_add_events_to_catalogue_via_method(self):
        c = Catalogue("Catalogue Name", "Patrick")
        c.add_events(self.events)

        event_list = get_events(c)
        self.assertListEqual(self.events, event_list)

        c.remove_events(self.events[0])
        event_list = get_events(c)
        self.assertListEqual(event_list, self.events[1:])

    def test_add_event_multiple_times_to_catalogue(self):
        c = Catalogue("Catalogue Name", "Patrick")
        c.add_events(self.events[0])
        with self.assertRaises(ValueError):
            c.add_events(self.events[0])

    def test_catalogues_of_event(self):
        a = Catalogue("Catalogue Name A", "Patrick")
        a.add_events(self.events[0])
        a.add_events(self.events[1])
        b = Catalogue("Catalogue Name B", "Patrick")
        b.add_events(self.events[0])

        cat_list = get_catalogues(self.events[0])
        self.assertListEqual(cat_list, [a, b])

        cat_list = get_catalogues(self.events[1])
        self.assertListEqual(cat_list, [a])


@ddt
class TestDynamicCatalogue(unittest.TestCase):
    def setUp(self) -> None:
        tscat._backend = tscat.orm_sqlalchemy.Backend(testing=True)  # create a memory-database for tests

        self.events = [
            Event(dt.datetime.now(), dt.datetime.now() + dt.timedelta(days=1), "Patrick"),
            Event(dt.datetime.now(), dt.datetime.now() + dt.timedelta(days=2), "Patrick"),
            Event(dt.datetime.now(), dt.datetime.now() + dt.timedelta(days=3), "Patrick"),
            Event(dt.datetime.now(), dt.datetime.now() + dt.timedelta(days=3), "Alexis"),
            Event(dt.datetime.now(), dt.datetime.now() + dt.timedelta(days=3), "Alexis"),
            Event(dt.datetime.now(), dt.datetime.now() + dt.timedelta(days=3), "Alexis"),
        ]

        self.catalogue = Catalogue("Catalogue A", "Patrick", events=self.events)

        save()

    def test_basic_usage(self):
        dcat = Catalogue("Dynamic Catalogue 'author=Patrick'", "Patrick",
                         predicate=Comparison("==", Field("author"), "Patrick"))

        events = get_events(dcat)
        self.assertListEqual(events, self.events[:3])

        event = Event(dt.datetime.now(), dt.datetime.now() + dt.timedelta(days=3), "Alexis")
        dcat.add_events(event)

        events = get_events(dcat)
        self.assertListEqual(events, self.events[0:3] + [event])

        dcat.remove_events(event)

        events = get_events(dcat)
        self.assertListEqual(events, self.events[0:3])

        # impossible to remove events which are queried with filters
        with self.assertRaises(ValueError):
            dcat.remove_events(self.events)

        events = get_events(dcat)
        self.assertListEqual(events, self.events[0:3])

        catalogues = get_catalogues()
        self.assertFalse(catalogues[0].is_dynamic())
        self.assertTrue(catalogues[1].is_dynamic())
        self.assertEqual(catalogues[1], dcat)

    def test_predicate_field_is_updatable(self):
        dc = Catalogue("Dynamic Catalogue", "Patrick",
                       predicate=Comparison("==", Field("author"), "Patrick"))

        self.assertListEqual(get_events(dc), self.events[:3])

        dc.predicate = Comparison("==", Field("author"), "Alexis")

        self.assertListEqual(get_events(dc), self.events[3:])
