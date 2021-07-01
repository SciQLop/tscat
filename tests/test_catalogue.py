#!/usr/bin/env python

import unittest
from ddt import ddt, data, unpack

from tscat import Event, Catalogue

import datetime as dt


@ddt
class TestCatalogue(unittest.TestCase):
    def setUp(self):
        self.events = [
            Event(dt.datetime.now(), dt.datetime.now() + dt.timedelta(days=1), "Patrick"),
            Event(dt.datetime.now(), dt.datetime.now() + dt.timedelta(days=2), "Patrick"),
            Event(dt.datetime.now(), dt.datetime.now() + dt.timedelta(days=3), "Patrick"),
        ]

    @data(
        ("Catalogue Name", "", {}),
        ("Catalogue Name", "Patrick", {}),
        ("Catalogue Name", "Patrick", {'field': 2}),
        ("Catalogue Name", "Patrick", {'field_': 2}),
        ("Catalogue Name", "Patrick", {'field_with_underscores': 2}),
        ("Catalogue Name", "Patrick", {'field': 2.0}),
        ("Catalogue Name", "Patrick", {'field': "2"}),
        ("Catalogue Name", "Patrick", {'field': True}),
        ("Catalogue Name", "Patrick", {'field': dt.datetime.now()}),
        ("Catalogue Name", "Patrick", {'field': 2}),
        ("Catalogue Name", "Patrick", {'field': 2, 'Field': 3}),
        ("Catalogue Name", "Patrick",
         {'field': 2, 'field2': 3.14, 'field3': "str", 'field4': True, 'field5': dt.datetime.now()}),
    )
    @unpack
    def test_constructor_various_combinations_all_ok(self, name, author, attrs):
        e = Catalogue(name, author, **attrs)

        self.assertEqual(e.name, name)
        self.assertEqual(e.author, author)

        for k, v in attrs.items():
            self.assertEqual(e.__getattribute__(k), v)

        attr_repr = ', '.join(f'{k}={v}' for k, v in attrs.items())
        self.assertRegex(f'{e}',
                         r'^Catalogue\(name=' + name + r', author=' + author +
                         r', predicate=None\) attributes\(' + attr_repr + r'\)$')

    @data(
        ("", "", {}),
        ("", "", {}),
        ("Catalogue Name", "", {"_invalid": 2}),
        ("Catalogue Name", "", {"'invalid'": 2}),
        ("Catalogue Name", "", {"invalid'": 2}),
        ("Catalogue Name", "", {'"invalid"': 2}),
        ("Catalogue Name", "", {"\nvalid": 2}),
        ("Catalogue Name", "", {"nvalid\\\'": 2}),
    )
    @unpack
    def test_constructor_various_combinations_value_errorl(self, name, author, attrs):
        with self.assertRaises(ValueError):
            assert Catalogue(name, author, **attrs)

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

    def test_add_events_to_catalogue_constructor(self):
        c = Catalogue("Catalogue Name", "Patrick", events=self.events)
        self.assertCountEqual(self.events, c._added_events)

        c.remove_events(self.events[0])
        self.assertCountEqual(self.events[:1], c._removed_events)

    def test_add_events_to_catalogue_via_method(self):
        c = Catalogue("Catalogue Name", "Patrick")
        c.add_events(self.events)
        self.assertCountEqual(self.events, c._added_events)

        c.remove_events(self.events[0])
        self.assertCountEqual(self.events[:1], c._removed_events)

    def test_add_event_multiple_times_to_catalogue_via_method(self):
        c = Catalogue("Catalogue Name", "Patrick")
        c.add_events(self.events[0])
        c.add_events(self.events[0])
        self.assertCountEqual(self.events[:1] * 2, c._added_events)

        c.remove_events(self.events[0])
        c.remove_events(self.events[0])

        self.assertCountEqual(self.events[:1] * 2, c._removed_events)
