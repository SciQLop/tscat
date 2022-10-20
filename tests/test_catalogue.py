import unittest
from ddt import ddt, data, unpack  # type: ignore

import tscat.orm_sqlalchemy
from tscat import create_event, create_catalogue, add_events_to_catalogue, remove_events_from_catalogue, save, discard, \
    get_catalogues, get_events
from tscat.filtering import Comparison, Field

import datetime as dt
import re


@ddt
class Testcreate_catalogue(unittest.TestCase):
    def setUp(self) -> None:
        tscat._backend = tscat.orm_sqlalchemy.Backend(testing=True)  # create a memory-database for tests

        self.events = [
            create_event(dt.datetime.now(), dt.datetime.now() + dt.timedelta(days=1), "Patrick"),
            create_event(dt.datetime.now(), dt.datetime.now() + dt.timedelta(days=2), "Patrick"),
            create_event(dt.datetime.now(), dt.datetime.now() + dt.timedelta(days=3), "Patrick"),
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
        ("Catalogue Name", "Patrick", None,
         {'field': 2, 'field2': 3.14, 'field3': "str", 'field4': True, 'field5': dt.datetime.now()}),
        ("Catalogue Name", "Patrick", "3c0bee4b-d38f-46e7-94d5-8a762a61bbf2", {'field': 2, 'Field': 3},
         ['tag1', '#tag2']),
        ("Catalogue Name", "Patrick", None, {}, ['', '\'as']),
    )
    @unpack
    def test_constructor_various_combinations_all_ok(self, name, author, uuid, attrs, tags=[]):
        e = create_catalogue(name, author, uuid, tags, **attrs)

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
            assert create_catalogue(name, author, uuid, tags, **attrs)

    def test_unequal_catalogues(self):
        a, b = create_catalogue("Catalogue Name1", "Patrick"), create_catalogue("Catalogue Name2", "Patrick")
        self.assertNotEqual(a, b)

        a, b = create_catalogue("Catalogue Name", "Patrick", attr1=20), create_catalogue("Catalogue Name", "Patrick",
                                                                                         attr1=10)
        self.assertNotEqual(a, b)

        a, b = create_catalogue("Catalogue Name", "Patrick", attr1=20), create_catalogue("Catalogue Name", "Patrick",
                                                                                         attr2=20)
        self.assertNotEqual(a, b)

    def test_constructor_with_dynamic_attribute_manual_access(self):
        dt_val = dt.datetime.now()
        c = create_catalogue("Catalogue Name", "Patrick",
                             field_int=100, field_float=1.234, field_str="string-test", field_bool=True,
                             field_dt=dt_val)

        self.assertEqual(c.name, "Catalogue Name")
        self.assertEqual(c.author, "Patrick")

        self.assertEqual(c.field_int, 100)
        self.assertEqual(c.field_float, 1.234)
        self.assertEqual(c.field_str, "string-test")
        self.assertEqual(c.field_bool, True)
        self.assertEqual(c.field_dt, dt_val)

    def test_add_and_get_empty_catalogues(self):
        catalogues = [create_catalogue("Catalogue Name1", "Patrick"), create_catalogue("Catalogue Name2", "Patrick")]
        cat_list = get_catalogues()
        self.assertListEqual(catalogues, cat_list)

    def test_add_and_get_empty_catalogues_discard_and_save(self):
        create_catalogue("Catalogue Name1", "Patrick")
        create_catalogue("Catalogue Name2", "Patrick")

        discard()

        cat_list = get_catalogues()
        self.assertListEqual([], cat_list)

        c = create_catalogue("Catalogue Name2", "Patrick")

        cat_list = get_catalogues()
        self.assertListEqual([c], cat_list)

        save()

        cat_list = get_catalogues()
        self.assertListEqual([c], cat_list)

        c2 = create_catalogue("Catalogue Name2", "Patrick")

        cat_list = get_catalogues()
        self.assertListEqual([c, c2], cat_list)

        discard()

        cat_list = get_catalogues()
        self.assertListEqual([c], cat_list)

    def test_add_events_to_catalogue_constructor(self):
        c = create_catalogue("Catalogue Name", "Patrick", events=self.events)

        event_list = get_events(c)
        self.assertListEqual(event_list, self.events)

        remove_events_from_catalogue(c, self.events[0])

        event_list = get_events(c)
        self.assertListEqual(event_list, self.events[1:])

    def test_add_events_to_catalogue_via_method(self):
        c = create_catalogue("Catalogue Name", "Patrick")
        add_events_to_catalogue(c, self.events)

        event_list = get_events(c)
        self.assertListEqual(self.events, event_list)

        remove_events_from_catalogue(c, self.events[0])
        event_list = get_events(c)
        self.assertListEqual(event_list, self.events[1:])

    def test_add_event_multiple_times_to_catalogue(self):
        c = create_catalogue("Catalogue Name", "Patrick")
        add_events_to_catalogue(c, self.events[0])
        with self.assertRaises(ValueError):
            add_events_to_catalogue(c, self.events[0])

    def test_catalogues_of_event(self):
        a = create_catalogue("Catalogue Name A", "Patrick")
        add_events_to_catalogue(a, self.events[0])
        add_events_to_catalogue(a, self.events[1])
        b = create_catalogue("Catalogue Name B", "Patrick")
        add_events_to_catalogue(b, self.events[0])

        cat_list = get_catalogues(self.events[0])
        self.assertListEqual(cat_list, [a, b])

        cat_list = get_catalogues(self.events[1])
        self.assertListEqual(cat_list, [a])


@ddt
class TestDynamiccreate_catalogue(unittest.TestCase):
    def setUp(self) -> None:
        tscat._backend = tscat.orm_sqlalchemy.Backend(testing=True)  # create a memory-database for tests

        self.events = [
            create_event(dt.datetime.now(), dt.datetime.now() + dt.timedelta(days=1), "Patrick"),
            create_event(dt.datetime.now(), dt.datetime.now() + dt.timedelta(days=2), "Patrick"),
            create_event(dt.datetime.now(), dt.datetime.now() + dt.timedelta(days=3), "Patrick"),
            create_event(dt.datetime.now(), dt.datetime.now() + dt.timedelta(days=3), "Alexis"),
            create_event(dt.datetime.now(), dt.datetime.now() + dt.timedelta(days=3), "Alexis"),
            create_event(dt.datetime.now(), dt.datetime.now() + dt.timedelta(days=3), "Alexis"),
        ]

        self.catalogue = create_catalogue("Catalogue A", "Patrick", events=self.events)

        save()

    def test_basic_usage(self):
        dcat = create_catalogue("Dynamic Catalogue 'author=Patrick'", "Patrick",
                                predicate=Comparison("==", Field("author"), "Patrick"))

        events = get_events(dcat)
        self.assertListEqual(events, self.events[:3])

        event = create_event(dt.datetime.now(), dt.datetime.now() + dt.timedelta(days=3), "Alexis")
        add_events_to_catalogue(dcat, event)

        events = get_events(dcat)
        self.assertListEqual(events, self.events[0:3] + [event])

        remove_events_from_catalogue(dcat, event)

        events = get_events(dcat)
        self.assertListEqual(events, self.events[0:3])

        # impossible to remove events which are queried with filters
        with self.assertRaises(ValueError):
            remove_events_from_catalogue(dcat, self.events)

        events = get_events(dcat)
        self.assertListEqual(events, self.events[0:3])

        catalogues = get_catalogues()
        self.assertFalse(catalogues[0].is_dynamic())
        self.assertTrue(catalogues[1].is_dynamic())
        self.assertEqual(catalogues[1], dcat)

    def test_predicate_field_is_updatable(self):
        dc = create_catalogue("Dynamic Catalogue", "Patrick",
                              predicate=Comparison("==", Field("author"), "Patrick"))

        self.assertListEqual(get_events(dc), self.events[:3])

        dc.predicate = Comparison("==", Field("author"), "Alexis")

        self.assertListEqual(get_events(dc), self.events[3:])
