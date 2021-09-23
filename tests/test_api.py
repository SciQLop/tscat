#!/usr/bin/env python

import unittest
from ddt import ddt, data, unpack

import tscat.orm_sqlalchemy
from tscat import Event, Catalogue, get_events, discard, save, has_unsaved_changes

import datetime as dt


@ddt
class TestAPIAttributes(unittest.TestCase):
    def setUp(self) -> None:
        tscat._backend = tscat.orm_sqlalchemy.Backend(testing=True)  # create a memory-database for tests

    def test_event_basic_add_get_sequence(self):
        e1 = Event(dt.datetime.now(), dt.datetime.now() + dt.timedelta(days=1), "Patrick")
        e2 = Event(dt.datetime.now(), dt.datetime.now() + dt.timedelta(days=1), "Patrick")

        event_list = get_events()
        self.assertListEqual([e1, e2], event_list)

        discard()

        event_list = get_events()
        self.assertListEqual([], event_list)

        e1 = Event(dt.datetime.now(), dt.datetime.now() + dt.timedelta(days=1), "Patrick")
        e2 = Event(dt.datetime.now(), dt.datetime.now() + dt.timedelta(days=1), "Patrick")

        save()

        event_list = get_events()
        self.assertListEqual([e1, e2], event_list)

    def test_event_multiple_changes_without_save(self):
        ev = Event(dt.datetime.now(), dt.datetime.now() + dt.timedelta(days=1), "Patrick")

        ev_db, = get_events()
        self.assertEqual(ev, ev_db)

        ev.attr = 12

        ev_db, = get_events()
        self.assertEqual(ev, ev_db)
        self.assertEqual(ev.attr, 12)

        del ev.attr

        ev_db, = get_events()
        self.assertEqual(ev, ev_db)
        self.assertFalse(hasattr(ev, 'attr'))

        ev.attr = True

        ev_db, = get_events()
        self.assertEqual(ev, ev_db)
        self.assertTrue(ev.attr)

    def test_event_add_attribute_discard(self):
        ev = Event(dt.datetime.now(), dt.datetime.now() + dt.timedelta(days=1), "Patrick")
        save()

        ev_db, = get_events()
        self.assertEqual(ev, ev_db)

        ev.new_attr = 'value'

        ev_db, = get_events()
        self.assertEqual(ev, ev_db)
        self.assertTrue(hasattr(ev_db, 'new_attr'))
        self.assertEqual(ev_db.new_attr, 'value')

        discard()  # from here on ev is out of sync with the backend-storage and contains an invalid backend_entity

        ev_db, = get_events()
        self.assertFalse(hasattr(ev_db, 'new_value'))

    def test_event_add_attribute_save(self):
        ev = Event(dt.datetime.now(), dt.datetime.now() + dt.timedelta(days=1), "Patrick")
        save()

        ev_db, = get_events()
        self.assertEqual(ev, ev_db)

        ev.new_attr = 'value'

        save()

        ev_db, = get_events()
        self.assertEqual(ev, ev_db)
        self.assertTrue(hasattr(ev_db, 'new_attr'))
        self.assertEqual(ev.new_attr, 'value')

    def test_event_modify_attribute_discard(self):
        ev = Event(dt.datetime.now(), dt.datetime.now() + dt.timedelta(days=1), "Patrick",
                   a_str="hello", a_int=10, a_bool=True)
        save()

        ev.a_str = 'world'
        ev.a_bool = False
        ev.a_int = 11

        ev_db, = get_events()
        self.assertEqual(ev, ev_db)
        self.assertEqual(ev_db.a_str, 'world')
        self.assertFalse(ev_db.a_bool)
        self.assertEqual(ev_db.a_int, 11)

        discard()  # from here on ev is out of sync with the backend-storage and backend_entity is still valid

        ev_db, = get_events()
        self.assertEqual(ev_db.a_str, 'hello')
        self.assertTrue(ev_db.a_bool)
        self.assertEqual(ev_db.a_int, 10)

    def test_event_modify_attribute_save(self):
        ev = Event(dt.datetime.now(), dt.datetime.now() + dt.timedelta(days=1), "Patrick",
                   a_str="hello", a_int=10, a_bool=True)
        save()

        ev.a_str = 'world'
        ev.a_bool = False
        ev.a_int = 11

        save()

        ev_db, = get_events()
        self.assertEqual(ev_db.a_str, 'world')
        self.assertFalse(ev_db.a_bool)
        self.assertEqual(ev_db.a_int, 11)

    def test_event_delete_attribute_save(self):
        ev = Event(dt.datetime.now(), dt.datetime.now() + dt.timedelta(days=1), "Patrick",
                   a_str="hello", a_int=10, a_bool=True)
        save()

        del ev.a_str

        ev_db, = get_events()
        self.assertFalse(hasattr(ev_db, 'a_str'))
        self.assertTrue(ev_db.a_bool)
        self.assertEqual(ev_db.a_int, 10)

        save()

        ev_db, = get_events()
        self.assertFalse(hasattr(ev_db, 'a_str'))
        self.assertTrue(ev_db.a_bool)
        self.assertEqual(ev_db.a_int, 10)

    def test_event_delete_attribute_discard(self):
        ev = Event(dt.datetime.now(), dt.datetime.now() + dt.timedelta(days=1), "Patrick",
                   a_str="hello", a_int=10, a_bool=True)
        save()

        del ev.a_str

        ev_db, = get_events()
        self.assertFalse(hasattr(ev_db, 'a_str'))
        self.assertTrue(ev_db.a_bool)
        self.assertEqual(ev_db.a_int, 10)

        discard()

        ev_db, = get_events()
        self.assertEqual(ev_db.a_str, 'hello')
        self.assertTrue(ev_db.a_bool)
        self.assertEqual(ev_db.a_int, 10)

    def test_event_mixed_actions_on_attribute(self):
        Event(dt.datetime.now(), dt.datetime.now() + dt.timedelta(days=1), "Patrick",
              a_str="hello", a_int=10, a_bool=True)
        save()

        ev, = get_events()
        self.assertTrue(hasattr(ev, 'a_str'))
        self.assertTrue(ev.a_bool)
        self.assertEqual(ev.a_int, 10)

        del ev.a_str
        discard()

        ev, = get_events()
        self.assertTrue(hasattr(ev, 'a_str'))

        del ev.a_str
        save()

        ev, = get_events()
        self.assertFalse(hasattr(ev, 'a_str'))

        ev.a_str = 'world'

        save()

        ev, = get_events()
        self.assertTrue(hasattr(ev, 'a_str'))
        self.assertEqual(ev.a_str, 'world')
        self.assertTrue(ev.a_bool)
        self.assertEqual(ev.a_int, 10)

        ev.a_int = 11
        discard()

        self.assertEqual(ev.a_int, 11)
        ev, = get_events()
        self.assertEqual(ev.a_int, 10)

        ev.a_int = 12
        save()

        ev, = get_events()
        self.assertEqual(ev.a_int, 12)


@ddt
class TestAPIField(unittest.TestCase):
    def setUp(self) -> None:
        tscat._backend = tscat.orm_sqlalchemy.Backend(testing=True)  # create a memory-database for tests

    def test_basic(self):
        ev = Event(dt.datetime.now(), dt.datetime.now() + dt.timedelta(days=1), "Patrick",
                   a_str="hello", a_int=10, a_bool=True)

        ev.stop = dt.datetime.now() + dt.timedelta(days=2)
        ev.start = dt.datetime.now() + dt.timedelta(days=1)

        ev_db, = get_events()
        self.assertEqual(ev_db, ev)

    @data(
        (ValueError, lambda x: setattr(x, 'start', dt.datetime.now() + dt.timedelta(days=2))),
        (ValueError, lambda x: setattr(x, 'stop', dt.datetime.now() - dt.timedelta(days=2))),
        (ValueError, lambda x: setattr(x, 'uuid', 'invalid-uuid')),
        (IndexError, lambda x: delattr(x, 'start')),
        (IndexError, lambda x: delattr(x, 'stop')),
        (IndexError, lambda x: delattr(x, 'author')),
        (IndexError, lambda x: delattr(x, 'uuid')),
    )
    @unpack
    def test_mandatory_attrs_exceptions_on_event(self, expected_exception, func):
        ev = Event(dt.datetime.now(), dt.datetime.now() + dt.timedelta(days=1), "Patrick")

        with self.assertRaises(expected_exception):
            func(ev)

    @data(
        (ValueError, lambda x: setattr(x, 'name', '')),
        (ValueError, lambda x: setattr(x, 'uuid', 'invalid-uuid')),
        (IndexError, lambda x: delattr(x, 'name')),
        (IndexError, lambda x: delattr(x, 'author')),
        (IndexError, lambda x: delattr(x, 'uuid')),
    )
    @unpack
    def test_mandatory_attrs_exceptions_on_catalogue(self, expected_exception, func):
        ev = Catalogue("Catalogue A", "Patrick")

        with self.assertRaises(expected_exception):
            func(ev)

    def test_unsaved_changes(self):
        Event(dt.datetime.now(), dt.datetime.now() + dt.timedelta(days=1), "Patrick")

        self.assertTrue(has_unsaved_changes())
        discard()
        self.assertFalse(has_unsaved_changes())

        Event(dt.datetime.now(), dt.datetime.now() + dt.timedelta(days=1), "Patrick")

        self.assertTrue(has_unsaved_changes())
        save()
        self.assertFalse(has_unsaved_changes())
