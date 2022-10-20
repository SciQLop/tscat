import unittest
from ddt import ddt, data, unpack  # type: ignore

import tscat.orm_sqlalchemy
from tscat import create_event, create_catalogue, add_events_to_catalogue, get_events, \
    discard, save, has_unsaved_changes, export_json, import_json, get_catalogues
from tscat.filtering import Comparison, Field
import tscat

import datetime as dt
from random import choice


@ddt
class TestAPIAttributes(unittest.TestCase):
    def setUp(self) -> None:
        tscat._backend = tscat.orm_sqlalchemy.Backend(testing=True)  # create a memory-database for tests

    def test_event_basic_add_get_sequence(self):
        e1 = create_event(dt.datetime.now(), dt.datetime.now() + dt.timedelta(days=1), "Patrick")
        e2 = create_event(dt.datetime.now(), dt.datetime.now() + dt.timedelta(days=1), "Patrick")

        event_list = get_events()
        self.assertListEqual([e1, e2], event_list)

        discard()

        event_list = get_events()
        self.assertListEqual([], event_list)

        e1 = create_event(dt.datetime.now(), dt.datetime.now() + dt.timedelta(days=1), "Patrick")
        e2 = create_event(dt.datetime.now(), dt.datetime.now() + dt.timedelta(days=1), "Patrick")

        save()

        event_list = get_events()
        self.assertListEqual([e1, e2], event_list)

    def test_event_multiple_changes_without_save(self):
        ev = create_event(dt.datetime.now(), dt.datetime.now() + dt.timedelta(days=1), "Patrick")

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
        ev = create_event(dt.datetime.now(), dt.datetime.now() + dt.timedelta(days=1), "Patrick")
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
        ev = create_event(dt.datetime.now(), dt.datetime.now() + dt.timedelta(days=1), "Patrick")
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
        ev = create_event(dt.datetime.now(), dt.datetime.now() + dt.timedelta(days=1), "Patrick",
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
        ev = create_event(dt.datetime.now(), dt.datetime.now() + dt.timedelta(days=1), "Patrick",
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
        ev = create_event(dt.datetime.now(), dt.datetime.now() + dt.timedelta(days=1), "Patrick",
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
        ev = create_event(dt.datetime.now(), dt.datetime.now() + dt.timedelta(days=1), "Patrick",
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
        create_event(dt.datetime.now(), dt.datetime.now() + dt.timedelta(days=1), "Patrick",
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

    def test_create_and_update_string_list_field_and_attribute_of_event(self):
        e = create_event(dt.datetime.now(), dt.datetime.now() + dt.timedelta(days=1),
                         "Patrick",
                         products=["mms2"],
                         str_list=["hello", "world"])
        save()

        self.assertListEqual(get_events(), [e])
        e.products = ["mms1"]
        e.str_list = ["goodbye", "earth"]

        self.assertListEqual(get_events(), [e])

    def test_create_and_update_string_list_field_and_attribute_of_catalogue(self):
        c = create_catalogue("Catalogue Name",
                             "Patrick",
                             tags=["mms2"],
                             str_list=["hello", "world"])
        save()

        self.assertListEqual(get_catalogues(), [c])
        c.tags = ["mms1"]
        c.str_list = ["goodbye", "earth"]

        self.assertListEqual(get_catalogues(), [c])

    def test_entities_fix_keys_and_values_can_be_retrieved(self):
        c = create_catalogue("Catalogue Name", "Patrick", other_attr="asd")

        keys = list(sorted(c.fixed_attributes().keys()))
        self.assertListEqual(sorted(['name', 'uuid', 'author', 'tags', 'predicate']), keys)

        keys = list(sorted(c.variable_attributes().keys()))
        self.assertListEqual(sorted(['other_attr']), keys)

        e = create_event(dt.datetime.now(), dt.datetime.now() + dt.timedelta(days=1), "Patrick", other_attr="asd",
                         other_attr2=123)
        keys = list(sorted(e.fixed_attributes().keys()))
        self.assertListEqual(sorted(['author', 'products', 'start', 'stop', 'tags', 'uuid']), keys)

        keys = list(sorted(e.variable_attributes().keys()))
        self.assertListEqual(sorted(['other_attr', 'other_attr2']), keys)


@ddt
class TestAPIField(unittest.TestCase):
    def setUp(self) -> None:
        tscat._backend = tscat.orm_sqlalchemy.Backend(testing=True)  # create a memory-database for tests

    def test_basic(self):
        ev = create_event(dt.datetime.now(), dt.datetime.now() + dt.timedelta(days=1), "Patrick",
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
        ev = create_event(dt.datetime.now(), dt.datetime.now() + dt.timedelta(days=1), "Patrick")

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
        ev = create_catalogue("Catalogue A", "Patrick")

        with self.assertRaises(expected_exception):
            func(ev)

    def test_unsaved_changes(self):
        create_event(dt.datetime.now(), dt.datetime.now() + dt.timedelta(days=1), "Patrick")

        self.assertTrue(has_unsaved_changes())
        discard()
        self.assertFalse(has_unsaved_changes())

        create_event(dt.datetime.now(), dt.datetime.now() + dt.timedelta(days=1), "Patrick")

        self.assertTrue(has_unsaved_changes())
        save()
        self.assertFalse(has_unsaved_changes())


def generate_event() -> tscat._Event:
    return create_event(
        dt.datetime.now(), dt.datetime.now() + dt.timedelta(days=1),
        choice(["Patrick", "Alexis", "Nicolas"]),
        tag=['tag1', 'tag2'],
        products=['product1', 'mms2'],
        attr_str_list=["custom", "attr", "list"],
        attr_str_list_empty=[])


__cid = 0


def generate_catalogue() -> tscat._Catalogue:
    global __cid
    __cid += 1
    return create_catalogue(
        f"TestCatalogue{__cid}",
        choice(["Patrick", "Alexis", "Nicolas"]),
        tag=['tag1', 'tag2'],
        products=['product1', 'mms2'],
        attr_str_list=["custom", "attr", "list"],
        attr_str_list_empty=[])


class TestImportExport(unittest.TestCase):
    def setUp(self) -> None:
        tscat._backend = tscat.orm_sqlalchemy.Backend(testing=True)  # create a memory-database for tests

    def test_data_is_preserved_with_multiple_export_import_cycles_in_empty_database(self):
        events = [generate_event() for _ in range(10)]
        catalogue = create_catalogue("TestExportImportCatalogue", "Patrick", events=events)

        for _ in range(3):
            export_blob = export_json(catalogue)

            discard()

            self.assertEqual(len(get_events()), 0)
            self.assertEqual(len(get_catalogues()), 0)

            import_json(export_blob)

            self.assertListEqual(events, get_events())
            self.assertListEqual([catalogue], get_catalogues())

    def test_data_is_preserved_when_importing_over_existing_events_and_catalogues_in_database(self):
        events = [generate_event() for _ in range(10)]
        catalogue = create_catalogue("TestExportImportCatalogue", "Patrick", events=events)

        for _ in range(3):
            export_blob = export_json(catalogue)

            import_json(export_blob)

            self.assertListEqual(events, get_events())
            self.assertListEqual([catalogue], get_catalogues())

    def test_importing_a_catalogue_where_all_events_are_already_present(self):
        events = [generate_event() for _ in range(10)]
        catalogue = create_catalogue("TestExportImportCatalogue", "Patrick", events=events)

        export_blob = export_json(catalogue)

        catalogue.remove(permanently=True)

        import_json(export_blob)

        self.assertListEqual(events, get_events())
        self.assertListEqual([catalogue], get_catalogues())

    def test_exception_raised_upon_event_import_with_same_uuid_but_different_attrs(self):
        events = [generate_event() for _ in range(2)]
        catalogue = create_catalogue("TestExportImportCatalogue", "Patrick", events=events)

        export_blob = export_json(catalogue)

        events[0].author = "Someone Else"

        with self.assertRaises(ValueError):
            import_json(export_blob)

    def test_exception_raised_upon_catalogue_import_with_same_uuid_but_different_attrs(self):
        catalogue = create_catalogue("TestExportImportCatalogue", "Patrick")

        export_blob = export_json(catalogue)

        catalogue.author = "Someone Else"

        with self.assertRaises(ValueError):
            import_json(export_blob)

    def test_exception_raised_upon_catalogue_import_with_same_uuid_but_different_events(self):
        events = [generate_event() for _ in range(2)]
        catalogue = create_catalogue("TestExportImportCatalogue", "Patrick", events=events)

        export_blob = export_json(catalogue)

        event = generate_event()

        add_events_to_catalogue(catalogue, event)

        with self.assertRaises(ValueError):
            import_json(export_blob)


class TestTrash(unittest.TestCase):
    def setUp(self) -> None:
        tscat._backend = tscat.orm_sqlalchemy.Backend(testing=True)  # create a memory-database for tests

    def create_events_for_test(self, count: int = 6):
        events = [generate_event() for _ in range(count)]
        for i in range(count):
            if i < 3:
                events[i].author = 'Patrick'
            else:
                events[i].author = 'Alexis'
        return events

    def create_catalogues_for_test(self, count: int = 4):
        catalogues = [generate_catalogue() for _ in range(count)]
        for i in range(count):
            if i < 2:
                catalogues[i].author = "Patrick"
            else:
                catalogues[i].author = "Alexis"
        return catalogues

    def test_event_is_removed_and_cannot_be_retrieved_via_get_events(self):
        events = self.create_events_for_test()

        events[0].remove()
        events_after_remove = get_events()
        self.assertListEqual(events_after_remove, events[1:])

    def test_event_is_removed_and_can_only_be_retrieved_via_get_events_removed_items(self):
        events = self.create_events_for_test()

        events[0].remove()
        events_after_remove = get_events(removed_items=True)
        self.assertListEqual(events_after_remove, events[0:1])

    def test_event_is_removed_and_cannot_be_retrieved_via_catalogue(self):
        events = self.create_events_for_test()
        catalogue = create_catalogue("TestTrashEventCatalogue", "Patrick", events=events)

        events[0].remove()
        events_after_remove = get_events(catalogue)
        self.assertListEqual(events_after_remove, events[1:])

    def test_event_is_removed_and_cannot_be_retrieved_via_predicate(self):
        events = self.create_events_for_test()

        events[0].remove()
        events_after_remove = get_events(Comparison('==', Field('author'), 'Patrick'))
        self.assertListEqual(events_after_remove, events[1:3])

    def test_event_is_removed_and_cannot_be_retrieved_via_dynamic_catalogue(self):
        events = self.create_events_for_test()
        catalogue = create_catalogue("TestTrashEventCatalogue", "Patrick",
                                     predicate=Comparison('==', Field('author'), 'Patrick'),
                                     events=events[3:])

        events[0].remove()
        events_after_remove = get_events(catalogue)
        self.assertListEqual(events_after_remove, events[1:])

    def test_catalogue_is_removed_and_cannot_be_retrieved_via_get_catalogue(self):
        catalogues = [generate_catalogue() for _ in range(2)]
        catalogues[0].remove()

        catalogues_after_remove = get_catalogues()
        self.assertListEqual(catalogues_after_remove, catalogues[1:])

    def test_catalogue_is_removed_and_cannot_be_retrieved_via_get_catalogue_with_predicate(self):
        catalogues = self.create_catalogues_for_test()
        catalogues[0].remove()

        catalogues_after_remove = get_catalogues(Comparison('==', Field('author'), 'Patrick'))
        self.assertListEqual(catalogues_after_remove, catalogues[1:2])

    def test_event_is_removed_and_restored_then_retrieved_via_get_events(self):
        events = self.create_events_for_test()

        events[0].remove()
        events_after_remove = get_events()
        self.assertListEqual(events_after_remove, events[1:])

        events[0].restore()
        events_after_restore = get_events()
        self.assertListEqual(events_after_restore, events)

    def test_event_is_removed_and_restored_then_retrieved_via_get_events_with_catalogue(self):
        events = self.create_events_for_test()
        catalogue = generate_catalogue()
        add_events_to_catalogue(catalogue, events)

        events[0].remove()
        events_after_remove = get_events(catalogue)
        self.assertListEqual(events_after_remove, events[1:])

        events[0].restore()
        events_after_restore = get_events()
        self.assertListEqual(events_after_restore, events)

    def test_catalogue_is_removed_and_restored_and_retrieved_via_get_catalogue(self):
        catalogues = [generate_catalogue() for _ in range(2)]
        catalogues[0].remove()

        catalogues_after_remove = get_catalogues()
        self.assertListEqual(catalogues_after_remove, catalogues[1:])

        catalogues[0].restore()
        catalogues_after_restore = get_catalogues()
        self.assertListEqual(catalogues_after_restore, catalogues)

    def test_catalogue_is_removed_and_restored_and_retrieved_via_get_catalogue_with_predicate(self):
        catalogues = self.create_catalogues_for_test()

        catalogues[0].remove()

        catalogues_after_remove = get_catalogues(Comparison('==', Field('author'), 'Patrick'))
        self.assertListEqual(catalogues_after_remove, catalogues[1:2])

        catalogues[0].restore()
        catalogues_after_restore = get_catalogues(Comparison('==', Field('author'), 'Patrick'))
        self.assertListEqual(catalogues_after_restore, catalogues[:2])

    def test_get_removed_catalogues_before_and_after_remove_and_after_restore(self):
        catalogues = self.create_catalogues_for_test()

        removed_catalogues = get_catalogues(removed_items=True)
        self.assertListEqual(removed_catalogues, [])

        catalogues[0].remove()
        self.assertTrue(catalogues[0].is_removed())

        removed_catalogues = get_catalogues(removed_items=True)
        self.assertListEqual(removed_catalogues, catalogues[0:1])

        all_catalogues = get_catalogues()
        self.assertListEqual(all_catalogues, catalogues[1:])

        catalogues[0].restore()
        removed_catalogues = get_catalogues(removed_items=True)
        self.assertListEqual(removed_catalogues, [])

        all_catalogues = get_catalogues()
        self.assertListEqual(all_catalogues, catalogues)

    def test_get_removed_events_from_a_catalogue(self):
        cat, = self.create_catalogues_for_test(1)
        ev = self.create_events_for_test(3)

        add_events_to_catalogue(cat, ev)

        ev[0].remove()
        ev[1].remove()

        self.assertListEqual(get_events(cat), ev[2:3])
        self.assertListEqual(get_events(cat, removed_items=True), ev[0:2])

    def test_removing_catalogue_does_not_remove_events(self):
        cat, = self.create_catalogues_for_test(1)
        ev = self.create_events_for_test(3)

        add_events_to_catalogue(cat, ev)

        cat.remove()

        self.assertTrue(cat.is_removed())

        self.assertListEqual(get_events(cat), ev)
        self.assertListEqual(get_events(cat, removed_items=True), [])

    def test_remove_catalogue_permanently(self):
        cat, = self.create_catalogues_for_test(1)
        self.assertListEqual(get_catalogues(), [cat])

        cat.remove(permanently=True)

        self.assertListEqual(get_catalogues(), [])
        self.assertListEqual(get_catalogues(removed_items=True), [])

    def test_remove_event_permanently(self):
        ev, = self.create_events_for_test(1)
        self.assertListEqual(get_events(), [ev])

        ev.remove(permanently=True)

        self.assertListEqual(get_events(), [])
        self.assertListEqual(get_events(removed_items=True), [])

    def test_raise_if_permanently_removed_catalogue_used_with_get_event(self):
        cat, = self.create_catalogues_for_test(1)
        ev = self.create_events_for_test(3)

        add_events_to_catalogue(cat, ev)

        self.assertListEqual(get_events(cat), ev)

        cat.remove(permanently=True)

        with self.assertRaises(ValueError):
            get_events(cat)
        self.assertListEqual(get_events(), ev)

    def test_raise_if_permanently_removed_event_is_added_to_catalogue(self):
        cat, = self.create_catalogues_for_test(1)
        ev, = self.create_events_for_test(1)

        ev.remove(permanently=True)

        with self.assertRaises(ValueError):
            add_events_to_catalogue(cat, ev)

    def test_permanently_remove_event_from_catalogue_makes_it_disappear(self):
        cat, = self.create_catalogues_for_test(1)
        ev = self.create_events_for_test(2)

        add_events_to_catalogue(cat, ev)

        self.assertListEqual(get_events(cat), ev)
        self.assertListEqual(get_events(), ev)

        ev[0].remove(permanently=True)

        self.assertListEqual(get_events(cat), ev[1:])
        self.assertListEqual(get_events(), ev[1:])
