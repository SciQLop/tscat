import datetime as dt
import re
import unittest

from ddt import data, ddt, unpack  # type: ignore

import tscat.orm_sqlalchemy
from tscat import create_event
from tscat.filtering import Field, Comparison


@ddt
class TestEvent(unittest.TestCase):
    def setUp(self) -> None:
        tscat.base._backend = tscat.orm_sqlalchemy.Backend(testing=True)  # create a memory-database for tests

    @data(
        (dt.datetime.now(), dt.datetime.now() + dt.timedelta(days=1), "Patrick", None, {}),
        (dt.datetime.now(), dt.datetime.now() + dt.timedelta(days=1), "", None, {}),
        (dt.datetime.now(), dt.datetime.now() + dt.timedelta(days=1), "Patrick", None, {'field': 2}),
        (dt.datetime.now(), dt.datetime.now() + dt.timedelta(days=1), "Patrick", None, {'field_with_underscores': 2}),
        (dt.datetime.now(), dt.datetime.now() + dt.timedelta(days=1), "Patrick", None, {'field': 2.0}),
        (dt.datetime.now(), dt.datetime.now() + dt.timedelta(days=1), "Patrick", None, {'field': "2"}),
        (dt.datetime.now(), dt.datetime.now() + dt.timedelta(days=1), "Patrick", None, {'field': True}),
        (dt.datetime.now(), dt.datetime.now() + dt.timedelta(days=1), "Patrick", None, {'field': dt.datetime.now()}),
        (dt.datetime.now(), dt.datetime.now() + dt.timedelta(days=1), "Patrick", None, {'field': 2}),
        (dt.datetime.now(), dt.datetime.now() + dt.timedelta(days=1), "Patrick",
         '7b732d98-da74-11eb-89a0-f3d357f13cae', {'field': 2}),

        (dt.datetime.now(), dt.datetime.now() + dt.timedelta(days=1), "Patrick", None, {'field': 2, 'Field': 3}),
        (dt.datetime.now(), dt.datetime.now() + dt.timedelta(days=1), "Patrick",
         '7b732d98-da74-11eb-89a0-f3d357f13cae',
         {'field': 2, 'field2': 3.14, 'field3': "str", 'field4': True, 'field5': dt.datetime.now(), }),
        (dt.datetime.now(), dt.datetime.now() + dt.timedelta(days=1), "", None, {}, ["tag1", "tag2"],
         ["productA", "productB"]),
        (dt.datetime.now(), dt.datetime.now() + dt.timedelta(days=1), "", None, {}, ["tag1", "tag3"],
         ["productA", "productC"]),
        (dt.datetime(2000, 1, 1), dt.datetime(2000, 1, 1), "Patrick", None, {}, ["Zero_duration_event"]),
    )
    @unpack
    def test_constructor_various_combinations_all_ok(self, start, stop, author, uuid, attrs, tags=[], products=[]):
        e = create_event(start, stop, author, uuid, tags, products, **attrs)

        self.assertEqual(e.start, start)
        self.assertEqual(e.stop, stop)
        self.assertEqual(e.author, author)
        if uuid:
            self.assertEqual(e.uuid, uuid)

        for k, v in attrs.items():
            self.assertEqual(e.__getattribute__(k), v)

        attr_repr = ', '.join(f'{k}={v}' for k, v in attrs.items())
        tags = re.escape(str(tags))
        products = re.escape(str(products))
        r = r'^Event\(start=.*, stop=.*, author=' + author + r', uuid=[0-9a-f-]{36}, tags=' + tags \
            + r', products=' + products + r', rating=None\) attributes\(' + attr_repr + r'\)$'

        self.assertRegex(f'{e}', r)

    @data(
        (dt.datetime.now() + dt.timedelta(days=1), dt.datetime.now(), "", None, {}),
        (dt.datetime.now(), dt.datetime.now() + dt.timedelta(days=1), "", "invalid_uuid", {}),
        (dt.datetime.now(), dt.datetime.now() + dt.timedelta(days=1), "", None, {"_invalid": 2}),
        (dt.datetime.now(), dt.datetime.now() + dt.timedelta(days=1), "", None, {"'invalid'": 2}),
        (dt.datetime.now(), dt.datetime.now() + dt.timedelta(days=1), "", None, {"'invalid": 2}),
        (dt.datetime.now(), dt.datetime.now() + dt.timedelta(days=1), "", None, {'"invalid"': 2}),
        (dt.datetime.now(), dt.datetime.now() + dt.timedelta(days=1), "", None, {"i\nvalid": 2}),
        (dt.datetime.now(), dt.datetime.now() + dt.timedelta(days=1), "", None, {"invalid\\\'": 2}),
        (dt.datetime.now(), dt.datetime.now() + dt.timedelta(days=1), "", None, {}, ["tags", 123]),
        (dt.datetime.now(), dt.datetime.now() + dt.timedelta(days=1), "", None, {}, ["tags", dict()]),
        (dt.datetime.now(), dt.datetime.now() + dt.timedelta(days=1), "", None, {}, [], ["test", 1234]),
    )
    @unpack
    def test_constructor_various_combinations_value_error(self, start, stop, author, uuid, attrs, tags=[],
                                                          products=[]):
        with self.assertRaises(ValueError):
            assert create_event(start, stop, author, uuid, tags, products, **attrs)

    def test_unequal_events(self):
        t1, t2 = dt.datetime.now(), dt.datetime.now() + dt.timedelta(days=1)

        a, b = create_event(t1, t2, "Patrick"), create_event(t1, t2, "Patrick"),
        self.assertNotEqual(a, b)

        # two Events are never equal because of the UUID
        a, b = create_event(t1, t2, "Patrick"), create_event(t1, t2, "Patrick"),
        self.assertNotEqual(a, b)

    def test_constructor_with_dynamic_attribute_manual_access(self):
        dt_val = dt.datetime.now()
        e = create_event(dt_val + dt.timedelta(days=1), dt_val + dt.timedelta(days=2), "Patrick",
                         '7b732d98-da74-11eb-89a0-f3d357f13cae',
                         field_int=100, field_float=1.234, field_str="string-test", field_bool=True, field_dt=dt_val)

        self.assertEqual(e.start, dt_val + dt.timedelta(days=1))
        self.assertEqual(e.stop, dt_val + dt.timedelta(days=2))
        self.assertEqual(e.author, "Patrick")
        self.assertEqual(e.uuid, '7b732d98-da74-11eb-89a0-f3d357f13cae')

        self.assertEqual(e.field_int, 100)
        self.assertEqual(e.field_float, 1.234)
        self.assertEqual(e.field_str, "string-test")
        self.assertEqual(e.field_bool, True)
        self.assertEqual(e.field_dt, dt_val)

    @data(
        (None,),
        (1,),
        (5,),
    )
    @unpack
    def test_event_valid_rating_values(self, value):
        t1, t2 = dt.datetime.now(), dt.datetime.now() + dt.timedelta(days=1)

        e = create_event(t1, t2, "Patrick")
        self.assertEqual(e.rating, None)

        e.rating = value
        self.assertEqual(e.rating, value)

    @data(
        (-1,),
        (0,),
        (1.5,),
        (11,),
    )
    @unpack
    def test_event_invalid_rating_values(self, value):
        t1, t2 = dt.datetime.now(), dt.datetime.now() + dt.timedelta(days=1)

        e = create_event(t1, t2, "Patrick")
        self.assertEqual(e.rating, None)

        with self.assertRaises(ValueError):
            e.rating = value

    def test_is_assigned_true_when_added_to_catalogue_and_fetched_with_get_event(self):
        t1, t2 = dt.datetime.now(), dt.datetime.now() + dt.timedelta(days=1)

        e = create_event(t1, t2, "Patrick")
        c = tscat.create_catalogue("test", "Patrick")
        tscat.add_events_to_catalogue(c, [e])

        _, info = tscat.get_events(c)
        self.assertTrue(info[0].assigned)

    def test_is_assigned_true_for_event_assigned_and_false_for_filtered_ones(self):
        t1, t2 = dt.datetime.now(), dt.datetime.now() + dt.timedelta(days=1)

        a, b, c = (create_event(t1, t2, "Patrick"), create_event(t1, t2, "Nicolas"),
                   create_event(t1, t2, "Alexis"))

        c = tscat.create_catalogue("test", "Patrick",
                                   predicate=Comparison("==", Field('author'), "Alexis"))
        tscat.add_events_to_catalogue(c, [a])

        es, info = tscat.get_events(c)
        self.assertEqual(len(es), 2)
        self.assertTrue(info[0].assigned)
        self.assertFalse(info[1].assigned)
