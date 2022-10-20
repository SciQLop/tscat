import unittest
from ddt import ddt, data, unpack  # type: ignore

import tscat.orm_sqlalchemy
from tscat import create_event

import datetime as dt
import re


@ddt
class TestEvent(unittest.TestCase):
    def setUp(self) -> None:
        tscat._backend = tscat.orm_sqlalchemy.Backend(testing=True)  # create a memory-database for tests

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
            + r', products=' + products + r'\) attributes\(' + attr_repr + r'\)$'

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
