#!/usr/bin/env python

import datetime as dt
import unittest

import tscat.orm_sqlalchemy
from tscat.orm_sqlalchemy.orm import Event


class TestORMCustomCodes(unittest.TestCase):
    def setUp(self) -> None:
        tscat._backend = tscat.orm_sqlalchemy.Backend(testing=True)  # create a memory-database for tests

    def test_additional_functions_on_proxied_dict_mixin(self):
        e = Event(dt.datetime.now(), dt.datetime.now() + dt.timedelta(days=1), 'Patrick', 'uuid', [], [])

        # __setitem__
        e['attr'] = ["str1", "str2"]

        # __len__
        self.assertEqual(len(e), 1)

        e['attr2'] = 123
        # __getitem__
        self.assertEqual(e['attr2'], 123)
        self.assertEqual(len(e), 2)

        # __contains__
        self.assertTrue('attr' in e)
        self.assertFalse('attr3' in e)

        # __iter__
        self.assertListEqual([k for k in e], ['attr', 'attr2'])
