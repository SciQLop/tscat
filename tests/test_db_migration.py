import unittest
import tscat.orm_sqlalchemy
import os
from glob import glob
import tscat
import datetime as dt


class DBMigration(unittest.TestCase):

    def setUp(self) -> None:
        if tscat.base._backend:
            tscat.base._backend.close()
        test_db_file = os.path.join(os.path.dirname(os.path.realpath(__file__)), 'migration-db-test.sqlite')
        tscat.base._backend = tscat.orm_sqlalchemy.Backend(testing=test_db_file)

    def test_existing_event_now_has_rating_field(self):
        existing, = tscat.get_events()
        self.assertEqual(existing.rating, None)
        self.assertGreater(len(glob(f'{tscat.base.backend()._tmp_dir}/*.sqlite.backup')), 0)

    def test_creating_event_with_rating(self):
        tscat.create_event(dt.datetime.now(), dt.datetime.now() + dt.timedelta(days=1), "Patrick", rating=3)

        _, e = tscat.get_events()
        self.assertEqual(e.author, "Patrick")
        self.assertEqual(e.rating, 3)

        self.assertEqual(len(tscat.get_events()), 2)


class DBMigrationBackup(unittest.TestCase):

    def setUp(self):
        if tscat.base._backend:
            tscat.base._backend.close()
        test_db_file = os.path.join(os.path.dirname(os.path.realpath(__file__)), 'migration-db-test.sqlite')
        tscat.base._backend = tscat.orm_sqlalchemy.Backend(testing=test_db_file)
