import unittest
from datetime import datetime, timedelta, timezone
from harvest.templating import filters


class TestFilters(unittest.TestCase):
    def test_list_filters(self):
        filter_methods = filters.list_filters()
        self.assertIsInstance(filter_methods, dict)
        self.assertIn('datetime_since', filter_methods)
        self.assertIn('datetime_until', filter_methods)
        self.assertIn('datetime_now', filter_methods)

    def test_parse_datetime(self):
        date_str = '2022-01-01T00:00:00'
        date_obj = datetime(2022, 1, 1, tzinfo=timezone.utc)
        self.assertEqual(filters.parse_datetime(date_str), date_obj)
        self.assertEqual(filters.parse_datetime(date_obj), date_obj)
        self.assertIsNone(filters.parse_datetime('invalid date'))

    def test_filter_datetime_since(self):
        reference_date = datetime(2022, 1, 1, tzinfo=timezone.utc)
        expected_date = datetime(2021, 12, 31, tzinfo=timezone.utc)
        self.assertEqual(filters.filter_datetime_since(reference_date, days=1), expected_date)
        self.assertEqual(filters.filter_datetime_since(reference_date.isoformat(), days=1), expected_date)
        self.assertEqual(filters.filter_datetime_since(reference_date, result_as_string=True, days=1), expected_date.isoformat())

    def test_filter_datetime_until(self):
        reference_date = datetime(2022, 1, 1, tzinfo=timezone.utc)
        expected_date = datetime(2022, 1, 2, tzinfo=timezone.utc)
        self.assertEqual(filters.filter_datetime_until(reference_date, days=1), expected_date)
        self.assertEqual(filters.filter_datetime_until(reference_date.isoformat(), days=1), expected_date)
        self.assertEqual(filters.filter_datetime_until(reference_date, result_as_string=True, days=1), expected_date.isoformat())

    def test_filter_datetime_now(self):
        from harvest.templating.filters import filter_datetime_now

        # Test that the function returns a timezone aware datetime object by default
        self.assertIsInstance(filter_datetime_now(), datetime)
        self.assertIsNotNone(filter_datetime_now().tzinfo)

        # Test that the function returns a naive datetime object if result_tz_aware is False
        self.assertIsInstance(filter_datetime_now(result_tz_aware=False), datetime)
        self.assertIsNone(filter_datetime_now(result_tz_aware=False).tzinfo)

        # Test that the function returns a Unix timestamp if as_epoc is True
        self.assertIsInstance(filter_datetime_now(as_epoc=True), float)


if __name__ == '__main__':
    unittest.main()
