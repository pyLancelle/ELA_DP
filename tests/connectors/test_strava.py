"""
Unit tests for connectors/strava.py
"""
import unittest
from unittest.mock import patch, MagicMock
import connectors.strava as strava

class TestStravaExtraction(unittest.TestCase):
    @patch("connectors.strava.requests.get")
    def test_fetch_activities(self, mock_get):
        mock_get.return_value = MagicMock(status_code=200, json=lambda: [{"id": 1, "name": "Test Activity"}])
        activities = strava.fetch_activities(token="dummy", per_page=1, max_pages=1)
        self.assertEqual(len(activities), 1)
        self.assertEqual(activities[0]["name"], "Test Activity")

    @patch("connectors.strava.requests.get")
    def test_fetch_activity_comments(self, mock_get):
        mock_get.return_value = MagicMock(status_code=200, json=lambda: [{"id": 2, "text": "Nice!"}])
        comments = strava.fetch_activity_comments(token="dummy", activity_id=1)
        self.assertEqual(len(comments), 1)
        self.assertEqual(comments[0]["text"], "Nice!")

    @patch("connectors.strava.requests.get")
    def test_fetch_activity_kudos(self, mock_get):
        mock_get.return_value = MagicMock(status_code=200, json=lambda: [{"id": 3, "firstname": "John"}])
        kudos = strava.fetch_activity_kudos(token="dummy", activity_id=1)
        self.assertEqual(len(kudos), 1)
        self.assertEqual(kudos[0]["firstname"], "John")

    @patch("connectors.strava.requests.get")
    def test_fetch_activities_error(self, mock_get):
        mock_get.return_value = MagicMock(status_code=500, raise_for_status=MagicMock(side_effect=Exception("API error")))
        with self.assertRaises(Exception):
            strava.fetch_activities(token="dummy", per_page=1, max_pages=1)

    @patch("connectors.strava.requests.get")
    def test_fetch_activities_empty(self, mock_get):
        mock_get.return_value = MagicMock(status_code=200, json=lambda: [])
        activities = strava.fetch_activities(token="dummy", per_page=1, max_pages=1)
        self.assertEqual(activities, [])

    def test_get_token_missing(self):
        import os
        from importlib import reload
        os.environ.pop("STRAVA_API_TOKEN", None)
        reload(strava)
        with self.assertRaises(RuntimeError):
            strava.get_token()

if __name__ == "__main__":
    unittest.main()
