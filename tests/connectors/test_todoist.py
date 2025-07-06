"""
Unit tests for connectors/todoist.py
"""

import unittest
from unittest.mock import patch, MagicMock
import connectors.todoist as todoist


class TestTodoistExtraction(unittest.TestCase):
    @patch("connectors.todoist.requests.get")
    def test_fetch_projects(self, mock_get):
        mock_get.return_value = MagicMock(
            status_code=200, json=lambda: [{"id": 1, "name": "Test Project"}]
        )
        projects = todoist.fetch_projects(token="dummy")
        self.assertEqual(len(projects), 1)
        self.assertEqual(projects[0]["name"], "Test Project")

    @patch("connectors.todoist.requests.get")
    def test_fetch_sections(self, mock_get):
        mock_get.return_value = MagicMock(
            status_code=200, json=lambda: [{"id": 2, "name": "Section"}]
        )
        sections = todoist.fetch_sections(token="dummy", project_id=1)
        self.assertEqual(len(sections), 1)
        self.assertEqual(sections[0]["id"], 2)

    @patch("connectors.todoist.requests.get")
    def test_fetch_tasks(self, mock_get):
        mock_get.return_value = MagicMock(
            status_code=200, json=lambda: [{"id": 3, "content": "Task"}]
        )
        tasks = todoist.fetch_tasks(token="dummy", limit=1)
        self.assertEqual(len(tasks), 1)
        self.assertEqual(tasks[0]["content"], "Task")

    @patch("connectors.todoist.requests.get")
    def test_fetch_activity_logs(self, mock_get):
        mock_get.return_value = MagicMock(
            status_code=200, json=lambda: {"events": [{"id": 4, "event": "added"}]}
        )
        logs = todoist.fetch_activity_logs(token="dummy", limit=1)
        self.assertEqual(len(logs), 1)
        self.assertEqual(logs[0]["event"], "added")

    @patch("connectors.todoist.requests.get")
    def test_fetch_projects_error(self, mock_get):
        mock_get.return_value = MagicMock(
            status_code=500,
            raise_for_status=MagicMock(side_effect=Exception("API error")),
        )
        with self.assertRaises(Exception):
            todoist.fetch_projects(token="dummy")

    @patch("connectors.todoist.requests.get")
    def test_fetch_projects_empty(self, mock_get):
        mock_get.return_value = MagicMock(status_code=200, json=lambda: [])
        projects = todoist.fetch_projects(token="dummy")
        self.assertEqual(projects, [])

    @patch("connectors.todoist.requests.get")
    def test_fetch_tasks_malformed(self, mock_get):
        mock_get.return_value = MagicMock(
            status_code=200, json=lambda: {"unexpected": "data"}
        )
        with self.assertRaises(RuntimeError):
            todoist.fetch_tasks(token="dummy", limit=1)

    @patch("connectors.todoist.os.getenv", return_value=None)
    def test_get_token_missing(self, mock_getenv):
        with self.assertRaises(RuntimeError):
            todoist.get_token()


if __name__ == "__main__":
    unittest.main()
