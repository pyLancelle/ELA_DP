import pytest
from unittest.mock import MagicMock, patch
from pathlib import Path
import sys

# Add project root to path
project_root = Path(__file__).parent.parent.parent.parent
sys.path.append(str(project_root))

from src.connectors.garmin import __main__ as garmin_main

@pytest.fixture
def mock_garmin_client():
    client = MagicMock()
    # Mock basic data returns
    client.get_sleep_data.return_value = {"dailySleepDTO": {"id": 123, "sleepTimeSeconds": 28800}}
    client.get_steps_data.return_value = [{"steps": 10000}]
    client.get_activities_by_date.return_value = [{"activityId": 999, "activityName": "Run"}]
    return client

@pytest.fixture
def mock_args(tmp_path):
    args = MagicMock()
    args.days = 1
    args.output_dir = tmp_path
    args.env = Path(".env")
    args.log_level = "DEBUG"
    args.timezone = "UTC"
    args.data_types = ["sleep", "steps", "activities"]
    args.no_withings_sync = True
    args.user_height = 1.80
    args.withings_dedupe_hours = 24
    return args

def test_refactored_execution_flow(mock_garmin_client, mock_args):
    """
    Test that the NEW modular execution flow works exactly as expected.
    """
    
    # Mock dependencies in the NEW module paths
    with patch('src.connectors.garmin.__main__.parse_args', return_value=mock_args), \
         patch('src.connectors.garmin.client.Garmin', return_value=mock_garmin_client), \
         patch('src.connectors.garmin.__main__.load_env'), \
         patch('src.connectors.garmin.__main__.validate_env_vars', return_value={"GARMIN_USERNAME": "u", "GARMIN_PASSWORD": "p"}), \
         patch('src.connectors.garmin.__main__.write_jsonl') as mock_write:
        
        # Run main from the new entry point
        garmin_main.main()
        
        # Verify client calls
        assert mock_garmin_client.login.called
        assert mock_garmin_client.get_sleep_data.called
        assert mock_garmin_client.get_steps_data.called
        assert mock_garmin_client.get_activities_by_date.called
        
        # Verify write_jsonl was called 3 times
        assert mock_write.call_count == 3
        
        # Verify data structure
        sleep_call_args = mock_write.call_args_list[0]
        data, output_path = sleep_call_args[0]
        
        # Should still be 2 items (today + yesterday)
        assert len(data) == 2
        
        # Verify data normalization (our new fetcher adds 'data_type' and 'date')
        assert "date" in data[0]
        assert "data_type" in data[0]
        assert data[0]["data_type"] == "sleep"
