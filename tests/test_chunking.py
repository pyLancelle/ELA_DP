import sys
from pathlib import Path
from datetime import datetime, timedelta
from unittest.mock import MagicMock

# Add src to path
sys.path.append(str(Path(__file__).parent.parent))

from src.connectors.garmin.fetcher import GarminFetcher

def test_chunking():
    # Mock client
    client = MagicMock()
    fetcher = GarminFetcher(client)
    
    # Test endurance_score (chunk_days=28)
    start_date = datetime(2023, 1, 1)
    end_date = datetime(2023, 3, 1) # ~60 days
    
    print("Testing endurance_score chunking...")
    fetcher.fetch_metric("endurance_score", start_date, end_date)
    
    # Verify calls
    # Should be called 3 times:
    # 1. Jan 1 - Jan 29 (28 days) -> actually logic is start + chunk_days. 
    #    min(Jan 1 + 28 days, Mar 1) = Jan 29.
    #    So Jan 1 to Jan 29.
    # 2. Jan 30 - Feb 26.
    # 3. Feb 27 - Mar 1.
    
    calls = client.get_endurance_score.call_args_list
    print(f"Call count: {len(calls)}")
    for i, call in enumerate(calls):
        print(f"Call {i+1}: {call.args}")

    assert len(calls) >= 3
    
    # Test hill_score (chunk_days=28)
    print("\nTesting hill_score chunking...")
    fetcher.fetch_metric("hill_score", start_date, end_date)
    
    calls_hill = client.get_hill_score.call_args_list
    print(f"Call count: {len(calls_hill)}")
    assert len(calls_hill) >= 3

    # Test body_battery (chunk_days=28)
    print("\nTesting body_battery chunking...")
    fetcher.fetch_metric("body_battery", start_date, end_date)
    
    calls_bb = client.get_body_battery.call_args_list
    print(f"Call count: {len(calls_bb)}")
    assert len(calls_bb) >= 3

if __name__ == "__main__":
    test_chunking()
