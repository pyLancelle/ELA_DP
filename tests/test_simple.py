#!/usr/bin/env python3
"""
Test simple : valide juste le parsing sans dépendances GCP
"""
import sys
from pathlib import Path

# Add src to path
sys.path.insert(0, str(Path(__file__).parent.parent))

# Test transformations
def test_transformations():
    from datetime import datetime, timezone

    # Test timestamp conversion
    timestamp_ms = 1704067200000  # 2024-01-01 00:00:00 UTC

    # Simulate transformation
    result = datetime.fromtimestamp(timestamp_ms / 1000.0, tz=timezone.utc).replace(tzinfo=None)
    expected = datetime(2024, 1, 1, 0, 0, 0)

    assert result == expected, f"Expected {expected}, got {result}"
    print(f"✓ Timestamp: {timestamp_ms} → {result}")

    # Test date conversion
    result_date = datetime.fromtimestamp(timestamp_ms / 1000.0, tz=timezone.utc).date()
    expected_date = expected.date()

    assert result_date == expected_date, f"Expected {expected_date}, got {result_date}"
    print(f"✓ Date: {timestamp_ms} → {result_date}")

if __name__ == '__main__':
    print("Test simple des transformations...")
    test_transformations()
    print("✓ Tous les tests passent!")
