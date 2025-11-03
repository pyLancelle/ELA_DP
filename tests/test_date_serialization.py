#!/usr/bin/env python3
"""Test date serialization"""
import datetime as dt

# Simulate what happens
timestamp_ms = 1729930028000  # 2024-10-26 08:07:08 UTC

# Transform to date
date_obj = dt.datetime.fromtimestamp(timestamp_ms / 1000.0, tz=dt.timezone.utc).date()
print(f"Date object: {date_obj} (type: {type(date_obj)})")

# Serialize
date_str = date_obj.isoformat()
print(f"Date string: {date_str}")

# Also test datetime
datetime_obj = dt.datetime.fromtimestamp(timestamp_ms / 1000.0, tz=dt.timezone.utc).replace(tzinfo=None)
print(f"\nDatetime object: {datetime_obj} (type: {type(datetime_obj)})")
datetime_str = datetime_obj.isoformat()
print(f"Datetime string: {datetime_str}")

# Test isinstance
print(f"\ndate is instance of datetime? {isinstance(date_obj, dt.datetime)}")
print(f"datetime is instance of date? {isinstance(datetime_obj, dt.date)}")
