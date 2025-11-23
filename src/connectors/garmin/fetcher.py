"""
Garmin Generic Fetcher
----------------------
Core logic for fetching data from Garmin Connect using a configuration-driven approach.
"""
import logging
import time
from datetime import datetime, timedelta
from typing import List, Dict, Any, Optional, Callable

from .config import METRICS_CONFIG
from .utils import flatten_nested_arrays

class GarminFetcher:
    """Generic fetcher for Garmin Connect data."""
    
    def __init__(self, client: Any):
        self.client = client
        
    def fetch_metric(
        self, 
        metric_name: str, 
        start_date: datetime, 
        end_date: datetime
    ) -> List[Dict[str, Any]]:
        """
        Fetch a specific metric based on its configuration.
        
        Args:
            metric_name: Name of the metric (must be in METRICS_CONFIG)
            start_date: Start date for fetching
            end_date: End date for fetching
            
        Returns:
            List of data dictionaries
        """
        if metric_name not in METRICS_CONFIG:
            logging.warning(f"Unknown metric: {metric_name}")
            return []
            
        config = METRICS_CONFIG[metric_name]
        method_name = config["method"]
        fetch_type = config["type"]
        
        # Check if client has the method
        if not hasattr(self.client, method_name):
            logging.error(f"Client missing method: {method_name}")
            return []
            
        method = getattr(self.client, method_name)
        
        logging.info(f"📊 Fetching {metric_name} data...")
        
        if fetch_type == "daily":
            return self._fetch_daily(method, metric_name, start_date, end_date)
        elif fetch_type == "range":
            return self._fetch_range(method, metric_name, start_date, end_date)
        elif fetch_type == "simple":
            return self._fetch_simple(method, metric_name)
        elif fetch_type == "activity_detail":
            return self._fetch_activity_details(self.client, start_date, end_date)
        elif fetch_type == "activity_subdata":
            return self._fetch_activity_subdata(self.client, metric_name, method_name, start_date, end_date)
        else:
            logging.warning(f"Unknown fetch type {fetch_type} for {metric_name}")
            return []

    def _fetch_daily(
        self, 
        method: Callable, 
        metric_name: str, 
        start_date: datetime, 
        end_date: datetime
    ) -> List[Dict[str, Any]]:
        """Fetch data day by day."""
        results = []
        current_date = start_date
        
        while current_date <= end_date:
            date_str = current_date.strftime("%Y-%m-%d")
            try:
                data = method(date_str)
                if data:
                    # Transform nested arrays first
                    data = flatten_nested_arrays(data, path=f"{metric_name}.{date_str}")
                    
                    # Normalize data structure
                    if isinstance(data, list):
                        for item in data:
                            if isinstance(item, dict):
                                item["date"] = date_str
                                item["data_type"] = metric_name
                                results.append(item)
                    elif isinstance(data, dict):
                        data["date"] = date_str
                        data["data_type"] = metric_name
                        results.append(data)
                    else:
                        results.append({
                            "date": date_str, 
                            "data": data, 
                            "data_type": metric_name
                        })
                
                time.sleep(0.3) # Rate limiting
                
            except Exception as e:
                logging.warning(f"Error fetching {metric_name} for {date_str}: {e}")
                
            current_date += timedelta(days=1)
            
        logging.info(f"Fetched {metric_name} for {len(results)} entries")
        return results

    def _fetch_range(
        self, 
        method: Callable, 
        metric_name: str, 
        start_date: datetime, 
        end_date: datetime
    ) -> List[Dict[str, Any]]:
        """Fetch data using a date range, chunking if necessary."""
        try:
            results = []
            
            # Chunking logic: split into chunks to avoid API limits
            # Some endpoints limit to 1 year or less (e.g. bodyBattery, enduranceScore)
            config = METRICS_CONFIG.get(metric_name, {})
            CHUNK_SIZE_DAYS = config.get("chunk_days", 364) # Default to 52 weeks
            
            current_start = start_date
            while current_start <= end_date:
                current_end = min(current_start + timedelta(days=CHUNK_SIZE_DAYS), end_date)
                
                start_str = current_start.strftime("%Y-%m-%d")
                end_str = current_end.strftime("%Y-%m-%d")
                
                logging.info(f"  Fetching chunk: {start_str} to {end_str}")
                
                chunk_data = None
                # Some methods might not take args if they are "max metrics" fallback
                try:
                    chunk_data = method(start_str, end_str)
                except TypeError:
                    # Fallback for methods that might have changed signature or behave differently
                    # If method doesn't accept args, we can't chunk it effectively in this loop
                    # so we just call it once and break
                    logging.debug(f"Method {method.__name__} rejected range args, trying without")
                    chunk_data = method()
                    current_start = end_date + timedelta(days=1) # Force exit loop
    
                if chunk_data:
                    # Special handling for weight: extract and flatten weight data BEFORE generic flattening
                    if metric_name == "weight" and isinstance(chunk_data, dict):
                        # Check if we have dailyWeightSummaries at the top level
                        if "dailyWeightSummaries" in chunk_data and isinstance(chunk_data["dailyWeightSummaries"], list):
                            all_weight_entries = []
                            for daily_summary in chunk_data["dailyWeightSummaries"]:
                                if isinstance(daily_summary, dict) and "allWeightMetrics" in daily_summary:
                                    summary_date = daily_summary.get("summaryDate")
                                    for entry in daily_summary["allWeightMetrics"]:
                                        if isinstance(entry, dict):
                                            # Add summaryDate if not present
                                            if summary_date and "summaryDate" not in entry:
                                                entry["summaryDate"] = summary_date
                                            all_weight_entries.append(entry)
                            chunk_data = all_weight_entries
                            logging.info(f"Flattened weight data: {len(chunk_data)} entries")
                        # Fallback: check if allWeightMetrics is directly at the top level
                        elif "allWeightMetrics" in chunk_data:
                            summary_date = chunk_data.get("summaryDate")
                            chunk_data = chunk_data["allWeightMetrics"]
                            if summary_date and isinstance(chunk_data, list):
                                for entry in chunk_data:
                                    if isinstance(entry, dict) and "summaryDate" not in entry:
                                        entry["summaryDate"] = summary_date
                            logging.info(f"Flattened weight data: {len(chunk_data)} entries")
                    
                    # Special handling for body_composition: flatten dateWeightList if present
                    elif metric_name == "body_composition" and isinstance(chunk_data, dict) and "dateWeightList" in chunk_data:
                        chunk_data = chunk_data["dateWeightList"]
                        logging.info(f"Flattened body_composition data: {len(chunk_data)} entries")
                    else:
                        # Transform nested arrays for other metrics
                        chunk_data = flatten_nested_arrays(chunk_data, path=metric_name)
                        
                    if isinstance(chunk_data, list):
                        for item in chunk_data:
                            if isinstance(item, dict):
                                item["data_type"] = metric_name
                            results.append(item)
                    elif isinstance(chunk_data, dict):
                        chunk_data["data_type"] = metric_name
                        results.append(chunk_data)
                    else:
                        results.append({"data": chunk_data, "data_type": metric_name})
                
                # Move to next chunk
                current_start = current_end + timedelta(days=1)
                if current_start <= end_date:
                    time.sleep(1) # Sleep between chunks
                
            logging.info(f"Fetched {metric_name}: {len(results)} items")
            return results
            
        except Exception as e:
            logging.error(f"Error fetching {metric_name} (range): {e}")
            return []

    def _fetch_simple(self, method: Callable, metric_name: str) -> List[Dict[str, Any]]:
        """Fetch data without parameters."""
        try:
            data = method()
            if not data:
                return []

            # Transform nested arrays
            data = flatten_nested_arrays(data, path=metric_name)
                
            results = []
            if isinstance(data, list):
                for item in data:
                    if isinstance(item, dict):
                        item["data_type"] = metric_name
                    results.append(item)
            else:
                # If it's a dict or primitive
                if isinstance(data, dict):
                    data["data_type"] = metric_name
                    results.append(data)
                else:
                    results.append({"data": data, "data_type": metric_name})
                    
            logging.info(f"Fetched {metric_name}: {len(results)} items")
            return results
        except Exception as e:
            logging.error(f"Error fetching {metric_name} (simple): {e}")
            return []

    def _fetch_activity_details(
        self, 
        client: Any, 
        start_date: datetime, 
        end_date: datetime
    ) -> List[Dict[str, Any]]:
        """Special handling for activity details (requires 2 steps)."""
        try:
            # 1. Get activities
            activities = client.get_activities_by_date(
                start_date.strftime("%Y-%m-%d"), 
                end_date.strftime("%Y-%m-%d")
            )
            
            results = []
            for activity in activities:
                activity_id = activity.get("activityId")
                if not activity_id:
                    continue
                    
                try:
                    details = client.get_activity_details(activity_id, maxchart=2000, maxpoly=4000)
                    
                    # Transform nested arrays in activity and details
                    clean_activity = flatten_nested_arrays(activity, path=f"activity_{activity_id}")
                    clean_details = flatten_nested_arrays(details, path=f"details_{activity_id}")
                    
                    enriched = {
                        **clean_activity,
                        "detailed_data": clean_details,
                        "data_type": "activity_details"
                    }
                    results.append(enriched)
                    time.sleep(0.5)
                except Exception as e:
                    logging.warning(f"Failed details for {activity_id}: {e}")
                    
            logging.info(f"Fetched details for {len(results)} activities")
            return results
        except Exception as e:
            logging.error(f"Error fetching activity details: {e}")
            return []

    def _fetch_activity_subdata(
        self,
        client: Any,
        metric_name: str,
        method_name: str,
        start_date: datetime,
        end_date: datetime
    ) -> List[Dict[str, Any]]:
        """Generic fetcher for activity-related subdata (splits, weather, etc)."""
        try:
            activities = client.get_activities_by_date(
                start_date.strftime("%Y-%m-%d"), 
                end_date.strftime("%Y-%m-%d")
            )
            
            results = []
            method = getattr(client, method_name)
            
            for activity in activities:
                activity_id = activity.get("activityId")
                if not activity_id:
                    continue
                    
                try:
                    # Special case for splits which has multiple calls in original script
                    if metric_name == "activity_splits":
                        splits = client.get_activity_splits(activity_id)
                        # Try/except for other split types if they exist? 
                        # For simplicity, we stick to the main method defined in config for now,
                        # but the original script called 3 methods.
                        # To match original behavior exactly, we might need custom logic here or in config.
                        # Let's implement the original logic for splits specifically.
                        typed_splits = client.get_activity_typed_splits(activity_id)
                        split_summaries = client.get_activity_split_summaries(activity_id)
                        
                        # Transform nested arrays
                        clean_splits = flatten_nested_arrays(splits, path=f"splits_{activity_id}")
                        clean_typed = flatten_nested_arrays(typed_splits, path=f"typed_splits_{activity_id}")
                        clean_summaries = flatten_nested_arrays(split_summaries, path=f"summaries_{activity_id}")
                        
                        data = {
                            "activityId": activity_id,
                            "activityName": activity.get("activityName", ""),
                            "activityType": activity.get("activityType", ""),
                            "startTimeLocal": activity.get("startTimeLocal", ""),
                            "splits": clean_splits,
                            "typed_splits": clean_typed,
                            "split_summaries": clean_summaries,
                            "data_type": metric_name
                        }
                    else:
                        # Standard subdata (weather, hr_zones, etc)
                        subdata = method(activity_id)
                        if subdata:
                            # Transform nested arrays
                            clean_subdata = flatten_nested_arrays(subdata, path=f"{metric_name}_{activity_id}")
                            
                            data = {
                                "activityId": activity_id,
                                "activityName": activity.get("activityName", ""),
                                "activityType": activity.get("activityType", ""),
                                "startTimeLocal": activity.get("startTimeLocal", ""),
                                f"{metric_name}_data": clean_subdata, # Naming convention from original script varies...
                                # Original: weather_data, hr_zones_data, exercise_sets_data
                                # We might need a mapping for the data key too.
                                "data_type": metric_name
                            }
                            # Fix data key name to match original if possible
                            if metric_name == "activity_weather":
                                data["weather_data"] = subdata
                            elif metric_name == "activity_hr_zones":
                                data["hr_zones_data"] = subdata
                            elif metric_name == "activity_exercise_sets":
                                data["exercise_sets_data"] = subdata
                        else:
                            continue

                    results.append(data)
                    time.sleep(0.3)
                except Exception as e:
                    logging.warning(f"Failed {metric_name} for {activity_id}: {e}")
            
            logging.info(f"Fetched {metric_name} for {len(results)} activities")
            return results
        except Exception as e:
            logging.error(f"Error fetching {metric_name}: {e}")
            return []
