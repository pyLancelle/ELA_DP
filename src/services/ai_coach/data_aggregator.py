"""
Data Aggregator for AI Coach
----------------------------
Aggregates data from BigQuery for AI Coach analysis.
Implements smart sampling to optimize token usage:
- Last 7 days: Full detailed data (~2K tokens)
- Days 8-30: Activity summaries (~1K tokens)
- Days 31-90: Weekly aggregates (~500 tokens)
"""

import logging
from datetime import date, timedelta
from typing import Any, Optional

from google.cloud import bigquery

from .config import PROJECT_ID

logger = logging.getLogger(__name__)

# BigQuery datasets
DATASET_HUB = "dp_hub_dev"

# Table names
TABLE_ACTIVITIES = f"{PROJECT_ID}.{DATASET_HUB}.hub_health__svc_activities"
TABLE_SLEEP = f"{PROJECT_ID}.{DATASET_HUB}.hub_health__svc_sleep"

# Singleton BigQuery client
_bq_client: Optional[bigquery.Client] = None


def _get_bq_client() -> bigquery.Client:
    """
    Get or create a BigQuery client singleton.

    Returns:
        Configured BigQuery client.
    """
    global _bq_client
    if _bq_client is None:
        _bq_client = bigquery.Client(project=PROJECT_ID)
    return _bq_client


def _execute_query(query: str) -> list[dict[str, Any]]:
    """
    Execute a BigQuery query and return results as list of dicts.

    Args:
        query: SQL query to execute.

    Returns:
        List of dictionaries representing rows.
    """
    client = _get_bq_client()
    logger.debug(f"Executing query: {query[:200]}...")

    try:
        results = client.query(query).result()
        rows = [dict(row) for row in results]
        logger.info(f"Query returned {len(rows)} rows")
        return rows
    except Exception as e:
        logger.error(f"Query failed: {e}")
        raise


# =============================================================================
# Activity Aggregation Functions
# =============================================================================


def get_recent_activity_snapshot(days: int = 7) -> list[dict[str, Any]]:
    """
    Get detailed activity data for the last N days.
    Returns full activity details for recent analysis.

    Args:
        days: Number of days to look back. Defaults to 7.

    Returns:
        List of activity records with full details.
    """
    query = f"""
    SELECT
        activityId,
        activityName,
        DATE(startTimeGMT) AS activity_date,
        typeKey AS activity_type,
        ROUND(distance / 1000, 2) AS distance_km,
        ROUND(duration / 60, 1) AS duration_minutes,
        ROUND(averageSpeed * 3.6, 2) AS avg_speed_kmh,
        CASE
            WHEN averageSpeed > 0 THEN ROUND(1000 / averageSpeed / 60, 2)
            ELSE NULL
        END AS avg_pace_min_per_km,
        averageHR AS avg_hr,
        maxHR AS max_hr,
        ROUND(elevationGain, 0) AS elevation_gain_m,
        calories,
        aerobicTrainingEffect AS aerobic_te,
        anaerobicTrainingEffect AS anaerobic_te,
        activityTrainingLoad AS training_load,
        hr_zones.hrTimeInZone_1 AS hr_zone1_seconds,
        hr_zones.hrTimeInZone_2 AS hr_zone2_seconds,
        hr_zones.hrTimeInZone_3 AS hr_zone3_seconds,
        hr_zones.hrTimeInZone_4 AS hr_zone4_seconds,
        hr_zones.hrTimeInZone_5 AS hr_zone5_seconds
    FROM `{TABLE_ACTIVITIES}`
    WHERE DATE(startTimeGMT) >= DATE_SUB(CURRENT_DATE(), INTERVAL {days} DAY)
    ORDER BY startTimeGMT DESC
    """

    logger.info(f"Fetching activity snapshot for last {days} days")
    return _execute_query(query)


def get_activity_summary_for_profile(days: int = 90) -> dict[str, Any]:
    """
    Get aggregated activity summary for runner profile generation.
    Implements smart sampling: detailed (7d), summary (30d), weekly (90d).

    Args:
        days: Total days to analyze. Defaults to 90.

    Returns:
        Dictionary containing activity summaries at different granularities.
    """
    logger.info(f"Generating activity summary for profile (last {days} days)")

    # Recent 7 days - detailed
    recent_activities = get_recent_activity_snapshot(days=7)

    # Days 8-30 - per-activity summary (no timeseries)
    query_month = f"""
    SELECT
        activityId,
        DATE(startTimeGMT) AS activity_date,
        typeKey AS activity_type,
        ROUND(distance / 1000, 2) AS distance_km,
        ROUND(duration / 60, 1) AS duration_minutes,
        CASE
            WHEN averageSpeed > 0 THEN ROUND(1000 / averageSpeed / 60, 2)
            ELSE NULL
        END AS avg_pace_min_per_km,
        averageHR AS avg_hr,
        aerobicTrainingEffect AS aerobic_te,
        activityTrainingLoad AS training_load
    FROM `{TABLE_ACTIVITIES}`
    WHERE DATE(startTimeGMT) BETWEEN
        DATE_SUB(CURRENT_DATE(), INTERVAL 30 DAY)
        AND DATE_SUB(CURRENT_DATE(), INTERVAL 8 DAY)
    ORDER BY startTimeGMT DESC
    """
    month_activities = _execute_query(query_month)

    # Days 31-90 - weekly aggregates only
    query_weekly = f"""
    SELECT
        DATE_TRUNC(DATE(startTimeGMT), WEEK(MONDAY)) AS week_start,
        COUNT(*) AS run_count,
        ROUND(SUM(distance) / 1000, 1) AS total_distance_km,
        ROUND(SUM(duration) / 3600, 1) AS total_duration_hours,
        ROUND(AVG(CASE WHEN averageSpeed > 0 THEN 1000 / averageSpeed / 60 END), 2) AS avg_pace_min_per_km,
        ROUND(AVG(averageHR), 0) AS avg_hr,
        ROUND(SUM(elevationGain), 0) AS total_elevation_m,
        ROUND(AVG(aerobicTrainingEffect), 2) AS avg_aerobic_te,
        ROUND(SUM(activityTrainingLoad), 0) AS total_training_load
    FROM `{TABLE_ACTIVITIES}`
    WHERE DATE(startTimeGMT) BETWEEN
        DATE_SUB(CURRENT_DATE(), INTERVAL {days} DAY)
        AND DATE_SUB(CURRENT_DATE(), INTERVAL 31 DAY)
    GROUP BY week_start
    ORDER BY week_start DESC
    """
    weekly_aggregates = _execute_query(query_weekly)

    # Overall statistics
    query_stats = f"""
    SELECT
        COUNT(*) AS total_runs,
        ROUND(SUM(distance) / 1000, 1) AS total_distance_km,
        ROUND(AVG(distance) / 1000, 1) AS avg_distance_km,
        ROUND(MAX(distance) / 1000, 1) AS max_distance_km,
        ROUND(SUM(duration) / 3600, 1) AS total_duration_hours,
        ROUND(AVG(CASE WHEN averageSpeed > 0 THEN 1000 / averageSpeed / 60 END), 2) AS avg_pace_min_per_km,
        ROUND(MIN(CASE WHEN averageSpeed > 0 AND distance > 5000 THEN 1000 / averageSpeed / 60 END), 2) AS best_pace_min_per_km,
        ROUND(AVG(averageHR), 0) AS avg_hr,
        ROUND(AVG(maxHR), 0) AS avg_max_hr,
        ROUND(SUM(elevationGain), 0) AS total_elevation_m,
        ROUND(AVG(aerobicTrainingEffect), 2) AS avg_aerobic_te,
        ROUND(AVG(anaerobicTrainingEffect), 2) AS avg_anaerobic_te,
        ROUND(SUM(activityTrainingLoad), 0) AS total_training_load,
        ROUND(AVG(activityTrainingLoad), 1) AS avg_training_load
    FROM `{TABLE_ACTIVITIES}`
    WHERE DATE(startTimeGMT) >= DATE_SUB(CURRENT_DATE(), INTERVAL {days} DAY)
    """
    overall_stats = _execute_query(query_stats)

    return {
        "period_days": days,
        "overall_statistics": overall_stats[0] if overall_stats else {},
        "recent_7_days": recent_activities,
        "days_8_to_30": month_activities,
        "weekly_aggregates_31_to_90": weekly_aggregates,
    }


# =============================================================================
# Health/Sleep Aggregation Functions
# =============================================================================


def get_recent_health_snapshot(days: int = 7) -> list[dict[str, Any]]:
    """
    Get detailed health/sleep data for the last N days.
    Returns full sleep metrics for recent analysis.

    Args:
        days: Number of days to look back. Defaults to 7.

    Returns:
        List of sleep records with full details.
    """
    query = f"""
    SELECT
        date,
        sleep_score,
        FORMAT_DATETIME('%H:%M', bedtime) AS bedtime,
        FORMAT_DATETIME('%H:%M', waketime) AS waketime,
        total_sleep_hours,
        deep_sleep.hours AS deep_sleep_hours,
        deep_sleep.percentage AS deep_sleep_pct,
        deep_sleep.status AS deep_sleep_status,
        light_sleep.hours AS light_sleep_hours,
        light_sleep.percentage AS light_sleep_pct,
        rem_sleep.hours AS rem_sleep_hours,
        rem_sleep.percentage AS rem_sleep_pct,
        awake.hours AS awake_hours,
        awake.wake_count,
        awake.quality AS awake_quality,
        body_battery.at_bedtime AS bb_at_bedtime,
        body_battery.at_waketime AS bb_at_waketime,
        body_battery.recovery AS bb_recovery,
        body_battery.recovery_quality AS bb_recovery_quality,
        avg_hrv,
        avg_stress,
        resting_heart_rate,
        sleep_quality
    FROM `{TABLE_SLEEP}`
    WHERE date >= DATE_SUB(CURRENT_DATE(), INTERVAL {days} DAY)
    ORDER BY date DESC
    """

    logger.info(f"Fetching health snapshot for last {days} days")
    return _execute_query(query)


def get_health_summary_for_profile(days: int = 90) -> dict[str, Any]:
    """
    Get aggregated health/sleep summary for runner profile generation.
    Implements smart sampling: detailed (7d), summary (30d), weekly (90d).

    Args:
        days: Total days to analyze. Defaults to 90.

    Returns:
        Dictionary containing health summaries at different granularities.
    """
    logger.info(f"Generating health summary for profile (last {days} days)")

    # Recent 7 days - detailed
    recent_health = get_recent_health_snapshot(days=7)

    # Days 8-30 - daily summary (key metrics only)
    query_month = f"""
    SELECT
        date,
        sleep_score,
        total_sleep_hours,
        deep_sleep.percentage AS deep_sleep_pct,
        rem_sleep.percentage AS rem_sleep_pct,
        body_battery.recovery AS bb_recovery,
        avg_hrv,
        resting_heart_rate,
        sleep_quality
    FROM `{TABLE_SLEEP}`
    WHERE date BETWEEN
        DATE_SUB(CURRENT_DATE(), INTERVAL 30 DAY)
        AND DATE_SUB(CURRENT_DATE(), INTERVAL 8 DAY)
    ORDER BY date DESC
    """
    month_health = _execute_query(query_month)

    # Days 31-90 - weekly aggregates
    query_weekly = f"""
    SELECT
        DATE_TRUNC(date, WEEK(MONDAY)) AS week_start,
        COUNT(*) AS nights_tracked,
        ROUND(AVG(sleep_score), 1) AS avg_sleep_score,
        ROUND(AVG(total_sleep_hours), 2) AS avg_sleep_hours,
        ROUND(AVG(deep_sleep.percentage), 1) AS avg_deep_sleep_pct,
        ROUND(AVG(rem_sleep.percentage), 1) AS avg_rem_sleep_pct,
        ROUND(AVG(body_battery.recovery), 1) AS avg_bb_recovery,
        ROUND(AVG(avg_hrv), 1) AS avg_hrv,
        ROUND(AVG(resting_heart_rate), 0) AS avg_resting_hr
    FROM `{TABLE_SLEEP}`
    WHERE date BETWEEN
        DATE_SUB(CURRENT_DATE(), INTERVAL {days} DAY)
        AND DATE_SUB(CURRENT_DATE(), INTERVAL 31 DAY)
    GROUP BY week_start
    ORDER BY week_start DESC
    """
    weekly_aggregates = _execute_query(query_weekly)

    # Overall statistics
    query_stats = f"""
    SELECT
        COUNT(*) AS nights_tracked,
        ROUND(AVG(sleep_score), 1) AS avg_sleep_score,
        ROUND(MIN(sleep_score), 0) AS min_sleep_score,
        ROUND(MAX(sleep_score), 0) AS max_sleep_score,
        ROUND(AVG(total_sleep_hours), 2) AS avg_sleep_hours,
        ROUND(AVG(deep_sleep.percentage), 1) AS avg_deep_sleep_pct,
        ROUND(AVG(light_sleep.percentage), 1) AS avg_light_sleep_pct,
        ROUND(AVG(rem_sleep.percentage), 1) AS avg_rem_sleep_pct,
        ROUND(AVG(body_battery.recovery), 1) AS avg_bb_recovery,
        ROUND(AVG(avg_hrv), 1) AS avg_hrv,
        ROUND(MIN(avg_hrv), 0) AS min_hrv,
        ROUND(MAX(avg_hrv), 0) AS max_hrv,
        ROUND(AVG(resting_heart_rate), 0) AS avg_resting_hr,
        ROUND(MIN(resting_heart_rate), 0) AS min_resting_hr,
        ROUND(AVG(avg_stress), 1) AS avg_stress,
        COUNTIF(sleep_quality = 'excellent') AS excellent_nights,
        COUNTIF(sleep_quality = 'good') AS good_nights,
        COUNTIF(sleep_quality = 'fair') AS fair_nights,
        COUNTIF(sleep_quality IN ('poor', 'very_poor')) AS poor_nights
    FROM `{TABLE_SLEEP}`
    WHERE date >= DATE_SUB(CURRENT_DATE(), INTERVAL {days} DAY)
    """
    overall_stats = _execute_query(query_stats)

    return {
        "period_days": days,
        "overall_statistics": overall_stats[0] if overall_stats else {},
        "recent_7_days": recent_health,
        "days_8_to_30": month_health,
        "weekly_aggregates_31_to_90": weekly_aggregates,
    }


# =============================================================================
# Combined Data Functions
# =============================================================================


def get_full_profile_data(days: int = 90) -> dict[str, Any]:
    """
    Get all data needed for runner profile generation.
    Combines activity and health data with smart sampling.

    Args:
        days: Total days to analyze. Defaults to 90.

    Returns:
        Dictionary containing both activity and health summaries.
    """
    logger.info(f"Generating full profile data for last {days} days")

    activity_data = get_activity_summary_for_profile(days=days)
    health_data = get_health_summary_for_profile(days=days)

    return {
        "analysis_period_days": days,
        "generated_at": date.today().isoformat(),
        "activities": activity_data,
        "health": health_data,
    }


def get_weekly_review_data(
    week_start: date,
    week_end: date,
) -> dict[str, Any]:
    """
    Get data for weekly review analysis.
    Fetches activities and health data for a specific week.

    Args:
        week_start: First day of the week (Monday).
        week_end: Last day of the week (Sunday).

    Returns:
        Dictionary containing week's activities and health data.
    """
    logger.info(f"Fetching weekly review data: {week_start} to {week_end}")

    # Activities for the week
    query_activities = f"""
    SELECT
        activityId,
        activityName,
        DATE(startTimeGMT) AS activity_date,
        typeKey AS activity_type,
        ROUND(distance / 1000, 2) AS distance_km,
        ROUND(duration / 60, 1) AS duration_minutes,
        CASE
            WHEN averageSpeed > 0 THEN ROUND(1000 / averageSpeed / 60, 2)
            ELSE NULL
        END AS avg_pace_min_per_km,
        averageHR AS avg_hr,
        maxHR AS max_hr,
        ROUND(elevationGain, 0) AS elevation_gain_m,
        aerobicTrainingEffect AS aerobic_te,
        anaerobicTrainingEffect AS anaerobic_te,
        activityTrainingLoad AS training_load,
        hr_zones.hrTimeInZone_1 AS hr_zone1_seconds,
        hr_zones.hrTimeInZone_2 AS hr_zone2_seconds,
        hr_zones.hrTimeInZone_3 AS hr_zone3_seconds,
        hr_zones.hrTimeInZone_4 AS hr_zone4_seconds,
        hr_zones.hrTimeInZone_5 AS hr_zone5_seconds
    FROM `{TABLE_ACTIVITIES}`
    WHERE DATE(startTimeGMT) BETWEEN '{week_start}' AND '{week_end}'
    ORDER BY startTimeGMT
    """
    activities = _execute_query(query_activities)

    # Health data for the week
    query_health = f"""
    SELECT
        date,
        sleep_score,
        total_sleep_hours,
        deep_sleep.percentage AS deep_sleep_pct,
        rem_sleep.percentage AS rem_sleep_pct,
        body_battery.at_bedtime AS bb_at_bedtime,
        body_battery.at_waketime AS bb_at_waketime,
        body_battery.recovery AS bb_recovery,
        avg_hrv,
        resting_heart_rate,
        sleep_quality
    FROM `{TABLE_SLEEP}`
    WHERE date BETWEEN '{week_start}' AND '{week_end}'
    ORDER BY date
    """
    health = _execute_query(query_health)

    # Week summary
    query_summary = f"""
    SELECT
        COUNT(*) AS total_runs,
        ROUND(SUM(distance) / 1000, 1) AS total_distance_km,
        ROUND(SUM(duration) / 3600, 2) AS total_duration_hours,
        ROUND(AVG(CASE WHEN averageSpeed > 0 THEN 1000 / averageSpeed / 60 END), 2) AS avg_pace,
        ROUND(SUM(elevationGain), 0) AS total_elevation_m,
        ROUND(SUM(activityTrainingLoad), 0) AS total_training_load
    FROM `{TABLE_ACTIVITIES}`
    WHERE DATE(startTimeGMT) BETWEEN '{week_start}' AND '{week_end}'
    """
    activity_summary = _execute_query(query_summary)

    query_health_summary = f"""
    SELECT
        ROUND(AVG(sleep_score), 1) AS avg_sleep_score,
        ROUND(AVG(total_sleep_hours), 2) AS avg_sleep_hours,
        ROUND(AVG(avg_hrv), 1) AS avg_hrv,
        ROUND(AVG(resting_heart_rate), 0) AS avg_resting_hr,
        ROUND(AVG(body_battery.recovery), 1) AS avg_bb_recovery
    FROM `{TABLE_SLEEP}`
    WHERE date BETWEEN '{week_start}' AND '{week_end}'
    """
    health_summary = _execute_query(query_health_summary)

    return {
        "week_start": week_start.isoformat(),
        "week_end": week_end.isoformat(),
        "activities": activities,
        "health": health,
        "activity_summary": activity_summary[0] if activity_summary else {},
        "health_summary": health_summary[0] if health_summary else {},
    }
