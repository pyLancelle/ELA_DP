/*
Table service : persistence incrémentale des activités running consolidées.
Toute la logique métier est dans hub_health__stg_activities.
Inclut : métriques de base, laps, intervalles, timeseries et polyline GPS.
*/

{{
    config(
        materialized='incremental',
        incremental_strategy='merge',
        unique_key='activityid',
        tags=['health', 'hub', 'garmin']
    )
}}

SELECT
    *,
    CURRENT_TIMESTAMP() AS _dp_updated_at
FROM {{ ref('hub_health__stg_activities') }}
{% if is_incremental() %}
WHERE startTimeGMT > (SELECT MAX(startTimeGMT) FROM {{ this }})
{% endif %}
