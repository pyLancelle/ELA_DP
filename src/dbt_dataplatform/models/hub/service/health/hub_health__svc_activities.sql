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
    * EXCEPT(_dp_inserted_at),
    CURRENT_TIMESTAMP() AS _dp_updated_at
FROM {{ ref('hub_health__stg_activities') }}
{% if is_incremental() %}
WHERE _dp_inserted_at > (SELECT MAX(_dp_updated_at) FROM {{ this }})
{% endif %}
