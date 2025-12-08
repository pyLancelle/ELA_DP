/*
Table service : persistence incrémentale des splits/laps d'activités running.
Toute la logique métier est dans hub_health__stg_activity_splits.
*/

{{
    config(
        materialized='incremental',
        incremental_strategy='merge',
        unique_key=['activity_id', 'split_number'],
        tags=['health', 'hub', 'garmin']
    )
}}

SELECT
    *,
    CURRENT_TIMESTAMP() AS _dp_updated_at
FROM {{ ref('hub_health__stg_activity_splits') }}
{% if is_incremental() %}
WHERE date > (SELECT MAX(date) FROM {{ this }})
{% endif %}
