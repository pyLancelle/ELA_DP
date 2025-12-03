/*
Table service : persistence incrémentale des activités running.
Toute la logique métier est dans hub_health__stg_activities.
*/

{{
    config(
        materialized='incremental',
        incremental_strategy='merge',
        unique_key='activity_id',
        tags=['health', 'hub']
    )
}}

SELECT
    *,
    CURRENT_TIMESTAMP() AS _dp_updated_at
FROM {{ ref('hub_health__stg_activities') }}
{% if is_incremental() %}
WHERE date > (SELECT MAX(date) FROM {{ this }})
{% endif %}
