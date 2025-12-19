/*
Table service : persistence incrémentale des données de sommeil.
Toute la logique métier est dans hub_health__stg_sleep.
*/

{{
    config(
        materialized='incremental',
        incremental_strategy='merge',
        unique_key='date',
        tags=['health', 'hub', 'garmin']
    )
}}

SELECT
    * EXCEPT(_dp_inserted_at),
    CURRENT_TIMESTAMP() AS _dp_updated_at
FROM {{ ref('hub_health__stg_sleep') }}
{% if is_incremental() %}
WHERE _dp_inserted_at > (SELECT MAX(_dp_updated_at) FROM {{ this }})
{% endif %}
