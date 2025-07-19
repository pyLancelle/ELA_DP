{{ config(dataset=get_schema('lake')) }}
SELECT *
FROM {{ source('spotify', 'staging_spotify') }}
