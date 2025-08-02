-- Vue dbt optimisée pour le dashboard de sommeil ELA
-- Modèle : sleep_dashboard_optimized.sql
-- Description : Vue aggregée et optimisée des données de sommeil pour alimenter le dashboard

{{ config(
    dataset=get_schema('product'),
    materialized='view',
    description='Vue optimisée des données de sommeil pour le dashboard ELA avec métriques dérivées',
    tags=["product", "dashboard", "garmin"]
) }}

with

-- =============================================================================
-- IMPORT CTEs - Extraction des données sources
-- =============================================================================

sleep_import as (

    select 
        sleep_date,
        total_sleep_seconds,
        deep_sleep_seconds,
        light_sleep_seconds,
        rem_sleep_seconds,
        awake_sleep_seconds,
        sleep_window.start_local as bedtime,
        sleep_window.end_local as wake_time,
        sleep_scores.overall.value as sleep_score,
        sleep_need.actual as recommended_minutes

    from {{ ref('hub_garmin__sleep') }}
    where 
        total_sleep_seconds >= 3600
        and sleep_window.confirmed = true

),

-- =============================================================================
-- LOGICAL CTEs - Transformations et calculs métier
-- =============================================================================

sleep_formatted as (

    select
        sleep_date,
        bedtime,
        wake_time,
        
        -- Conversion des durées en heures
        round(total_sleep_seconds / 3600.0, 2) as duration_hours,
        round(deep_sleep_seconds / 3600.0, 2) as deep_hours,
        round(light_sleep_seconds / 3600.0, 2) as light_hours,
        round(rem_sleep_seconds / 3600.0, 2) as rem_hours,
        round(awake_sleep_seconds / 3600.0, 2) as awake_hours,
        
        -- Heures formatées pour le dashboard (avec décimales)
        extract(hour from bedtime) + extract(minute from bedtime) / 60.0 as bedtime_hour,
        extract(hour from wake_time) + extract(minute from wake_time) / 60.0 as wake_time_hour,
        
        -- Métriques de qualité
        sleep_score,
        round(recommended_minutes / 60.0, 1) as recommended_hours

    from sleep_import

),

sleep_metrics as (

    select
        *,
        
        -- Calculs de métriques dérivées
        (duration_hours - recommended_hours) as sleep_debt,
        round((deep_hours + light_hours + rem_hours) / nullif(duration_hours, 0) * 100, 1) as sleep_efficiency

    from sleep_formatted

),

sleep_categories as (

    select
        *,
        
        -- Classification de la qualité du sommeil
        case 
            when sleep_score >= 85 then 'Excellent'
            when sleep_score >= 75 then 'Bon'
            when sleep_score >= 65 then 'Moyen'
            when sleep_score >= 50 then 'Faible'
            else 'Très faible'
        end as sleep_quality_category,
        
        -- Indicateurs de patterns
        case 
            when bedtime_hour between 19 and 22.5 then 'Coucher tôt'
            when bedtime_hour <= 24 then 'Coucher normal'
            else 'Coucher tardif'
        end as bedtime_category,
        
        case 
            when wake_time_hour <= 6.5 then 'Réveil tôt'
            when wake_time_hour <= 9 then 'Réveil normal'
            else 'Réveil tardif'
        end as wake_time_category

    from sleep_metrics

),

sleep_trends as (

    select 
        *,
        
        -- Moyennes mobiles sur 7 jours
        avg(duration_hours) over (
            order by sleep_date 
            rows between 6 preceding and current row
        ) as duration_7day_avg,
        
        avg(sleep_score) over (
            order by sleep_date 
            rows between 6 preceding and current row
        ) as score_7day_avg,
        
        avg(sleep_debt) over (
            order by sleep_date 
            rows between 6 preceding and current row
        ) as sleep_debt_7day_avg,
        
        -- Moyennes mobiles sur 30 jours
        avg(duration_hours) over (
            order by sleep_date 
            rows between 29 preceding and current row
        ) as duration_30day_avg,
        
        avg(sleep_score) over (
            order by sleep_date 
            rows between 29 preceding and current row
        ) as score_30day_avg

    from sleep_categories

),

sleep_analysis as (

    select 
        *,
        
        -- Tendances (comparaison avec moyenne sur 7 jours précédents)
        duration_hours - avg(duration_hours) over (
            order by sleep_date 
            rows between 13 preceding and 7 preceding
        ) as duration_trend,
        
        sleep_score - avg(sleep_score) over (
            order by sleep_date 
            rows between 13 preceding and 7 preceding
        ) as score_trend,
        
        -- Rang de performance (percentile du score)
        percent_rank() over (order by sleep_score) as score_percentile,
        
        -- Cohérence du sommeil (écart-type mobile sur 7 jours)
        stddev(bedtime_hour) over (
            order by sleep_date 
            rows between 6 preceding and current row
        ) as bedtime_consistency,
        
        stddev(duration_hours) over (
            order by sleep_date 
            rows between 6 preceding and current row
        ) as duration_consistency

    from sleep_trends

),

-- =============================================================================
-- FINAL CTE - Formatage final pour le dashboard
-- =============================================================================

final as (

    select 
        -- Données de base pour le dashboard
        format_date('%Y-%m-%d', sleep_date) as sleep_date,
        format_timestamp('%Y-%m-%d %H:%M:%S', bedtime) as bedtime,
        format_timestamp('%Y-%m-%d %H:%M:%S', wake_time) as wake_time,
        bedtime_hour,
        wake_time_hour,
        duration_hours as duration,
        deep_hours as deep,
        light_hours as light,
        rem_hours as rem,
        awake_hours as awake,
        sleep_score as score,
        recommended_hours as recommended,
        
        -- Métriques dérivées
        sleep_debt,
        sleep_efficiency,
        sleep_quality_category,
        bedtime_category,
        wake_time_category,
        
        -- Tendances et moyennes mobiles (arrondies)
        round(duration_7day_avg, 2) as duration_7day_avg,
        round(score_7day_avg, 1) as score_7day_avg,
        round(sleep_debt_7day_avg, 2) as sleep_debt_7day_avg,
        round(duration_30day_avg, 2) as duration_30day_avg,
        round(score_30day_avg, 1) as score_30day_avg,
        
        -- Indicateurs de tendance
        round(duration_trend, 2) as duration_trend,
        round(score_trend, 1) as score_trend,
        round(score_percentile * 100, 1) as score_percentile,
        
        -- Métriques de cohérence
        round(bedtime_consistency, 2) as bedtime_consistency,
        round(duration_consistency, 2) as duration_consistency,
        
        -- Flags pour le dashboard
        case when sleep_debt < -0.5 then true else false end as sleep_deficit_flag,
        case when score_trend > 5 then true else false end as improving_trend_flag,
        case when bedtime_consistency < 0.5 then true else false end as consistent_schedule_flag,
        
        -- Métadonnées pour la dernière mise à jour
        current_timestamp() as last_updated

    from sleep_analysis

)

select * from final
order by sleep_date desc