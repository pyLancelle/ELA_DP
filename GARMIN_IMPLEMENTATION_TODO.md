# Garmin Implementation Complete Todo List

## Overview
This document provides a comprehensive todo list for implementing both historical data ingestion and dbt incremental models for the Garmin data pipeline.

## Phase 1: Historical Backfill Completion 🔄 IN PROGRESS

### Backfill Monitoring & Management
- [ ] **Monitor backfill progress** - Check logs and `backfill_progress.json`
  - Command: `tail -f logs/garmin_backfill.log`
  - Expected: ~20 monthly batches, 4-6 hours total
  
- [ ] **Verify backfill completion**
  - Check final batch completion in logs
  - Validate all data types extracted for each month
  - Confirm no critical errors in failed_batches

- [ ] **Validate local historical files**
  - Count files: `ls data/historical/garmin/*.jsonl | wc -l` (expect ~240 files)
  - Check file sizes: `du -sh data/historical/garmin/`
  - Sample data quality: `head -1 data/historical/garmin/*activities*.jsonl | jq`

## Phase 2: Historical Data Ingestion to BigQuery

### GCS Upload Preparation
- [ ] **Prepare GCS upload environment**
  - Verify GCS bucket access: `gsutil ls gs://ela-dp-dev/garmin/`
  - Check authentication: `gcloud auth list`
  - Clear landing folder if needed: `gsutil rm gs://ela-dp-dev/garmin/landing/*HISTORICAL*`

- [ ] **Upload historical files to GCS**
  ```bash
  # Upload all historical files to landing folder
  gsutil -m cp data/historical/garmin/*HISTORICAL*.jsonl gs://ela-dp-dev/garmin/landing/
  
  # Verify upload
  gsutil ls gs://ela-dp-dev/garmin/landing/ | grep HISTORICAL | wc -l
  ```

### Historical Ingestion Execution
- [ ] **Run historical ingestion (DEV)**
  ```bash
  python -m src.connectors.garmin.garmin_ingest --env dev
  ```

- [ ] **Monitor ingestion process**
  - Watch BigQuery job progress
  - Monitor logs for any processing errors
  - Verify files moved to archive folder

- [ ] **Validate historical data in BigQuery**
  ```sql
  -- Check historical data volume
  SELECT 
      data_type,
      COUNT(*) as record_count,
      MIN(dp_inserted_at) as first_ingested,
      MAX(dp_inserted_at) as last_ingested
  FROM `polar-scene-465223-f7.dp_lake_dev.staging_garmin_raw`
  WHERE source_file LIKE '%HISTORICAL%'
  GROUP BY data_type;

  -- Verify date coverage
  SELECT 
      DATE(JSON_EXTRACT_SCALAR(raw_data, '$.startTimeGMT')) as activity_date,
      COUNT(*) as activities
  FROM `polar-scene-465223-f7.dp_lake_dev.staging_garmin_raw`
  WHERE data_type = 'activities' AND source_file LIKE '%HISTORICAL%'
  GROUP BY 1 
  ORDER BY 1 LIMIT 20;
  ```

## Phase 3: dbt Incremental Model Implementation

### dbt Configuration Updates
- [ ] **Update dbt_project.yml**
  ```yaml
  models:
    dbt_dataplatform:
      lake:
        garmin:
          +materialized: incremental
          +incremental_strategy: append
          +on_schema_change: sync_all_columns
  ```

### Lake Model Conversions (Priority Order)
- [ ] **Convert lake_garmin__steps.sql** (Highest volume)
  - Add incremental config
  - Add incremental WHERE logic
  - Test with small data set

- [ ] **Convert lake_garmin__activities.sql** (Complex transformations)
  - Add incremental config and logic
  - Test JSON extraction performance
  - Validate all nested fields extracted correctly

- [ ] **Convert lake_garmin__body_battery.sql** (Time-series complexity)
  - Add incremental logic
  - Test time-series array processing

- [ ] **Convert remaining high-priority models**
  - [ ] lake_garmin__sleep.sql
  - [ ] lake_garmin__hrv.sql
  - [ ] lake_garmin__floors.sql

- [ ] **Convert remaining models**
  - [ ] lake_garmin__weight.sql
  - [ ] lake_garmin__device_info.sql  
  - [ ] lake_garmin__training_status.sql
  - [ ] lake_garmin__race_predictions.sql

### dbt Model Template Implementation
For each model, implement this pattern:
```sql
{{ config(
    materialized='incremental',
    incremental_strategy='append'
) }}

-- [Existing SELECT logic]

FROM {{ source('garmin', 'staging_garmin_raw') }}
WHERE data_type = '[data_type]'
  AND JSON_EXTRACT_SCALAR(raw_data, '$.[key_field]') IS NOT NULL

{% if is_incremental() %}
  AND dp_inserted_at > (SELECT MAX(dp_inserted_at) FROM {{ this }})
{% endif %}
```

## Phase 4: Testing & Validation

### Initial Full Refresh Testing
- [ ] **Run full refresh on converted models**
  ```bash
  cd src/dbt_dataplatform/
  dbt run --select lake.garmin --full-refresh --log-level debug
  ```

- [ ] **Validate data completeness**
  ```sql
  -- Compare record counts between source and lake models
  WITH source_counts AS (
    SELECT data_type, COUNT(*) as source_count
    FROM staging_garmin_raw 
    GROUP BY data_type
  )
  SELECT 
    'activities' as data_type,
    COUNT(*) as lake_count,
    s.source_count,
    COUNT(*) - s.source_count as difference
  FROM lake_garmin__activities a
  JOIN source_counts s ON s.data_type = 'activities'
  GROUP BY s.source_count
  -- Repeat for each data type
  ```

### Incremental Behavior Testing  
- [ ] **Test incremental processing**
  ```bash
  # Should process only new data
  dbt run --select lake.garmin
  
  # Check logs for "0 rows processed" or minimal processing
  ```

- [ ] **Simulate new data ingestion**
  - Add a new daily file to GCS landing
  - Run ingestion + dbt
  - Verify only new data processed

### Hub/Product Layer Validation
- [ ] **Test hub models with historical data**
  ```bash
  dbt run --select hub.garmin
  ```

- [ ] **Validate hub model performance**
  - Check query execution times
  - Verify expected record volumes
  - Test date range coverage

- [ ] **Test product models**
  ```bash
  dbt run --select product.garmin
  ```

## Phase 5: Production Deployment

### Production Historical Ingestion
- [ ] **Upload historical files to production GCS**
  ```bash
  gsutil -m cp data/historical/garmin/*HISTORICAL*.jsonl gs://ela-dp-prd/garmin/landing/
  ```

- [ ] **Run production historical ingestion**
  ```bash
  python -m src.connectors.garmin.garmin_ingest --env prd
  ```

### Production dbt Deployment  
- [ ] **Deploy incremental models to production**
  ```bash
  cd src/dbt_dataplatform/
  dbt run --select lake.garmin --target prd --full-refresh
  ```

- [ ] **Validate production data**
  - Run same validation queries as dev
  - Compare dev vs prod record counts
  - Verify date coverage matches

## Phase 6: Monitoring & Optimization

### Performance Monitoring
- [ ] **Monitor daily build times**
  - Track dbt run duration
  - Compare before/after incremental conversion
  - Expected: 5-10 minutes → 30-60 seconds

- [ ] **Monitor BigQuery costs**
  - Track query processing in BigQuery console
  - Compare monthly costs before/after
  - Expected: ~95% reduction in lake processing costs

### Data Quality Monitoring
- [ ] **Set up data freshness monitoring**
  ```yaml
  # In schema.yaml
  sources:
    - name: garmin
      freshness:
        warn_after: {count: 2, period: day}
        error_after: {count: 3, period: day}
  ```

- [ ] **Create data quality tests**
  ```sql
  -- Test for data gaps
  SELECT date_spine.date
  FROM (SELECT DATE_ADD('2024-01-01', INTERVAL day_offset DAY) as date
        FROM UNNEST(GENERATE_ARRAY(0, 600)) as day_offset) date_spine
  LEFT JOIN lake_garmin__activities a 
    ON date_spine.date = DATE(a.start_time_gmt)
  WHERE a.activity_id IS NULL
    AND date_spine.date <= CURRENT_DATE()
  ```

## Success Criteria Checklist

### Historical Data Integration ✅
- [ ] All historical files (18+ months) successfully ingested
- [ ] ~110,000 records in staging_garmin_raw with HISTORICAL marker
- [ ] Date coverage from 2024-01-01 to 2025-07-20
- [ ] All 12 data types represented in historical data
- [ ] No data quality issues or corrupted records

### Incremental Model Performance ✅  
- [ ] All 10 lake models converted to incremental
- [ ] Daily build time reduced by >80%
- [ ] BigQuery processing costs reduced by >90%
- [ ] Data completeness maintained (no record loss)
- [ ] Hub/Product models work with combined historical+daily data

### Production Readiness ✅
- [ ] Dev and prod environments have identical data
- [ ] Daily ingestion + incremental dbt runs working smoothly  
- [ ] Monitoring and alerting in place
- [ ] Documentation updated and accessible
- [ ] Rollback procedures tested and documented

## Timeline Estimate
- **Phase 1**: In progress (4-6 hours for backfill)
- **Phase 2**: 1 hour (GCS upload + ingestion)
- **Phase 3**: 4 hours (dbt incremental conversion)
- **Phase 4**: 2 hours (testing and validation)
- **Phase 5**: 1 hour (production deployment)
- **Phase 6**: 30 minutes (monitoring setup)

**Total remaining**: ~8.5 hours after backfill completes

## Dependencies & Blockers
1. **Backfill completion** → Required for historical ingestion
2. **Historical ingestion** → Required for dbt testing with full data volume
3. **Dev validation** → Required before production deployment
4. **Production deployment** → Required for monitoring setup

## Risk Mitigation
- **Backup current state** before each major change
- **Test in dev first** for all modifications
- **Rollback procedures** documented for each phase
- **Incremental implementation** (can pause at any step)
- **Progress tracking** with checkpoints at each phase

This comprehensive todo list ensures systematic implementation of both historical data integration and performance optimization through incremental modeling.