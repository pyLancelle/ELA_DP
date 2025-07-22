# dbt Incremental Strategy for Garmin Lake Models

## Overview
This document outlines the strategy for converting Garmin lake models from full-refresh tables to incremental models for performance and cost optimization.

## Current State Analysis

### Current Configuration
```yaml
# dbt_project.yml
models:
  dbt_dataplatform:
    lake:
      +materialized: table  # ❌ Full refresh on every run
    hub:
      +materialized: view   # ✅ Keep as views
    product:
      +materialized: view   # ✅ Keep as views
```

### Data Volume Projections (Post-Historical Backfill)
- **Total lake records**: ~110,000 records (18+ months)
- **Daily additions**: ~300 records/day (12 data types × ~25 records avg)
- **JSON extraction cost**: High computational overhead for complex nested data

### Current Problems
1. **Full refresh inefficiency**: Re-processes ALL 110K records daily
2. **Expensive JSON operations**: Complex extractions on every run
3. **Slow build times**: 5-10 minutes for lake layer alone
4. **BigQuery costs**: Unnecessary processing of historical data

## Incremental Strategy

### Why Incremental is Perfect for Garmin Data
1. **Historical immutability**: Past Garmin data never changes
2. **Daily additions**: Only new data needs processing
3. **Expensive transformations**: JSON extractions should be done once
4. **Natural incremental key**: `dp_inserted_at` from ingestion

### Proposed Configuration
```yaml
# dbt_project.yml
models:
  dbt_dataplatform:
    lake:
      +materialized: incremental
      +incremental_strategy: append
      +on_schema_change: sync_all_columns
    hub:
      +materialized: view   # No change
    product:
      +materialized: view   # No change
```

## Technical Implementation

### Incremental Logic Pattern
Each lake model will follow this pattern:

```sql
{{ config(
    materialized='incremental',
    incremental_strategy='append',
    unique_key=['dp_inserted_at', 'data_type', 'source_file']
) }}

SELECT
    -- All existing JSON extractions
    JSON_EXTRACT_SCALAR(raw_data, '$.field') AS field,
    -- ... rest of transformations
    dp_inserted_at,
    source_file

FROM {{ source('garmin', 'staging_garmin_raw') }}
WHERE data_type = 'activities'  -- or respective data type

{% if is_incremental() %}
  -- Only process new data since last run
  AND dp_inserted_at > (SELECT MAX(dp_inserted_at) FROM {{ this }})
{% endif %}
```

### Unique Key Strategy
Using compound key for deduplication:
- `dp_inserted_at`: Ingestion timestamp
- `data_type`: Garmin data type
- `source_file`: Source filename

This ensures no duplicate processing while allowing for re-ingestion if needed.

## Model-by-Model Implementation

### High-Volume Models (Priority 1)
These models benefit most from incremental processing:

1. **`lake_garmin__steps.sql`**
   - ~60K records (15-min intervals)
   - Simple structure, high volume

2. **`lake_garmin__activities.sql`** 
   - Complex JSON extractions (50+ fields)
   - Expensive nested object processing

3. **`lake_garmin__body_battery.sql`**
   - Time-series array processing
   - Complex nested structures

### Medium-Volume Models (Priority 2)
4. **`lake_garmin__sleep.sql`**
5. **`lake_garmin__hrv.sql`**
6. **`lake_garmin__floors.sql`**

### Low-Volume Models (Priority 3)
7. **`lake_garmin__weight.sql`**
8. **`lake_garmin__device_info.sql`**
9. **`lake_garmin__training_status.sql`**
10. **`lake_garmin__race_predictions.sql`**

## Implementation Steps

### Step 1: Update dbt_project.yml
```yaml
models:
  dbt_dataplatform:
    lake:
      garmin:
        +materialized: incremental
        +incremental_strategy: append
        +on_schema_change: sync_all_columns
```

### Step 2: Add Incremental Logic to Each Model
Template for each lake model:
```sql
-- Add at top of each lake_garmin__*.sql file
{{ config(
    materialized='incremental',
    incremental_strategy='append'
) }}

-- Add at end of WHERE clause
{% if is_incremental() %}
  AND dp_inserted_at > (SELECT MAX(dp_inserted_at) FROM {{ this }})
{% endif %}
```

### Step 3: Initial Full Refresh
```bash
# First run processes all historical data
dbt run --select lake.garmin --full-refresh
```

### Step 4: Test Incremental Behavior
```bash
# Subsequent runs should only process new data
dbt run --select lake.garmin
```

## Error Handling & Edge Cases

### Schema Changes
- `on_schema_change: sync_all_columns` handles new JSON fields
- Backwards compatible with existing data

### Data Quality Issues
- Duplicate detection via unique_key
- Failed runs can be retried safely
- `--full-refresh` available for complete rebuild

### Historical Data Integration
- Incremental logic works seamlessly with historical backfill
- HISTORICAL files will be processed once, then ignored
- Daily files processed incrementally

## Performance Benefits

### Expected Improvements
- **Build time**: 5-10 minutes → 30-60 seconds daily
- **BigQuery costs**: ~95% reduction in daily processing
- **Query performance**: Same (materialized tables)
- **Storage**: Minimal increase (only new data)

### Resource Optimization
```yaml
# Optional: Resource allocation for large initial run
models:
  dbt_dataplatform:
    lake:
      garmin:
        +materialized: incremental
        +incremental_strategy: append
        +cluster_by: ['data_type', 'dp_inserted_at']
        +partition_by: {
          'field': 'dp_inserted_at',
          'data_type': 'timestamp'
        }
```

## Testing Strategy

### Unit Tests
```sql
-- Test incremental logic
SELECT COUNT(*) 
FROM {{ ref('lake_garmin__activities') }}
WHERE dp_inserted_at = '2025-07-21'  -- Today's data only
```

### Integration Tests
```sql
-- Validate no data loss during conversion
WITH pre_conversion AS (
  SELECT data_type, COUNT(*) as old_count
  FROM staging_garmin_raw GROUP BY 1
),
post_conversion AS (
  SELECT 'activities' as data_type, COUNT(*) as new_count
  FROM {{ ref('lake_garmin__activities') }}
  UNION ALL
  -- ... other data types
)
SELECT * FROM pre_conversion p 
JOIN post_conversion pc ON p.data_type = pc.data_type
WHERE p.old_count != pc.new_count
```

## Rollback Plan
If issues occur during conversion:

1. **Immediate**: `dbt run --select lake.garmin --full-refresh`
2. **Revert config**: Change back to `materialized: table`
3. **Debug**: Analyze incremental logic issues
4. **Retry**: Fix and re-implement incrementally

## Success Criteria
- [ ] All 10 lake models converted to incremental
- [ ] Initial full refresh completes successfully
- [ ] Daily incremental runs process only new data
- [ ] Build time reduced by >80%
- [ ] Data quality maintained (record counts match)
- [ ] Hub/Product models work unchanged
- [ ] No data loss during conversion

## Timeline
- **Setup**: 30 minutes (config + first model)
- **Conversion**: 2 hours (all 10 models)
- **Testing**: 1 hour (validation + edge cases)
- **Deployment**: 30 minutes (full refresh run)

**Total**: ~4 hours for complete incremental conversion

## Maintenance
- **Daily**: Automatic incremental processing
- **Weekly**: Monitor build times and costs
- **Monthly**: Validate data completeness
- **Quarterly**: Consider partitioning optimizations

This incremental strategy provides massive performance gains while maintaining data quality and enabling seamless integration with the historical backfill data.