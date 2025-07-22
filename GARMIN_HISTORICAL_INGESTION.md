# Garmin Historical Data Ingestion Implementation Plan

## Overview
This document outlines the implementation plan for ingesting historical Garmin data using the existing ingestion pipeline with minimal modifications.

## Current Architecture
- **Backfill**: Historical files created in `data/historical/garmin/` with `HISTORICAL` prefix
- **GCS Upload**: Historical files need to be uploaded to `gs://ela-dp-{env}/garmin/landing/`
- **Existing Pipeline**: `garmin_ingest.py` processes files from GCS landing folder
- **Universal Schema**: All data stored as JSON in `staging_garmin_raw` table

## Implementation Strategy

### Phase 1: Upload Historical Files to GCS
**Task**: Create script to upload historical files to GCS landing folder

```bash
# Upload historical files to GCS landing folder
gsutil -m cp data/historical/garmin/*HISTORICAL*.jsonl gs://ela-dp-dev/garmin/landing/
# or
gsutil -m cp data/historical/garmin/*HISTORICAL*.jsonl gs://ela-dp-prd/garmin/landing/
```

**Key Points**:
- Keep `HISTORICAL` prefix in filenames for traceability
- Upload to same `garmin/landing/` folder as daily files
- Use `-m` flag for parallel uploads (faster for many files)

### Phase 2: Modify garmin_ingest.py (Minimal Changes)
**File**: `src/connectors/garmin/garmin_ingest.py`

**Required Changes**:

1. **Update file detection logic** in `list_gcs_files()`:
```python
# Current: Only processes daily files
# Update: Process both daily AND HISTORICAL files
def list_gcs_files(bucket_name: str, prefix: str = "garmin/landing/") -> list:
    # ... existing logic ...
    return [
        f"gs://{bucket_name}/{blob.name}"
        for blob in blobs
        if blob.name.endswith(".jsonl")  # This already handles HISTORICAL files
    ]
```

2. **File type detection** in `detect_file_type()`:
```python
# Current logic already works - just extracts data type from filename
# HISTORICAL files: "2024_01_01_to_2024_01_31_HISTORICAL_garmin_activities.jsonl"
# Will correctly detect "activities" as data_type
```

3. **Archive path handling** in `move_gcs_file()`:
```python
# Current: moves to garmin/archive/
# Update: Keep same archive path for both daily and historical
# HISTORICAL files will be moved to garmin/archive/ after processing
```

**No Changes Needed**:
- ✅ `load_jsonl_as_raw_json()` - Already handles any JSON structure
- ✅ `get_universal_schema()` - Already perfect for historical data
- ✅ BigQuery table creation - Uses same `staging_garmin_raw` table
- ✅ Error handling and logging - Works for any file type

### Phase 3: Process Historical Data
**Execution**:
```bash
# Run existing ingestion script (dev environment)
python -m src.connectors.garmin.garmin_ingest --env dev

# Run existing ingestion script (prod environment)  
python -m src.connectors.garmin.garmin_ingest --env prd
```

**What Happens**:
1. Script scans `gs://ela-dp-{env}/garmin/landing/` 
2. Finds both daily files AND historical files
3. Processes each file with universal JSON schema
4. Inserts into `staging_garmin_raw` with:
   - `raw_data`: Complete JSON record
   - `data_type`: Extracted from filename (activities, sleep, etc.)
   - `dp_inserted_at`: Current ingestion timestamp  
   - `source_file`: Full filename (including HISTORICAL marker)
5. Moves processed files to `garmin/archive/`

### Phase 4: Validate Data Quality
**dbt Validation Queries**:
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

-- Check date coverage for activities
SELECT 
    DATE(JSON_EXTRACT_SCALAR(raw_data, '$.startTimeGMT')) as activity_date,
    COUNT(*) as activities
FROM `polar-scene-465223-f7.dp_lake_dev.staging_garmin_raw`
WHERE data_type = 'activities'
    AND source_file LIKE '%HISTORICAL%'
GROUP BY 1
ORDER BY 1;
```

### Phase 5: Run dbt Models
**Command**:
```bash
cd src/dbt_dataplatform/
dbt run --select lake.garmin
```

**Expected Outcome**:
- Lake models will automatically process historical + daily data
- No dbt changes needed - models designed for this universal schema
- Historical data becomes immediately available in all models

## File Naming Convention Understanding
**Backfill Files**: `YYYY_MM_DD_to_YYYY_MM_DD_HISTORICAL_garmin_[type].jsonl`
**Daily Files**: `YYYY_MM_DD_HH_MM_garmin_[type].jsonl`

**Data Type Extraction**:
- Both patterns end with `garmin_[type].jsonl`
- `detect_file_type()` function already handles this correctly
- No code changes needed for type detection

## Risk Mitigation
1. **Test Environment First**: Always test with dev environment
2. **Backup Current Data**: Take BigQuery snapshot before historical ingestion
3. **Monitor Ingestion**: Watch logs for any processing errors
4. **Validate dbt Models**: Ensure lake models handle the volume increase
5. **Archive Management**: Historical files will be archived normally

## Success Criteria
- [ ] Historical files uploaded to GCS landing folder
- [ ] garmin_ingest.py processes historical files successfully  
- [ ] Data appears in `staging_garmin_raw` with HISTORICAL marker in `source_file`
- [ ] dbt lake models run successfully with combined data
- [ ] Historical files moved to GCS archive folder
- [ ] Date range validation shows full 2024-2025 coverage

## Rollback Plan
If issues occur:
1. **Stop ingestion**: Interrupt the ingestion process
2. **Remove historical data**: Delete records where `source_file LIKE '%HISTORICAL%'`
3. **Restore files**: Move historical files back to landing folder from archive
4. **Debug and retry**: Fix issues and re-run process

## Implementation Timeline
1. **Upload files to GCS**: 10-15 minutes (depending on file count)
2. **Run ingestion**: 30-60 minutes (depending on data volume)  
3. **Run dbt models**: 10-20 minutes
4. **Validation**: 10-15 minutes

**Total**: ~2 hours for complete historical ingestion

## Notes
- **No pipeline duplication**: Uses exact same ingestion logic
- **Maintainability**: Single codebase for daily + historical
- **Traceability**: HISTORICAL marker preserved in source_file column
- **Performance**: BigQuery handles the volume increase well
- **Future-proof**: Any new dbt models automatically include historical data