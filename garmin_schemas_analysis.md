# Garmin Connect API Schema Analysis

## Overview
This document provides a comprehensive analysis of the Garmin Connect API data schemas based on actual data samples collected from the Garmin connector.

## Data Types Available
The Garmin connector can fetch the following data types:
- `activities` - Exercise activities (running, cycling, etc.)
- `sleep` - Sleep data and analytics
- `steps` - Step counting data (15-minute intervals)
- `heart_rate` - Heart rate monitoring data
- `body_battery` - Body battery/energy levels
- `stress` - All-day stress measurements
- `weight` - Weight/body composition data
- `device_info` - Device information and capabilities
- `training_status` - Training status and fitness metrics
- `hrv` - Heart Rate Variability data
- `race_predictions` - Race time predictions
- `floors` - Floors climbed data

## Detailed Schema Analysis

### Activities Schema
**File pattern**: `*_garmin_activities.jsonl`
**Records per fetch**: Variable (depends on activity count in date range)
**Key fields**:
- `activityId` (int64): Unique identifier
- `activityName` (string): User-defined activity name
- `startTimeLocal/GMT` (string): Activity start timestamps
- `activityType` (object): Type information with `typeId`, `typeKey`, `parentTypeId`
- `eventType` (object): Event classification
- `distance` (float): Distance in meters
- `duration` (float): Activity duration in seconds
- `elevationGain/Loss` (float): Elevation changes
- `averageSpeed/maxSpeed` (float): Speed metrics
- `startLatitude/Longitude` (float): GPS coordinates
- `calories/bmrCalories` (float): Calorie metrics
- `averageHR/maxHR` (int): Heart rate metrics
- `steps` (int): Step count during activity
- `avgPower/maxPower` (int): Power metrics for supported activities
- `aerobicTrainingEffect/anaerobicTrainingEffect` (float): Training effect scores
- `vO2MaxValue` (float): VO2 Max measurement
- `splitSummaries` (array): Detailed interval/split data
- `hrTimeInZone_1-5` (float): Time spent in each HR zone
- `powerTimeInZone_1-5` (float): Time spent in each power zone

**Complex nested objects**:
- `activityType`: Contains type hierarchy and metadata
- `eventType`: Activity categorization
- `splitSummaries`: Array of interval data with detailed metrics
- Various time-in-zone arrays for HR and power

### Sleep Schema
**File pattern**: `*_garmin_sleep.jsonl`
**Records per fetch**: One per day with sleep data
**Key fields**:
- `dailySleepDTO` (object): Main sleep data container
  - `id` (int64): Sleep session ID (timestamp-based)
  - `calendarDate` (string): Date in YYYY-MM-DD format
  - `sleepTimeSeconds` (int): Total sleep time
  - `napTimeSeconds` (int): Nap duration
  - `deepSleepSeconds/lightSleepSeconds/remSleepSeconds/awakeSleepSeconds` (int): Sleep stage durations
  - `sleepStartTimestampGMT/Local` (int64): Sleep start timestamps
  - `sleepEndTimestampGMT/Local` (int64): Sleep end timestamps
  - `averageSpO2Value/lowestSpO2Value/highestSpO2Value` (float): Blood oxygen levels
  - `averageRespirationValue` (float): Breathing rate
  - `awakeCount` (int): Number of wake-ups
  - `avgSleepStress` (float): Average stress during sleep
  - `sleepScores` (object): Detailed sleep quality scores
- `sleepMovement` (array): Movement data during sleep
- `wellnessSpO2SleepSummaryDTO` (object): SpO2 sleep summary
- `sleepStress` (array): Stress measurements during sleep

**Complex nested structures**:
- Sleep scores with multiple quality metrics
- Time-series data for movement, SpO2, and stress during sleep
- Multiple timestamp formats (GMT/Local, seconds/milliseconds)

### Body Battery Schema
**File pattern**: `*_garmin_body_battery.jsonl`
**Records per fetch**: One per day
**Key fields**:
- `date` (string): Date in YYYY-MM-DD format
- `charged/drained` (int): Energy gained/lost during day
- `startTimestampGMT/Local` (string): Day start timestamps
- `endTimestampGMT/Local` (string): Day end timestamps
- `bodyBatteryValuesArray` (array): Time-series data [timestamp, battery_level]
- `bodyBatteryValueDescriptorDTOList` (array): Describes array structure
- `bodyBatteryDynamicFeedbackEvent` (object): Feedback messages
- `bodyBatteryActivityEvent` (array): Activities affecting body battery
- `endOfDayBodyBatteryDynamicFeedbackEvent` (object): End-of-day feedback

**Time-series structure**:
- Values stored as arrays of [timestamp, value] pairs
- Descriptor objects explain array column meanings
- Multiple feedback events with contextual information

### Steps Schema
**File pattern**: `*_garmin_steps.jsonl`
**Records per fetch**: 96 per day (15-minute intervals)
**Key fields**:
- `startGMT/endGMT` (string): 15-minute interval timestamps
- `steps` (int): Step count for interval
- `pushes` (int): Wheelchair pushes (accessibility feature)
- `primaryActivityLevel` (string): Activity classification ('sedentary', 'active', 'sleeping')
- `activityLevelConstant` (boolean): Whether activity level was consistent
- `date` (string): Date for organization

### Heart Rate Variability (HRV) Schema
**File pattern**: `*_garmin_hrv.jsonl`
**Records per fetch**: One per day with HRV data
**Key fields**:
- `userProfilePk` (int64): User identifier
- `hrvSummary` (object): Daily HRV summary
  - `calendarDate` (string): Date
  - `weeklyAvg/lastNightAvg` (int): Average HRV values
  - `baseline` (object): Personal HRV baseline ranges
  - `status` (string): HRV status ('BALANCED', 'UNBALANCED', etc.)
- `hrvReadings` (array): Individual HRV measurements
  - `hrvValue` (int): HRV measurement
  - `readingTimeGMT/Local` (string): Measurement timestamp
- Sleep-related timestamps for context

### Floors Schema
**File pattern**: `*_garmin_floors.jsonl`
**Records per fetch**: One per day
**Key fields**:
- `startTimestampGMT/Local` (string): Day timestamps
- `floorsValueDescriptorDTOList` (array): Describes data structure
- `floorValuesArray` (array): Time-series floor data
  - Format: [startTime, endTime, floorsAscended, floorsDescended]
- `date` (string): Date for organization

### Race Predictions Schema
**File pattern**: `*_garmin_race_predictions.jsonl`
**Records per fetch**: One record with current predictions
**Key fields**:
- `userId` (int64): User identifier
- `calendarDate` (string): Date of prediction
- `time5K/time10K/timeHalfMarathon/timeMarathon` (int): Predicted race times in seconds

### Device Info Schema
**File pattern**: `*_garmin_device_info.jsonl`
**Records per fetch**: One per device
**Key fields**: (Very extensive - 100+ capabilities and settings)
- `deviceId` (int64): Unique device identifier
- `productDisplayName` (string): Device name
- `serialNumber` (string): Device serial
- `currentFirmwareVersion` (string): Firmware version
- Extensive capability flags (100+ boolean fields for feature support)
- Device categories and supported sports/activities

## Schema Complexity Observations

### High Variability
- Different data types have vastly different schemas
- Some schemas are simple (race predictions) while others are extremely complex (activities, sleep)
- Nested objects and arrays are common, especially for time-series data

### Time Handling
- Multiple timestamp formats: strings, integers (milliseconds/seconds)
- Both GMT and Local time versions for most timestamps
- Date fields typically in YYYY-MM-DD string format

### Null Handling
- Many fields can be null, especially when data is unavailable
- Some arrays may be empty or contain null values
- Proper null handling crucial for data pipeline

### Data Organization Patterns
- Daily data typically keyed by date
- Activity data keyed by activityId
- Time-series data stored as arrays with descriptor objects
- Extensive use of nested objects for related metrics

## Recommendations for dbt Implementation

### Lake Layer Strategy
1. **Raw staging**: Preserve original JSON structure
2. **Flattening**: Extract nested objects into separate staging models
3. **Type conversion**: Convert timestamps and handle nulls consistently
4. **Date partitioning**: Use date fields for partitioning strategy

### Schema Evolution Handling
- Use flexible JSON parsing to handle schema changes
- Implement tests for critical fields
- Monitor for new fields in API responses

### Data Quality Considerations
- Implement null checks for critical fields
- Validate timestamp formats and ranges
- Check for data completeness across expected date ranges