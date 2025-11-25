{{
  config(
      tags=['garmin', 'lake']
  )
}}

SELECT
    activityId,
    STRUCT(
        activityName,
        description,
        activityType,
        eventType,
        sportTypeId,
        locationName,
        courseId,
        workoutId,
        calendarEventUuid,
        userRoles,
        privacy,
        lapCount,
        totalSets,
        activeSets,
        totalReps,
        summarizedExerciseSets
    ) AS activity_details,
    STRUCT(
        startTimeLocal,
        startTimeGMT,
        endTimeGMT,
        beginTimestamp,
        timeZoneId
    ) AS timestamps,
    STRUCT(
        distance,
        duration,
        elapsedDuration,
        movingDuration,
        calories,
        bmrCalories,
        steps,
        waterEstimated,
        waterConsumed,
        caloriesConsumed,
        moderateIntensityMinutes,
        vigorousIntensityMinutes,
        differenceBodyBattery,
        vO2MaxValue,
        activityTrainingLoad,
        minActivityLapDuration
    ) AS metrics,
    STRUCT(
        averageSpeed,
        maxSpeed,
        avgGradeAdjustedSpeed,
        maxVerticalSpeed,
        avgVerticalSpeed
    ) AS speed,
    STRUCT(
        elevationGain,
        elevationLoss,
        minElevation,
        maxElevation
    ) AS elevation,
    STRUCT(
        averageHR,
        maxHR,
        hrTimeInZone_1,
        hrTimeInZone_2,
        hrTimeInZone_3,
        hrTimeInZone_4,
        hrTimeInZone_5
    ) AS heart_rate,
    STRUCT(
        avgPower,
        maxPower,
        normPower,
        powerTimeInZone_1,
        powerTimeInZone_2,
        powerTimeInZone_3,
        powerTimeInZone_4,
        powerTimeInZone_5
    ) AS power,
    STRUCT(
        averageRunningCadenceInStepsPerMinute,
        maxRunningCadenceInStepsPerMinute,
        maxDoubleCadence
    ) AS cadence,
    STRUCT(
        avgVerticalOscillation,
        avgGroundContactTime,
        avgStrideLength,
        avgVerticalRatio,
        minRespirationRate,
        maxRespirationRate,
        avgRespirationRate
    ) AS running_dynamics,
    STRUCT(
        ownerId,
        ownerDisplayName,
        ownerFullName,
        ownerProfileImageUrlSmall,
        ownerProfileImageUrlMedium,
        ownerProfileImageUrlLarge,
        userPro
    ) AS owner,
    STRUCT(
        startLatitude,
        startLongitude,
        endLatitude,
        endLongitude
    ) AS location,
    STRUCT(
        aerobicTrainingEffect,
        anaerobicTrainingEffect,
        trainingEffectLabel,
        aerobicTrainingEffectMessage,
        anaerobicTrainingEffectMessage
    ) AS training_effect,
    STRUCT(
        fastestSplit_1000,
        fastestSplit_1609,
        fastestSplit_5000,
        fastestSplit_10000,
        fastestSplit_21098,
        fastestSplit_42195
    ) AS fastest_splits,
    STRUCT(
        hasPolyline,
        hasImages,
        hasVideo,
        hasSplits,
        hasHeatMap,
        qualifyingDive,
        pr,
        parent,
        decoDive,
        elevationCorrected,
        atpActivity,
        purposeful,
        favorite,
        manualActivity,
        autoCalcCalories
    ) AS flags,
    STRUCT(
        data_type,
        _dp_inserted_at,
        _source_file,
        deviceId,
        manufacturer,
        minTemperature,
        maxTemperature,
        summarizedDiveInfo,
        splitSummaries
    ) AS metadata
FROM {{ source('garmin','activities') }}
QUALIFY
    ROW_NUMBER() OVER (PARTITION BY activityid ORDER BY _dp_inserted_at DESC) = 1
