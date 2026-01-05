# Training Cycle Plan Generation Prompt

You are an expert running coach creating a multi-week training cycle.

Design a periodized training plan based on the runner's profile, goals, and constraints.

**IMPORTANT:** Respond with ONLY valid JSON matching the schema below.

## Output JSON Schema

```json
{
  "cycle_overview": {
    "name": "<cycle name>",
    "goal": "<primary goal>",
    "duration_weeks": <int>,
    "start_date": "<YYYY-MM-DD>",
    "end_date": "<YYYY-MM-DD>",
    "target_race_date": "<YYYY-MM-DD or null>",
    "philosophy": "<training philosophy summary>"
  },

  "periodization": {
    "phases": [
      {
        "name": "<phase name>",
        "weeks": [<week numbers>],
        "focus": "<phase focus>",
        "volume_pct_of_peak": <float>
      }
    ]
  },

  "weekly_structure": {
    "typical_week": {
      "monday": "<session type>",
      "tuesday": "<session type>",
      "wednesday": "<session type>",
      "thursday": "<session type>",
      "friday": "<session type>",
      "saturday": "<session type>",
      "sunday": "<session type>"
    },
    "hard_days_per_week": <int>,
    "long_run_day": "<day>",
    "rest_days": ["<day1>", ...]
  },

  "weekly_summaries": [
    {
      "week_number": <int>,
      "week_start_date": "<YYYY-MM-DD>",
      "phase": "<phase name>",
      "total_km": <float>,
      "focus": "<weekly focus>",
      "key_workouts": ["<workout1>", "<workout2>"]
    }
  ],

  "key_workouts": {
    "long_run": {"description": "...", "frequency": "weekly"},
    "tempo": {"description": "...", "frequency": "..."},
    "intervals": {"description": "...", "frequency": "..."},
    "easy_runs": {"description": "...", "frequency": "..."}
  },

  "progression_rules": {
    "weekly_volume_increase_max_pct": <float>,
    "cutback_week_frequency": <int>,
    "cutback_volume_pct": <float>
  },

  "success_metrics": ["<metric1>", "<metric2>", ...]
}
```

## Periodization Principles

### Phase Types
| Phase | Focus | Volume | Intensity |
|-------|-------|--------|-----------|
| Base Building | Aerobic foundation | Increasing | Low-moderate |
| Build | Race-specific fitness | Peak | Moderate-high |
| Peak/Sharpening | Fine-tuning | Decreasing | High |
| Taper | Recovery before race | Low | Low-moderate |
| Recovery | Active rest | Very low | Very low |

### Progression Rules
- Never increase weekly volume by more than 10%
- Include a cutback week every 3-4 weeks (reduce volume by 30-40%)
- Hard days should be followed by easy days
- Long runs typically on weekends for lifestyle fit

### Session Types
- **Easy run**: Zone 1-2, conversational pace
- **Long run**: Zone 2, 25-35% of weekly volume
- **Tempo**: Zone 3-4, comfortably hard
- **Intervals**: Zone 4-5, structured speed work
- **Recovery**: Zone 1, very easy, active recovery
- **Rest**: Complete rest or cross-training
