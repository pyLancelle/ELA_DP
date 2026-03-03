# api/main.py
from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from api.routers import music, homepage, activities, artist_focus

app = FastAPI(
    title="ELA DataPlatform API",
    description="API pour accéder aux données dbt transformées",
    version="1.0.0",
)

# CORS pour permettre les appels depuis Next.js
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],  # À restreindre en prod
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Inclure les routers
app.include_router(music.router, prefix="/api/music", tags=["music"])
app.include_router(homepage.router, prefix="/api/homepage", tags=["homepage"])
app.include_router(activities.router, prefix="/api/activities", tags=["activities"])
app.include_router(artist_focus.router, prefix="/api/artist-focus", tags=["artist-focus"])


@app.get("/")
async def root():
    return {
        "message": "ELA DataPlatform API",
        "version": "1.0.0",
        "endpoints": {
            "music_classement": "/api/music/music-classement",
            "top_artists": "/api/music/top-artists",
            "top_tracks": "/api/music/top-tracks",
            "top_albums": "/api/music/top-albums",
            "homepage": "/api/homepage",
            "homepage_music_time_daily": "/api/homepage/music-time-daily",
            "homepage_race_prediction": "/api/homepage/race-prediction",
            "homepage_running_weekly": "/api/homepage/running-weekly",
            "homepage_running_weekly_volume": "/api/homepage/running-weekly-volume",
            "homepage_sleep_stages": "/api/homepage/sleep-stages",
            "homepage_top_artists": "/api/homepage/top-artists",
            "homepage_top_tracks": "/api/homepage/top-tracks",
            "activities_recent": "/api/activities/recent",
            "activities_list": "/api/activities/list",
            "artist_focus_list": "/api/artist-focus/artists",
            "artist_focus_profile": "/api/artist-focus/{artist_id}",
            "artist_focus_overview": "/api/artist-focus/{artist_id}/overview",
            "artist_focus_tracks": "/api/artist-focus/{artist_id}/tracks",
            "artist_focus_albums": "/api/artist-focus/{artist_id}/albums",
            "artist_focus_calendar": "/api/artist-focus/{artist_id}/calendar",
            "artist_focus_heatmap": "/api/artist-focus/{artist_id}/heatmap",
            "artist_focus_evolution": "/api/artist-focus/{artist_id}/evolution",
        },
    }


@app.get("/health")
async def health():
    return {"status": "healthy"}
