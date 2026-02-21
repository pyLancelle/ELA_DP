# api/main.py
from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from api.routers import music, homepage, activities

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
        },
    }


@app.get("/health")
async def health():
    return {"status": "healthy"}
