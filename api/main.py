# api/main.py
from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from api.routers import music

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
        },
    }


@app.get("/health")
async def health():
    return {"status": "healthy"}
