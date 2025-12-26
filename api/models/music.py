# api/models/music.py
from pydantic import BaseModel, Field
from typing import Optional


class TopArtist(BaseModel):
    rank: int
    name: str
    play_count: int
    total_duration: str
    image_url: Optional[str] = None
    external_url: Optional[str] = None


class TopTrack(BaseModel):
    rank: int
    name: str
    artist_name: str
    play_count: int
    total_duration: str
    image_url: Optional[str] = None
    external_url: Optional[str] = None


class TopAlbum(BaseModel):
    rank: int
    name: str
    artist_name: str
    play_count: int
    total_duration: str
    image_url: Optional[str] = None
    external_url: Optional[str] = None
