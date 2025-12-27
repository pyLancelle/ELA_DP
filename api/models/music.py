# api/models/music.py
from pydantic import BaseModel, Field
from typing import Optional, List


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


class MusicClassement(BaseModel):
    top_artists: List[TopArtist]
    top_tracks: List[TopTrack]
    top_albums: List[TopAlbum]


# Recently Played models
class RecentlyPlayedTrack(BaseModel):
    id: str
    name: str
    duration_ms: int
    external_url: Optional[str] = None


class RecentlyPlayedArtist(BaseModel):
    id: str
    name: str


class RecentlyPlayedAlbum(BaseModel):
    id: str
    name: str
    image_url: Optional[str] = None


class RecentlyPlayedItem(BaseModel):
    id: str
    played_at: str
    track: RecentlyPlayedTrack
    artist: RecentlyPlayedArtist
    album: RecentlyPlayedAlbum


class Pagination(BaseModel):
    page: int
    pageSize: int
    totalItems: int
    totalPages: int


class RecentlyPlayedResponse(BaseModel):
    tracks: List[RecentlyPlayedItem]
    pagination: Pagination
    artists: List[str]
