# api/models/artist_focus.py
from pydantic import BaseModel
from typing import Optional, List, Any


class ArtistFocusOverview(BaseModel):
    artist_id: str
    artist_name: str
    artist_url: Optional[str] = None
    image_url: Optional[str] = None
    image_url_medium: Optional[str] = None
    genres: Optional[Any] = None
    spotify_popularity: Optional[int] = None
    follower_count: Optional[int] = None
    total_plays: int
    total_duration: str
    total_duration_ms: int
    unique_tracks: int
    unique_albums: int
    first_heard: str
    last_heard: str
    days_with_listens: int
    days_since_discovery: int
    consistency_score: float
    avg_plays_per_active_day: float


class ArtistFocusTrack(BaseModel):
    artist_id: str
    track_rank: int
    track_id: str
    track_name: str
    album_name: Optional[str] = None
    album_image_url: Optional[str] = None
    track_url: Optional[str] = None
    play_count: int
    total_duration: str
    total_duration_ms: int
    first_played_at: str
    last_played_at: str
    pct_of_artist_time: float


class ArtistFocusAlbum(BaseModel):
    artist_id: str
    album_id: str
    album_name: str
    album_image_url: Optional[str] = None
    album_url: Optional[str] = None
    album_type: Optional[str] = None
    release_date: Optional[str] = None
    total_tracks: int
    tracks_heard: int
    total_plays: int
    total_duration: str
    total_duration_ms: int
    first_played_at: str
    last_played_at: str
    completion_rate: float
    listen_depth: str
    artist_names: Optional[str] = None


class ArtistFocusCalendarDay(BaseModel):
    artist_id: str
    listen_date: str
    play_count: int
    total_duration_ms: int
    total_duration: str


class ArtistFocusHeatmapCell(BaseModel):
    artist_id: str
    hour_of_day: int
    day_of_week: int
    day_name: str
    play_count: int
    total_duration_ms: int


class ArtistFocusMonthly(BaseModel):
    artist_id: str
    year_month: str
    play_count: int
    unique_tracks: int
    total_duration_ms: int
    total_duration: str


class ArtistSummary(BaseModel):
    artist_id: str
    artist_name: str
    image_url: Optional[str] = None
    total_plays: int
    total_duration: str


class ArtistFocusProfile(BaseModel):
    overview: ArtistFocusOverview
    top_tracks: List[ArtistFocusTrack]
    albums: List[ArtistFocusAlbum]
    calendar: List[ArtistFocusCalendarDay]
    heatmap: List[ArtistFocusHeatmapCell]
    evolution: List[ArtistFocusMonthly]
