from dataclasses import dataclass


@dataclass
class Movie:
    """Класс для группировки данных об измененных фильмах."""
    id: str
    imdb_rating: float
    title: str
    description: str
    genre: list
    director: list
    actors_names: list
    writers_names: list
    actors: list
    writers: list
