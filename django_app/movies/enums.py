"""Movies enums."""
from django.db import models


class TypeInFilm(models.TextChoices):
    """Enum for film types."""

    movie = 'movie'
    tv_show = 'tv_show'


class RoleInFilm(models.TextChoices):
    """Enum for roles types."""

    actor = 'actor'
    writer = 'writer'
    director = 'director'
