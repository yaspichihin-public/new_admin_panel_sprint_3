"""Movies models."""
import uuid

from django.db import models
from django.utils.translation import gettext_lazy as _

from movies.enums import RoleInFilm, TypeInFilm
from movies.validators import rating_validator


class TimeStampedMixin(models.Model):
    """Mixin for adding created and modified timestamp."""

    created = models.DateTimeField(
        _('created'),
        auto_now_add=True,
    )
    modified = models.DateTimeField(
        _('modified'),
        auto_now=True,
    )

    class Meta:
        abstract = True


class UUIDMixin(models.Model):
    """Mixin for adding uuid."""

    id = models.UUIDField(
        primary_key=True,
        default=uuid.uuid4,
        editable=False,
    )

    class Meta:
        abstract = True


class Genre(UUIDMixin, TimeStampedMixin):
    """Description for the Genre table."""

    name = models.CharField(
        _('name'),
        max_length=255,
        unique=True,
    )
    description = models.TextField(
        _('description'),
        blank=True,
        default='',
    )

    class Meta:
        db_table = "content\".\"genre"
        verbose_name = _('genre')
        verbose_name_plural = _('genres')

    def __str__(self):
        return self.name


class Person(UUIDMixin, TimeStampedMixin):
    """Description for the Person table."""

    full_name = models.CharField(
        _('full_name'),
        max_length=255,
    )

    class Meta:
        db_table = "content\".\"person"
        verbose_name = _('person')
        verbose_name_plural = _('persons')

    def __str__(self):
        return self.full_name


class Filmwork(UUIDMixin, TimeStampedMixin):
    """Description for the Filmwork table."""

    title = models.CharField(
        _('title'),
        max_length=255,
        unique=True,
    )
    description = models.TextField(
        _('description'),
        blank=True,
        default='',
    )
    creation_date = models.DateField(
        _('creation_date'),
        blank=True,
        null=True,
    )
    rating = models.FloatField(
        _('rating'),
        blank=True,
        null=True,
        validators=rating_validator,
    )
    type = models.CharField(
        _('type'),
        max_length=20,
        choices=TypeInFilm.choices,
        default=TypeInFilm.movie,
    )
    genres = models.ManyToManyField(
        Genre,
        through='GenreFilmwork',
    )
    persons = models.ManyToManyField(
        Person,
        through='PersonFilmwork',
    )

    class Meta:
        db_table = "content\".\"film_work"
        verbose_name = _('film_work')
        verbose_name_plural = _('film_works')

    def __str__(self):
        return self.title


class GenreFilmwork(UUIDMixin):
    """
    Description for the GenreFilmwork table.

    This table is for the many-to-many relationship.
    Between Genres and Filmworks.
    """

    genre = models.ForeignKey(
        'Genre',
        verbose_name=_('genre'),
        on_delete=models.CASCADE,
    )
    film_work = models.ForeignKey(
        'Filmwork',
        verbose_name=_('film_work'),
        on_delete=models.CASCADE,
    )
    created = models.DateTimeField(
        auto_now_add=True,
    )

    class Meta:
        db_table = "content\".\"genre_film_work"
        verbose_name = _('genre_film_work')
        verbose_name_plural = _('genre_film_works')
        unique_together = ('genre', 'film_work')

    def __str__(self):
        return f'Genre: {self.genre}, Film: {self.film_work}'


class PersonFilmwork(UUIDMixin):
    """
    Description for the PersonFilmwork table.

    This table is for the many-to-many relationship.
    Between Persons and Filmworks.
    """

    person = models.ForeignKey(
        'Person',
        verbose_name=_('person'),
        on_delete=models.CASCADE,
    )
    film_work = models.ForeignKey(
        'Filmwork',
        verbose_name=_('film_work'),
        on_delete=models.CASCADE,
    )
    role = models.CharField(
        _('role'),
        max_length=64,
        choices=RoleInFilm.choices,
        default=RoleInFilm.actor,
    )
    created = models.DateTimeField(
        auto_now_add=True,
    )

    class Meta:
        db_table = "content\".\"person_film_work"
        verbose_name = _('person_film_work')
        verbose_name_plural = _('person_film_works')
        unique_together = ('person', 'film_work', 'role')

    def __str__(self):
        return f'Person: {self.person}, Role: {self.role}, Film: {self.film_work}'
