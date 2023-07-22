"""Movies configuration for admin panel."""
from django.contrib import admin

from movies import models as mdl


class GenreFilmworkInline(admin.TabularInline):
    """Inline table GenreFilmwork for FilmworkAdmin."""

    model = mdl.GenreFilmwork


class PersonFilmworkInline(admin.TabularInline):
    """Inline table PersonFilmwork for FilmworkAdmin."""

    model = mdl.PersonFilmwork


@admin.register(mdl.Genre)
class GenreAdmin(admin.ModelAdmin):
    """Adding the Genre table to the admin panel."""

    list_display = ('name', 'description')
    search_fields = ('name', 'description', 'id')


@admin.register(mdl.Filmwork)
class FilmworkAdmin(admin.ModelAdmin):
    """Adding the Filmwork table to the admin panel."""

    inlines = (GenreFilmworkInline, PersonFilmworkInline)
    list_display = ('title', 'type', 'creation_date', 'rating', 'created', 'modified')
    list_filter = ('type',)
    search_fields = ('title', 'description', 'id')


@admin.register(mdl.Person)
class PersonAdmin(admin.ModelAdmin):
    """Adding the Person table to the admin panel."""

    list_display = ('full_name',)
    search_fields = ('full_name', 'id')
