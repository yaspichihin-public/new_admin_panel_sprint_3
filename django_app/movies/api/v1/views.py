from django.http import HttpResponse
from django.core.handlers.wsgi import WSGIRequest
from django.db.models.query import QuerySet
from django.contrib.postgres.aggregates import ArrayAgg
from django.db.models import Q
from django.http import JsonResponse
from django.views.generic import DetailView
from django.views.generic.list import BaseListView

from movies.models import Filmwork
from movies.enums import RoleInFilm


class MoviesApiMixin:
    model = Filmwork
    http_method_names = ['get']

    def get_queryset(self) -> QuerySet:
        queryset = (
            self.model.objects.all()
            .prefetch_related('genres', 'persons')
            .values(
                'id',
                'title',
                'description',
                'creation_date',
                'rating',
                'type',
            )
            .annotate(
                genres=ArrayAgg(
                    "genres__name",
                    distinct=True,
                )
            )
            .annotate(
                actors=ArrayAgg(
                    'personfilmwork__person__full_name',
                    filter=Q(personfilmwork__role=RoleInFilm.actor),
                    distinct=True,
                )
            )
            .annotate(
                directors=ArrayAgg(
                    'personfilmwork__person__full_name',
                    filter=Q(personfilmwork__role=RoleInFilm.director),
                    distinct=True,
                )
            )
            .annotate(
                writers=ArrayAgg(
                    'personfilmwork__person__full_name',
                    filter=Q(personfilmwork__role=RoleInFilm.writer),
                    distinct=True,
                )
            )
        )
        return queryset

    def render_to_response(self, context, **response_kwargs):
        return JsonResponse(context)


class MoviesListApi(MoviesApiMixin, BaseListView):
    # Кол-во объектов на одной странице при пагинации
    paginate_by = 50

    def get_context_data(self, *, object_list=None, **kwargs):
        queryset = self.get_queryset()
        paginator, page, queryset, is_paginated = self.paginate_queryset(
            queryset,
            self.paginate_by
        )
        return {
            'count': paginator.count,
            'total_pages': paginator.num_pages,
            'prev': page.number - 1 if page.number > 1 else None,
            'next': page.number + 1 if page.number < paginator.num_pages else None,
            'result': list(queryset)
        }


class MoviesDetailApi(MoviesApiMixin, DetailView):
    model = Filmwork
    http_method_names = ['get']

    def get_context_data(self, *, object_list=None, **kwargs):
        return kwargs.get('object', {})


def smoketest(request: WSGIRequest) -> HttpResponse:
    return HttpResponse('OK')
