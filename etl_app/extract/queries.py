def get_filmworks_query(state_modified: str = '', batch: int = 100) -> str:
    """Подготовка SQL запроса для получения измененных фильмов.

    :param state_modified: Последняя обработанная запись.
    :param batch: Сколько записей за раз запрашивать.
    :return: Подготовленный SQL запрос.
    """
    if state_modified:
        filmworks_query = f"""
        select id, modified from content.film_work
        where modified > '{state_modified}'
        order by modified limit {batch};
        """
    else:
        filmworks_query = f"""
        select id, modified from content.film_work
        order by modified limit {batch};
        """
    return filmworks_query


def get_persons_query(state_modified: str = '', batch: int = 100) -> str:
    """Подготовка SQL запроса для получения измененных участников фильмов.

    :param state_modified: Последняя обработанная запись.
    :param batch: Сколько записей за раз запрашивать.
    :return: Подготовленный SQL запрос.
    """
    if state_modified:
        persons_query = f"""
        select id, modified from content.person
        where modified > '{state_modified}'
        order by modified limit {batch};
        """
    else:
        persons_query = f"""
        select id, modified from content.person
        order by modified limit {batch};
        """
    return persons_query


def get_genres_query(state_modified: str = '', batch: int = 100) -> str:
    """Подготовка SQL запроса для получения измененных жанров фильмов.

    :param state_modified: Последняя обработанная запись.
    :param batch: Сколько записей за раз запрашивать.
    :return: Подготовленный SQL запрос.
    """
    if state_modified:
        persons_query = f"""
        select id, modified from content.genre
        where modified > '{state_modified}'
        order by modified limit {batch};
        """
    else:
        persons_query = f"""
        select id, modified from content.genre
        order by modified limit {batch};
        """
    return persons_query


def get_filmworks_query_by_person_uuid(persons: str) -> str:
    """Подготовка SQL запроса для получения фильмов на которые повлияло изменение их участников.

    :param persons: Строка UUID участников фильмов через запятую.
    :return: Подготовленный SQL запрос.
    """
    filmworks_query = f"""
    select fw.id, modified
    from content.film_work fw
    left join content.person_film_work pfw 
        on pfw.film_work_id = fw.id
    where pfw.person_id in ('{persons}')
    order by fw.modified;
    """
    return filmworks_query


def get_filmworks_query_by_genre_uuid(genres: str) -> str:
    """Подготовка SQL запроса для получения фильмов на которые повлияло изменение жанра.

    :param genres: Строка UUID жанров фильмов через запятую.
    :return: Подготовленный SQL запрос.
    """
    filmworks_query = f"""
    select fw.id, modified
    from content.film_work fw
    left join content.genre_film_work gfw 
        on gfw.film_work_id = fw.id
    where gfw.genre_id in ('{genres}')
    order by fw.modified;
    """
    return filmworks_query


def get_filmworks_additional_query_by_filmwork_uuid(filmworks: str) -> str:
    """Подготовка SQL запроса для получения дополнительной информации по фильмам, которые изменились.

    :param filmworks: Строка UUID фильмов через запятую, для которых требуется собрать дополнительную информацию.
    :return: Подготовленный SQL запрос.
    """
    filmworks_additional_query = f"""
    select
        fw.id as fw_id, 
        fw.title, 
        fw.description, 
        fw.rating, 
        fw.type, 
        fw.created, 
        fw.modified, 
        pfw.role, 
        p.id, 
        p.full_name,
        g.name
    from content.film_work fw
    left join content.person_film_work pfw on pfw.film_work_id = fw.id
    left join content.person p on p.id = pfw.person_id
    left join content.genre_film_work gfw on gfw.film_work_id = fw.id
    left join content.genre g on g.id = gfw.genre_id
    where fw.id in ('{filmworks}');
    """
    return filmworks_additional_query
