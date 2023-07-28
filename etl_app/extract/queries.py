def get_filmworks_query(state_modified=None, batch=100) -> str:
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


def get_persons_query(state_modified=None, batch=100) -> str:
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


def get_genres_query(state_modified=None, batch=100) -> str:
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
