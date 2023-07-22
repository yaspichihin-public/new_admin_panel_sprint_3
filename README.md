<h2>Запуск проекта:</h2>
docker compose  --file .\docker-compose.yml up --build

Проверка запуска проекта:\
docker compose ps

| NAME  | IMAGE    | COMMAND                | SERVICE | CREATED    | STATUS              | PORTS              |
|-------|----------|------------------------|---------|------------|---------------------|--------------------|
| app   | ...app   | "/opt/django_app/doc…" | app     | 57 sec ago | Up 50 sec (healthy) | 8000/tcp           |
| db    | ...db    | "docker-entrypoint.s…" | db      | 57 sec ago | Up 56 sec (healthy) | 5432/tcp           |
| nginx | ...nginx | "/docker-entrypoint.…" | nginx   | 57 sec ago | Up 45 sec (healthy) | 0.0.0.0:80->80/tcp |

Проверки доступности админ панели:\
http://127.0.0.1/admin/login/?next=/admin/

Проверка доступности api:\
Получить список фильмов: http://127.0.0.1/api/v1/movies/ \
Получить фильм ао id: http://127.0.0.1/api/v1/movies/01cd80e2-5db8-4914-9a80-74f15a3a1a24


<h2>Запуск проекта в режиме разработчика:</h2>
docker compose  --file .\docker-compose.yml up --build


Проверка запуска проекта:\
docker compose ps

| NAME    | IMAGE      | COMMAND                | SERVICE | CREATED    | STATUS              | PORTS                          |
|---------|------------|------------------------|---------|------------|---------------------|--------------------------------|
| app     | ...app     | "/opt/django_app/doc…" | app     | 57 sec ago | Up 50 sec (healthy) | 0.0.0.0:8000->8000/tcp         |
| db      | ...db      | "docker-entrypoint.s…" | db      | 57 sec ago | Up 56 sec (healthy) | 0.0.0.0:5432->5432/tcp         |
| nginx   | ...nginx   | "/docker-entrypoint.…" | nginx   | 57 sec ago | Up 45 sec (healthy) | 0.0.0.0:80->80/tcp             |
| openapi | ...openapi | "/docker-entrypoint.…" | openapi | 57 sec ago | Up 45 sec (healthy) | 80/tcp, 0.0.0.0:8080->8080/tcp |

Пересборка после изменения в проекте:\
docker compose  --file .\docker-compose.dev.yml down && docker compose  --file .\docker-compose.dev.yml up --build
