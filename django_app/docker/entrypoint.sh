#!/bin/bash

python manage.py migrate

echo "
from django.contrib.auth.models import User;
User.objects.create_superuser('admin', 'admin@admin.com', 'admin')
" | python manage.py shell

cd import_data && python load_data.py && cd ..

python manage.py collectstatic --noinput

echo 'OK' > /opt/django_app/static/smoketest.html

gunicorn --config=gunicorn.conf.py
