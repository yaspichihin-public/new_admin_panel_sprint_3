from django.db import migrations


class Migration(migrations.Migration):

    initial = True

    dependencies = []

    operations = [
        migrations.RunSQL(
            sql="create schema if not exists content;",
            reverse_sql="drop schema if exists content;",
        )
    ]
