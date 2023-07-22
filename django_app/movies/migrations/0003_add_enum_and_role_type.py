from django.db import migrations


class Migration(migrations.Migration):

    initial = False

    dependencies = [
        ('movies', '0002_initial_tables'),
    ]

    operations = [
        migrations.RunSQL(
            sql="""
            create type type_enum as enum ('movie', 'tv_show');
            alter table content.film_work
                alter column type TYPE type_enum using type::type_enum;
                
            create type role_enum as enum ('actor', 'writer', 'director');
            alter table content.person_film_work
                alter column role type role_enum using role::role_enum;
                
            commit;
            """,
            reverse_sql="""
            alter table content.film_work alter column type type varchar(20);
            drop type type_enum;
            
            alter table content.person_film_work alter column role type varchar(64);
            drop type type_enum;
            
            commit;
            """,
        )
    ]
