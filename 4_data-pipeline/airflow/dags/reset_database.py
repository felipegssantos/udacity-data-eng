from datetime import datetime

from airflow import DAG
from airflow.operators.postgres_operator import PostgresOperator


CREATE_TABLES = {
    'artists': """ 
        CREATE TABLE public.artists (
            artistid varchar(256) NOT NULL,
            name varchar(256),
            location varchar(256),
            latitude numeric(18,0),
            longitude numeric(18,0)
        );""",
    'songplays': """
        CREATE TABLE public.songplays (
            playid varchar(32) NOT NULL,
            start_time timestamp NOT NULL,
            userid int4 NOT NULL,
            "level" varchar(256),
            songid varchar(256),
            artistid varchar(256),
            sessionid int4,
            location varchar(256),
            user_agent varchar(256),
            CONSTRAINT songplays_pkey PRIMARY KEY (playid)
        );""",
    'songs': """
        CREATE TABLE public.songs (
            songid varchar(256) NOT NULL,
            title varchar(256),
            artistid varchar(256),
            "year" int4,
            duration numeric(18,0),
            CONSTRAINT songs_pkey PRIMARY KEY (songid)
        );""",
    'staging_events': """
        CREATE TABLE public.staging_events (
            artist varchar(256),
            auth varchar(256),
            firstname varchar(256),
            gender varchar(256),
            iteminsession int4,
            lastname varchar(256),
            length numeric(18,0),
            "level" varchar(256),
            location varchar(256),
            "method" varchar(256),
            page varchar(256),
            registration numeric(18,0),
            sessionid int4,
            song varchar(256),
            status int4,
            ts int8,
            useragent varchar(256),
            userid int4
        );""",
    'staging_songs': """
        CREATE TABLE public.staging_songs (
            num_songs int4,
            artist_id varchar(256),
            artist_name varchar(256),
            artist_latitude numeric(18,0),
            artist_longitude numeric(18,0),
            artist_location varchar(256),
            song_id varchar(256),
            title varchar(256),
            duration numeric(18,0),
            "year" int4
        );""",
    'time': """
        CREATE TABLE public."time" (
            start_time timestamp NOT NULL,
            "hour" int4,
            "day" int4,
            week int4,
            "month" varchar(256),
            "year" int4,
            weekday varchar(256),
            CONSTRAINT time_pkey PRIMARY KEY (start_time)
        );""",
    'users': """
        CREATE TABLE public.users (
            userid int4 NOT NULL,
            first_name varchar(256),
            last_name varchar(256),
            gender varchar(256),
            "level" varchar(256),
            CONSTRAINT users_pkey PRIMARY KEY (userid)
        );"""
}


dag = DAG('project.reset_db', description='Drops and recreates all tables',
          schedule_interval=None, start_date=datetime.now())
    # for table, query in CREATE_TABLES.values():
table = 'artists'
query = CREATE_TABLES[table]
drop_task = PostgresOperator(sql=f"DROP TABLE IF EXISTS {table};",
                             postgres_conn_id='redshift',
                             autocommit=True,
                             task_id=f'drop_{table}',
                             dag=dag)
create_task = PostgresOperator(sql=query,
                               postgres_conn_id='redshift',
                               autocommit=True,
                               task_id=f'create_{table}',
                               dag=dag)
drop_task >> create_task
