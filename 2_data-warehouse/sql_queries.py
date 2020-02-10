import configparser


# CONFIG
config = configparser.ConfigParser()
config.read('dwh.cfg')

# DROP TABLES

staging_events_table_drop = "DROP TABLE IF EXISTS staging_events;"
staging_songs_table_drop = "DROP TABLE IF EXISTS staging_songs;"
songplay_table_drop = "DROP TABLE IF EXISTS songplay;"
user_table_drop = "DROP TABLE IF EXISTS customer;"
song_table_drop = "DROP TABLE IF EXISTS song;"
artist_table_drop = "DROP TABLE IF EXISTS artist;"
time_table_drop = "DROP TABLE IF EXISTS time;"

# CREATE TABLES
# TODO: choose wisely distribution style for each table

staging_events_table_create = ("""
    CREATE TABLE IF NOT EXISTS staging_events (
        artist text,
        auth text,
        first_name text,
        gender char(1),
        item_in_session int,
        last_name text,
        length decimal(10, 5),
        level text,
        location text,
        method text,
        page text,
        registration decimal,
        session_id int, 
        song text,
        status int,
        ts bigint,
        user_agent text,
        user_id int)
    diststyle even;
""")

staging_songs_table_create = ("""
    CREATE TABLE IF NOT EXISTS staging_songs (
        num_songs int,
        artist_id text,
        artist_latitude decimal,
        artist_longitude decimal,
        artist_location text,
        artist_name text,
        song_id text,
        title text,
        duration decimal(10, 5),
        year int)
    diststyle all;
""")

songplay_table_create = ("""
    CREATE TABLE IF NOT EXISTS songplay (
        songplay_id int IDENTITY(0,1) PRIMARY KEY,
        start_time timestamp REFERENCES time,
        user_id text DISTKEY REFERENCES customer,
        level text,
        song_id text REFERENCES song,
        artist_id text REFERENCES artist,
        session_id int NOT NULL,
        item_in_session int NOT NULL, 
        location text,
        user_agent text)
    SORTKEY (session_id, item_in_session);
""")

user_table_create = ("""
    CREATE TABLE IF NOT EXISTS customer (
        user_id int PRIMARY KEY,
        first_name text,
        last_name text,
        gender char(1),
        level text)
    diststyle all;
""")

song_table_create = ("""
    CREATE TABLE IF NOT EXISTS song (
        song_id text PRIMARY KEY,
        title text,
        artist_id text REFERENCES artist,
        year int,
        duration decimal(10, 5))
    diststyle all;
""")

artist_table_create = ("""
    CREATE TABLE IF NOT EXISTS artist (
        artist_id text PRIMARY KEY,
        name text,
        location text,
        latitude numeric,
        longitude numeric)
    diststyle all;
""")

time_table_create = ("""
    CREATE TABLE IF NOT EXISTS time (
        start_time timestamp PRIMARY KEY,
        hour int NOT NULL,
        day int NOT NULL,
        week int NOT NULL,
        month int NOT NULL,
        year int NOT NULL,
        weekday varchar(9) NOT NULL)
    diststyle all;
""")

# STAGING TABLES

staging_events_copy = ("""
    COPY staging_events
    FROM {}
    iam_role {}
    json {} truncatecolumns;
""").format(config.get('S3', 'LOG_DATA'), config.get('IAM_ROLE', 'ARN'), config.get('S3', 'LOG_JSONPATH'))

staging_songs_copy = ("""
    COPY staging_songs
    FROM {}
    iam_role {}
    json 'auto' truncatecolumns;
""").format(config.get('S3', 'SONG_DATA'), config.get('IAM_ROLE', 'ARN'))

# FINAL TABLES

songplay_table_insert = ("""
""")

user_table_insert = ("""
    INSERT INTO customer (user_id, first_name, last_name, gender, level)
    SELECT user_id, first_name, last_name, gender, level
    FROM (
        select user_id, first_name, last_name, gender, level,
            ROW_NUMBER() OVER (PARTITION BY user_id ORDER BY ts DESC) row_number
        from staging_events)
    WHERE row_number = 1
    AND user_id IS NOT NULL;
""")

song_table_insert = ("""
""")

artist_table_insert = ("""
    INSERT INTO artist (artist_id, name, location, latitude, longitude)
    SELECT artist_id, artist_name, artist_location, artist_latitude, artist_longitude
    FROM(
        SELECT artist_id, artist_name, artist_location, artist_latitude, artist_longitude,
            ROW_NUMBER() OVER (PARTITION BY artist_id ORDER BY LEN(artist_name), artist_location NULLS LAST, artist_latitude NULLS LAST, artist_longitude NULLS LAST) row_number
        FROM staging_songs
        WHERE artist_name NOT LIKE '%feat.%' or artist_name NOT LIKE '%featuring')
    WHERE row_number = 1;
""")  # TODO: check if I missed any artist ID due to the "NOT LIKE" rules

time_table_insert = ("""
""")

# QUERY LISTS

create_table_queries = [staging_events_table_create, staging_songs_table_create, user_table_create,
                        artist_table_create, song_table_create, time_table_create, songplay_table_create]
drop_table_queries = [staging_events_table_drop, staging_songs_table_drop, songplay_table_drop, user_table_drop,
                      song_table_drop, artist_table_drop, time_table_drop]
copy_table_queries = [staging_events_copy, staging_songs_copy]
insert_table_queries = [user_table_insert, artist_table_insert, song_table_insert, time_table_insert,
                        songplay_table_insert]
