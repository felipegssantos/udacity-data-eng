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

staging_events_table_create = ("""
    CREATE TABLE IF NOT EXISTS staging_events (
        artist text DISTKEY,
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
        ts bigint SORTKEY,
        user_agent text,
        user_id int);
""")  # TODO: justify DISTKEY and SORTKEY in README.md

staging_songs_table_create = ("""
    CREATE TABLE IF NOT EXISTS staging_songs (
        num_songs int,
        artist_id text DISTKEY,
        artist_latitude decimal,
        artist_longitude decimal,
        artist_location text,
        artist_name text,
        song_id text,
        title text,
        duration decimal(10, 5),
        year int);
""")  # TODO: justify DISTKEY and SORTKEY in README.md

songplay_table_create = ("""
    CREATE TABLE IF NOT EXISTS songplays (
        songplay_id int IDENTITY(0,1) PRIMARY KEY,
        start_time timestamp REFERENCES time,
        user_id text DISTKEY REFERENCES users,
        level text,
        song_id text REFERENCES songs,
        artist_id text REFERENCES artists,
        session_id int NOT NULL,
        location text,
        user_agent text)
    SORTKEY (start_time);
""")  # TODO: justify DISTKEY and SORTKEY in README.md

user_table_create = ("""
    CREATE TABLE IF NOT EXISTS users (
        user_id int PRIMARY KEY,
        first_name text,
        last_name text,
        gender char(1),
        level text NOT NULL)
    diststyle all;
""")

song_table_create = ("""
    CREATE TABLE IF NOT EXISTS songs (
        song_id text PRIMARY KEY,
        title text NOT NULL,
        artist_id text REFERENCES artist,
        year int,
        duration decimal(10, 5) NOT NULL)
    diststyle all;
""")

artist_table_create = ("""
    CREATE TABLE IF NOT EXISTS artists (
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
    INSERT INTO songplays (user_id, song_id, artist_id, start_time, session_id, location, user_agent, level)
    SELECT
        user_id,
        song_id,
        artist_id,
        timestamp 'epoch' + ts/1000 * interval '1 second' AS start_time,
        session_id,
        location,
        user_agent,
        level
    FROM staging_events e
    JOIN staging_songs s
    ON (e.song=s.title AND e.length=s.duration AND e.artist=s.artist_name)
    WHERE page='NextSong'
""")

user_table_insert = ("""
    INSERT INTO users (user_id, first_name, last_name, gender, level)
    SELECT user_id, first_name, last_name, gender, level
    FROM (
        select user_id, first_name, last_name, gender, level,
            ROW_NUMBER() OVER (PARTITION BY user_id ORDER BY ts DESC) row_number
        from staging_events)
    WHERE row_number = 1
    AND user_id IS NOT NULL;
""")  # TODO: README must explain "ORDER BY ts DESC" exists to get latest "level" for each user

# Known issue: the same song_id shows up with more than one value for duration
song_table_insert = ("""
    INSERT INTO songs (song_id, title, artist_id, year, duration)
    SELECT song_id, title, artist_id, year, duration
    FROM (
        SELECT song_id, title, artist_id, year, duration,
            ROW_NUMBER() OVER (PARTITION BY song_id) row_number
        FROM staging_songs
        WHERE song_id IS NOT NULL)
    WHERE row_number = 1;
""")

# Tried to make some data cleaning:
# 1. Sometimes an artist co-participates with (or "features") other in a song; we try to extract the main artist only
# 2. We try to extract some location/latitude/longitude
artist_table_insert = ("""
    INSERT INTO artists (artist_id, name, location, latitude, longitude)
    SELECT artist_id, artist_name, artist_location, artist_latitude, artist_longitude
    FROM(
        SELECT artist_id, artist_name, artist_location, artist_latitude, artist_longitude,
            ROW_NUMBER() OVER (PARTITION BY artist_id ORDER BY LEN(artist_name), 
                                   artist_location NULLS LAST, artist_latitude NULLS LAST, 
                                   artist_longitude NULLS LAST) row_number
        FROM staging_songs
        WHERE artist_name NOT LIKE '%feat.%' or artist_name NOT LIKE '%featuring')
    WHERE row_number = 1;
""")

time_table_insert = ("""
    INSERT INTO time (start_time, hour, day, week, month, year, weekday)
    SELECT start_time, EXTRACT(hour FROM start_time) AS hour, EXTRACT(day FROM start_time) AS day,
        EXTRACT(week FROM start_time) AS week, EXTRACT(month FROM start_time) AS month,
        EXTRACT(year FROM start_time) AS year, TO_CHAR(start_time, 'day') AS weekday
    FROM (
        SELECT DISTINCT timestamp 'epoch' + ts/1000 * interval '1 second' AS start_time
        FROM staging_events)
""")

# QUERY LISTS
# TODO: README must explain that I changed the query order to make "REFERENCES" work properly
create_table_queries = [staging_events_table_create, staging_songs_table_create, user_table_create,
                        artist_table_create, song_table_create, time_table_create, songplay_table_create]
drop_table_queries = [staging_events_table_drop, staging_songs_table_drop, songplay_table_drop, user_table_drop,
                      song_table_drop, artist_table_drop, time_table_drop]
copy_table_queries = [staging_events_copy, staging_songs_copy]
insert_table_queries = [user_table_insert, artist_table_insert, song_table_insert, time_table_insert,
                        songplay_table_insert]
