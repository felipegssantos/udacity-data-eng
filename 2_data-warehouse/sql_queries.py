import configparser


# CONFIG
config = configparser.ConfigParser()
config.read('dwh.cfg')

# DROP TABLES

staging_events_table_drop = "DROP TABLE IF EXISTS staging_events"
staging_songs_table_drop = "DROP TABLE IF EXISTS staging_songs"
songplay_table_drop = "DROP TABLE IF EXISTS songplay"
user_table_drop = "DROP TABLE IF EXISTS user"
song_table_drop = "DROP TABLE IF EXISTS song"
artist_table_drop = "DROP TABLE IF EXISTS artist"
time_table_drop = "DROP TABLE IF EXISTS time"

# CREATE TABLES

staging_events_table_create = ("""CREATE TABLE IF NOT EXISTS staging_events (artist text,
                                                                             auth text,
                                                                             first_name text,
                                                                             gender char(1),
                                                                             item_in_session int,
                                                                             last_name int,
                                                                             length int,
                                                                             level text,
                                                                             location text,
                                                                             method text,
                                                                             page text,
                                                                             registration numeric
                                                                             session_id int, 
                                                                             song text,
                                                                             status int,
                                                                             ts int,
                                                                             user_agent text,
                                                                             user_id int)
""")

staging_songs_table_create = ("""CREATE TABLE IF NOT EXISTS staging_songs (num_songs int,
                                                                           artist_id text,
                                                                           artist_latitude numeric,
                                                                           artist_longitude numeric,
                                                                           artist_location text,,
                                                                           artist_name text,
                                                                           song_id text,
                                                                           title text,
                                                                           duration numeric,
                                                                           year int)
""")

songplay_table_create = ("""
""")

user_table_create = ("""
""")

song_table_create = ("""
""")

artist_table_create = ("""
""")

time_table_create = ("""
""")

# STAGING TABLES

staging_events_copy = ("""COPY staging_events
                          FROM {}
                          iam_role '{}'
                          json '{}';
""").format(config.get('S3', 'LOG_DATA'), config.get('IAM_ROLE', 'ARN'), config.get('S3', 'LOG_JSONPATH'))

staging_songs_copy = ("""COPY staging_songs
                         FROM {}
                         iam_role '{}'
                         json 'auto';
""").format(config.get('S3', 'SONG_DATA'), config.get('IAM_ROLE', 'ARN'))

# FINAL TABLES

songplay_table_insert = ("""
""")

user_table_insert = ("""
""")

song_table_insert = ("""
""")

artist_table_insert = ("""
""")

time_table_insert = ("""
""")

# QUERY LISTS

create_table_queries = [staging_events_table_create, staging_songs_table_create, songplay_table_create,
                        user_table_create, song_table_create, artist_table_create, time_table_create]
drop_table_queries = [staging_events_table_drop, staging_songs_table_drop, songplay_table_drop, user_table_drop,
                      song_table_drop, artist_table_drop, time_table_drop]
copy_table_queries = [staging_events_copy, staging_songs_copy]
insert_table_queries = [songplay_table_insert, user_table_insert, song_table_insert, artist_table_insert,
                        time_table_insert]
