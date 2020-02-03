# DROP TABLES

songplay_table_drop = "DROP TABLE IF EXISTS songplays;"
user_table_drop = "DROP TABLE IF EXISTS users;"
song_table_drop = "DROP TABLE IF EXISTS songs;"
artist_table_drop = "DROP TABLE IF EXISTS artists;"
time_table_drop = "DROP TABLE IF EXISTS time;"

# CREATE TABLES
# TODO: use annotations "NOT NULL" and "PRIMARY KEY"

songplay_table_create = ("""CREATE TABLE IF NOT EXISTS songplays (songplay_id SERIAL PRIMARY KEY,
                                                                  start_time timestamp,
                                                                  user_id text,
                                                                  level text,
                                                                  song_id text,
                                                                  artist_id text,
                                                                  session_id text,
                                                                  location text,
                                                                  user_agent text);
""")

user_table_create = ("""CREATE TABLE IF NOT EXISTS users (user_id text PRIMARY KEY,
                                                          first_name text,
                                                          last_name text,
                                                          gender char(1),
                                                          level text);
""")

song_table_create = ("""CREATE TABLE IF NOT EXISTS songs (song_id text PRIMARY KEY,
                                                          title text,
                                                          artist_id text,
                                                          year int,
                                                          duration numeric);
""")

artist_table_create = ("""CREATE TABLE IF NOT EXISTS artists (artist_id text PRIMARY KEY,
                                                              name text,
                                                              location text,
                                                              latitude numeric,
                                                              longitude numeric);
""")

time_table_create = ("""CREATE TABLE IF NOT EXISTS time (start_time timestamp PRIMARY KEY,
                                                         hour int,
                                                         day int,
                                                         week int,
                                                         month int,
                                                         year int,
                                                         weekday varchar(9));
""")

# INSERT RECORDS

songplay_table_insert = ("""INSERT INTO songplays (start_time, user_id, level, song_id, 
                                                   artist_id, session_id, location, user_agent)
                            VALUES (%s, %s, %s, %s, %s, %s, %s, %s);
""")

user_table_insert = ("""INSERT INTO users (user_id, first_name, last_name, gender, level)
                        VALUES (%s, %s, %s, %s, %s)
                        ON CONFLICT (user_id) DO UPDATE SET first_name = EXCLUDED.first_name,
                                                            last_name = EXCLUDED.last_name,
                                                            gender = EXCLUDED.gender,
                                                            level = EXCLUDED.level;
""")

song_table_insert = ("""INSERT INTO songs (song_id, title, artist_id, year, duration)
                        VALUES (%s, %s, %s, %s, %s)
                        ON CONFLICT (song_id) DO UPDATE SET title = EXCLUDED.title,
                                                            artist_id = EXCLUDED.artist_id,
                                                            year = EXCLUDED.year,
                                                            duration = EXCLUDED.duration;
""")

artist_table_insert = ("""INSERT INTO artists (artist_id, name, location, latitude, longitude)
                          VALUES (%s, %s, %s, %s, %s)
                          ON CONFLICT (artist_id) DO UPDATE SET name = EXCLUDED.name,
                                                                location = EXCLUDED.location,
                                                                latitude = EXCLUDED.latitude,
                                                                longitude = EXCLUDED.longitude;
""")

time_table_insert = ("""INSERT INTO time (start_time, hour, day, week, month, year, weekday)
                        VALUES (%s, %s, %s, %s, %s, %s, %s)
                        ON CONFLICT (start_time) DO NOTHING;
""")

# FIND SONGS

song_select = ("""SELECT songs.song_id, artists.artist_id
                  FROM songs JOIN artists ON songs.artist_id = artists.artist_id
                  WHERE songs.title = %s AND artists.name = %s AND songs.duration = %s;
""")

# QUERY LISTS

create_table_queries = [songplay_table_create, user_table_create, song_table_create, artist_table_create, time_table_create]
drop_table_queries = [songplay_table_drop, user_table_drop, song_table_drop, artist_table_drop, time_table_drop]