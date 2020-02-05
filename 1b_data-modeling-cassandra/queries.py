# DROP TABLES
drop_session_history = 'DROP TABLE IF EXISTS session_history'
drop_user_history = 'DROP TABLE IF EXISTS user_history'
drop_song_history = 'DROP TABLE IF EXISTS song_history'

# CREATE TABLES
create_session_history = """CREATE TABLE IF NOT EXISTS session_history (session_id int,
                                                                        item_in_session int,
                                                                        artist text,
                                                                        song_title text,
                                                                        song_length double,
                                                                        PRIMARY KEY(session_id, item_in_session))"""
create_user_history = """CREATE TABLE IF NOT EXISTS user_history (user_id int,
                                                                  session_id int,
                                                                  item_in_session int,
                                                                  artist text,
                                                                  song_title text,
                                                                  first_name text,
                                                                  last_name text,
                                                                  PRIMARY KEY(user_id, session_id, item_in_session))"""
create_song_history = """CREATE TABLE IF NOT EXISTS song_history (song_title text,
                                                                  user_id int,
                                                                  first_name text,
                                                                  last_name text,
                                                                  PRIMARY KEY(song_title, user_id))"""

# INSERT RECORDS
insert_session_history = """INSERT INTO session_history (session_id, item_in_session, artist, song_title, song_length)
                            VALUES (%s, %s, %s, %s, %s)"""
insert_user_history = """INSERT INTO user_history (user_id, session_id, item_in_session,
                                                   artist, song_title, first_name, last_name)
                         VALUES (%s, %s, %s, %s, %s, %s, %s)"""
insert_song_history = """INSERT INTO song_history (song_title, user_id, first_name, last_name)
                         VALUES (%s, %s, %s, %s)"""

# SELECT RECORDS
select_session_data = """SELECT artist, song_title, song_length
                         FROM session_history WHERE session_id=%s AND item_in_session=%s"""
select_user_data = """SELECT artist, song_title, first_name, last_name
                      FROM user_history WHERE user_id=%s AND session_id=%s"""
select_song_data = """SELECT first_name, last_name
                      FROM song_history WHERE song_title=%s"""

# QUERY LISTS
create_table_queries = [create_session_history, create_user_history, create_song_history]
drop_table_queries = [drop_session_history, drop_user_history, drop_song_history]
