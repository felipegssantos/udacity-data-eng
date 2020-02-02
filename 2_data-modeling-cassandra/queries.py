# DROP TABLES
drop_session_history = 'DROP TABLE IF EXISTS session_history'
drop_user_history = 'DROP TABLE IF EXISTS user_history'
drop_song_history = 'DROP TABLE IF EXISTS song_history'

# CREATE TABLES
create_session_history = """CREATE TABLE IF NOT EXISTS session_history (artist text,
                                                                        song_title text,
                                                                        song_length double,
                                                                        session_id int,
                                                                        item_in_session int,
                                                                        PRIMARY KEY(session_id, item_in_session))"""
create_user_history = """CREATE TABLE IF NOT EXISTS user_history (artist text,
                                                                  song_title text,
                                                                  first_name text,
                                                                  last_name text,
                                                                  user_id int,
                                                                  session_id int,
                                                                  item_in_session int,
                                                                  PRIMARY KEY(user_id, session_id, item_in_session))"""
create_song_history = """CREATE TABLE IF NOT EXISTS song_history (song_title text,
                                                                  first_name text,
                                                                  last_name text,
                                                                  PRIMARY KEY(song_title))"""

# INSERT RECORDS
insert_session_history = """INSERT INTO session_history (artist, song_title, song_length, session_id, item_in_session)
                            VALUES (%s, %s, %s, %s, %s)"""
insert_user_history = """INSERT INTO user_history (artist, song_title, first_name, last_name, 
                                                   user_id, session_id, item_in_session)
                         VALUES (%s, %s, %s, %s, %s, %s, %s)"""
insert_song_history = """INSERT INTO song_history (song_title, first_name, last_name)
                         VALUES (%s, %s, %s)"""

# SELECT RECORDS
select_session_data = "SELECT * FROM session_history WHERE session_id=%s AND item_in_session=%s"
select_user_data = "SELECT * FROM user_history WHERE user_id=%s AND session_id=%s"
select_song_data = "SELECT * FROM song_history WHERE song_title=%s"

# QUERY LISTS
create_table_queries = [create_session_history, create_user_history, create_song_history]
drop_table_queries = [drop_session_history, drop_user_history, drop_song_history]
