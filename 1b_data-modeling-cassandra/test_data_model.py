from queries import select_session_data, select_song_data, select_user_data
from etl import connect_to_cassandra


def process_query(session, query, values):
    rows = session.execute(query, values)
    for row in rows:
        print(row)


if __name__ == '__main__':
    cluster, session = connect_to_cassandra()
    print("1. Give me the artist, song title and song's length in the music app history",
          "that was heard during sessionId = 338, and itemInSession = 4\n")
    process_query(session, select_session_data, (338, 4))
    print("\n2. Give me only the following: name of artist, song (sorted by itemInSession)",
          "and user (first and last name) for userid = 10, sessionid = 182\n")
    process_query(session, select_user_data, (10, 182))
    print("\n3. Give me every user name (first and last) in my music app history who listened",
          "to the song 'All Hands Against His Own'\n")
    process_query(session, select_song_data, ('All Hands Against His Own',))
    session.shutdown()
    cluster.shutdown()
