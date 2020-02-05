import csv
from glob import glob
import os

from cassandra.cluster import Cluster

from queries import insert_session_history, insert_song_history, insert_user_history


def connect_to_cassandra(keyspace='sparkifydb'):
    """
    Connects to Sparkify's Cassandra database.

    :param keyspace: name of the keyspace
    :return: cassandra.Cluster and cassandra.Session instances for this connection
    """
    cluster = Cluster()
    session = cluster.connect()
    session.set_keyspace(keyspace)
    return cluster, session


def process_event_data(session, csv_path):
    """
    Reads all data from `filepath` and loads it to Apache Cassandra.

    :param session: cassandra.Session instance
    :param csv_path: path to a directory of CSV files containing event data
    """
    event_stream = stream_from_files(csv_path)
    for event in event_stream:
        session.execute(insert_session_history, (event['sessionId'], event['itemInSession'], event['artist'],
                                                 event['song'], event['length']))
        session.execute(insert_user_history, (event['userId'], event['sessionId'], event['itemInSession'],
                                              event['artist'], event['song'], event['firstName'], event['lastName']))
        session.execute(insert_song_history, (event['song'], event['userId'], event['firstName'], event['lastName']))


def stream_from_files(csv_path):
    """
    Iterates over CSV files in `csv_path` line by line until all files have been consumed.

    :param csv_path: directory containing CSV event data
    :return: iterator of dict with event data
    """
    csv_files = glob(os.path.join(csv_path, '*.csv'))
    num_files = len(csv_files)
    for i, f in enumerate(csv_files, start=1):
        print(f'Processing file {i}/{num_files}')
        with open(f, 'r', encoding='utf8', newline='') as csv_file:
            csv_reader = csv.reader(csv_file)
            header = next(csv_reader)
            for line in csv_reader:
                if line[0] == '':
                    continue
                event = {k: v for k, v in zip(header, line)}
                event['length'] = float(event['length'])
                event['userId'] = int(event['userId'])
                event['itemInSession'] = int(event['itemInSession'])
                event['sessionId'] = int(event['sessionId'])
                yield event


def safe_cast(x, cast_func):
    """
    Casts x according to cast_func unless x is None or an empty string.

    :param x: a string representing an int or float
    :param cast_func: either int() or float() function
    :return: x as a numeric type or None
    """
    return cast_func(x) if x not in ['', None] else None


if __name__ == '__main__':
    cluster, session = connect_to_cassandra()
    process_event_data(session, f'{os.getcwd()}/event_data')
    session.shutdown()
    cluster.shutdown()
