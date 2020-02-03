from cassandra.cluster import Cluster
from queries import create_table_queries, drop_table_queries


def create_keyspace():
    """
    Connects to Apache Cassandra and creates a keyspace.

    :return: (cassandra.Cluster, cassandra.Session) for this keyspace
    """
    cluster = Cluster()
    session = cluster.connect()
    session.execute("""CREATE KEYSPACE IF NOT EXISTS sparkifydb
                       WITH REPLICATION =
                       {'class': 'SimpleStrategy', 'replication_factor': 1}""")
    session.set_keyspace('sparkifydb')
    return cluster, session


def drop_tables(session):
    """
    Drops all tables relevant to this ETL
    :param session: a cassandra.Session instance
    """
    for query in drop_table_queries:
        session.execute(query)


def create_tables(session):
    """
    Creates all tables relevant to the ETL
    :param session: a cassandra.Session instance
    """
    for query in create_table_queries:
        session.execute(query)


if __name__ == '__main__':
    cluster, session = create_keyspace()
    drop_tables(session)
    create_tables(session)
    session.shutdown()
    cluster.shutdown()
