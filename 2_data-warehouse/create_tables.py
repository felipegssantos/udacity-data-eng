import configparser
import psycopg2
from sql_queries import create_table_queries, drop_table_queries


def drop_tables(cur, conn):
    """
    Deletes pre-existing tables to ensure our tables will be created without the database throwing errors.

    :param cur: a psycopg2 cursor for the database
    :param conn: a psycopg2 connection to the database
    """
    for query in drop_table_queries:
        cur.execute(query)
        conn.commit()


def create_tables(cur, conn):
    """
    Creates staging tables, fact table and dimension tables in the data warehouse.

    :param cur: a psycopg2 cursor for the database
    :param conn: a psycopg2 connection to the database
    """
    for query in create_table_queries:
        cur.execute(query)
        conn.commit()


def main():
    """
    Runs the whole table creation routine.

    This function will first connect to the database, then drop all pre-existing tables and finally
    create all tables necessary for the data warehouse.
    """
    config = configparser.ConfigParser()
    config.read('dwh.cfg')

    conn = psycopg2.connect("host={} dbname={} user={} password={} port={}".format(*config['CLUSTER'].values()))
    cur = conn.cursor()

    drop_tables(cur, conn)
    create_tables(cur, conn)

    conn.close()


if __name__ == "__main__":
    main()
