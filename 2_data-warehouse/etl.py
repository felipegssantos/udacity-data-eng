import configparser
import psycopg2
from sql_queries import copy_table_queries, insert_table_queries


def load_staging_tables(cur, conn):
    """
    Extracts raw data from S3 buckets and loads it to staging tables

    :param cur: a psycopg2 cursor for the database
    :param conn: a psycopg2 connection to the database
    """
    for query in copy_table_queries:
        print('  Running:', end='')
        print(query)
        cur.execute(query)
        conn.commit()


def insert_tables(cur, conn):
    """
    Extracts data from staging tables, transforms it to match the star schema and loads the result to
    fact and dimension tables.

    :param cur: a psycopg2 cursor for the database
    :param conn: a psycopg2 connection to the database
    """
    for query in insert_table_queries:
        print('  Running:', end='')
        print(query)
        cur.execute(query)
        conn.commit()


def main():
    """
    Runs whole ETL pipeline.

    This function will first connect to the database, then copy raw data to staging tables and finally
    extract and transform data from staging tables to the fact and dimension tables.
    """
    config = configparser.ConfigParser()
    config.read('dwh.cfg')

    conn = psycopg2.connect("host={} dbname={} user={} password={} port={}".format(*config['CLUSTER'].values()))
    cur = conn.cursor()
    
    load_staging_tables(cur, conn)
    insert_tables(cur, conn)

    conn.close()


if __name__ == "__main__":
    main()
