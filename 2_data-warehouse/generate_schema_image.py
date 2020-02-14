"""
This scripts generates an er file which can be transformed into a ER diagram.
(Optionally, one may edit the er file in order to change the diagram appearance.)

It requires installing eralchemy - https://github.com/Alexis-benoist/eralchemy/
"""
import configparser

from eralchemy import render_er


if __name__ == '__main__':
    config = configparser.ConfigParser()
    config.read('dwh.cfg')
    host, db_name, username, password, port = config['CLUSTER'].values()
    conn_string = f'postgresql+psycopg2://{username}:{password}@{host}:{port}/{db_name}'
    render_er(conn_string, 'images/star_schema.er', exclude_tables=['staging_songs', 'staging_events'])
