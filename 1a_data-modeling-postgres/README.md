# Data Modeling with Postgres

The goal of this project is to implement an ETL pipeline for the fictitious company Sparkify.
We want to build a database from songplay records and song metadata provided as JSON logs,
such that data scientists and analysts can more easily understand what songs their users
are listening to on their music streaming app.

The database that allows for easy access to the user's behavior is modeled as a Star
Schema, with the songplay events as the fact table, as the main understanding of the user
behavior comes from what musics their listening to. We also have dimension tables that
may give further data about users, artists, songs and date-time related to each songplay.

## Set up

### Database
In order to run this project, you need to have a Postgres server up. There, you must have
a "studentdb" database along with a "student" user whose password is also "student" (of
course, we overlook security concerns for the sake of this demo project).

### Python
You'll need a Python 3 environment containing the libraries in 
[requirements.txt](./requirements.txt). Note that the python scripts depend only on
psycopg2 and pandas, whereas the jupyter notebooks require all dependencies.

### Raw data
The raw data is expected to be in the same directory as the scripts and notebooks, just
like in the Project Workspace.

## Running the ETL scripts

First, run `python create_tables.py` in order to create the database and the tables of
the Star Schema. After that, run `python etl.py` in order to extract raw data from the
JSON logs, transform them to match the database schema and load them into the database.
