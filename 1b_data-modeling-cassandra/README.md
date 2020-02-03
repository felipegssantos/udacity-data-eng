# Data Modeling with Apache Cassandra

The goal of this project is to implement an ETL pipeline for the fictitious company Sparkify.
We want to build a Cassandra database from songplay records and song metadata provided as CSV data,
such that data scientists and analysts can more easily understand what songs their users
are listening to on their music streaming app. 

## Set up

### Database
In order to run this project, you need to have an Apache Cassandra server up.

### Python
You'll need a Python 3 environment containing the libraries in
[requirements.txt](./requirement.txt). In order to run the notebook
[create_tables_and_run_etl.ipynb](./create_tables_and_run_etl.ipynb), you must also install
jupyter.

### Raw data
The raw data directory is expected to be in the same directory of the scripts, just like in the 
Project Workspace.

## Running the ETL scripts
First, run `python create_tables.py` in order to create the database and the tables of
the Star Schema. After that, run `python etl.py` in order to extract raw data from the
CSV files, transform them to match the data model and load the result into the database.
> *Note to Udacity's reviewer*: although my notebook does create the file `event_data_new.csv`,
>I believe this would not be a good idea in an actual big data scenario. Therefore, I decided to
>implement a generator function in my scripts that would act like a stream of CSV files.

After that, you can check the data model is able to answer the answers by running
`python test_data_model.py`. 

## Data modeling details: questions, queries and tables

As typical for NoSQL databases, data is modeled according to the queries one wish to perform. This
means we must know in advance what kind of questions would be important in order to build the data
model accordingly.

Below we show the typical questions this database must be able to answer and how this reflects
on the table schemas. 

- What is the artist name, song title and song's length in the music app history that was heard
as the n-th item of some specific session?
    - Typical query:
    ```
    SELECT * FROM session_history WHERE session_id=<session_id>
                                  AND item_in_session=<item_in_session>
    ```
   - Partition key: `session_id`
   - Clustering column: `item_in_session`
   - Other columns: `artist`, `song_title` and `song_length`

- Given a user ID and a session ID, what are the artist name, user name (first and last) and
song title, sorted by the order the songs were listened?
    - Typical query:
    ```
    SELECT * FROM user_history WHERE user_id=<user_id> AND session_id=<session_id>
    ```
   - Partition key: `user_id`
   - Clustering columns (in order of precedence): `session_id` and `item_in_session`; note that
   `item_in_session` is not used in the query, but is required in order to sort songs in the order
    they were listened by the user
   - Other columns: `artist`, `song_title`, `first_name` and `last_name`

- Give me every user name (first and last) who ever listened to some specific song.
    - Typical query:
    ```
    SELECT first_name, last_name FROM song_history WHERE song_title=<song_title>
    ```
    - Partition key: `song_title`
    - Clustering column: `user_id` (although not in the query, this is required to ensure 
    different users who listened to the same song both show up in the table, i.e., there's no
    accidental overwriting)
    - Other columns: `first_name` and `last_name`
     