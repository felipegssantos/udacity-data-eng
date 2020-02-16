import configparser
from datetime import datetime
import os

from pyspark.sql import SparkSession, Window
from pyspark.sql.functions import (udf, col, row_number, length, year, month, dayofmonth, hour, weekofyear,
                                   desc, dayofweek, to_timestamp)


config = configparser.ConfigParser()
config.read('dl.cfg')

os.environ['AWS_ACCESS_KEY_ID'] = config['AWS']['AWS_ACCESS_KEY_ID']
os.environ['AWS_SECRET_ACCESS_KEY'] = config['AWS']['AWS_SECRET_ACCESS_KEY']


def create_spark_session() -> SparkSession:
    """
    Creates a spark session.

    :return spark: SparkSession instance
    """
    spark = SparkSession \
        .builder \
        .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:2.7.0") \
        .getOrCreate()
    return spark


def process_song_data(spark: SparkSession, input_data: str, output_data: str) -> None:
    """
    Extracts raw song data from data lake, transforms it to songs and artists table and loads these tables back
    to the data lake.

    :param spark: instance of SparkSession
    :param input_data: data lake path to input data
    :param output_data: data lake path to output data
    """
    # get filepath to song data file
    song_data = os.path.join(input_data, 'song_data/*/*/*/*.json')

    # read song data file
    df = spark.read.json(song_data)

    # extract columns to create songs table
    songs_table = df.select('song_id', 'title', 'artist_id', 'year', 'duration') \
                    .where(col('song_id').isNotNull()) \
                    .drop_duplicates(['song_id'])

    # write songs table to parquet files partitioned by year and artist
    songs_table.write.mode('overwrite').partitionBy('year', 'artist_id').parquet(os.path.join(output_data, 'songs'))

    # extract columns to create artists table
    # we need some extra logic to clean up duplicated artist_id records with distinct artist_name; we want to:
    #  1. Remove "featured" names (e.g. "Elton John feat. Sting" or "Elton John featuring Sting");
    #  2. From the remaining records, choose the one with the shortest artist name and try to get a record with
    #     non-null location, latitude and longitude.
    artist_cleanup_ordering = row_number().over(Window.partitionBy('artist_id')
                                                      .orderBy(length('name'),
                                                               col('location').asc_nulls_last(),
                                                               col('latitude').asc_nulls_last(),
                                                               col('longitude').asc_nulls_last()))
    artists_table = df.select(col('artist_id'), col('artist_name').alias('name'),
                              col('artist_location').alias('location'), col('artist_latitude').alias('latitude'),
                              col('artist_longitude').alias('longitude')) \
                      .where(~col('name').like('%feat.%') | ~col('name').like('%featuring')) \
                      .withColumn('rn', artist_cleanup_ordering) \
                      .where(col('rn') == 1) \
                      .drop('rn')

    # write artists table to parquet files
    artists_table.write.mode('overwrite').parquet(os.path.join(output_data, 'artists'))


def process_log_data(spark: SparkSession, input_data: str, output_data: str) -> None:
    """
    Extracts raw event and songs data from data lake, transforms it to time, users and songplays table and
    loads the transformed tables back to the data lake.

    :param spark: instance of SparkSession
    :param input_data: data lake path to input data
    :param output_data: data lake path to output data
    """
    # get filepath to log data file
    log_data = os.path.join(input_data, 'log-data/*/*/*.json')

    # read log data file
    df = spark.read.json(log_data)

    # filter by actions for song plays
    df = df.where(col('page') == 'NextSong')

    # extract columns for users table
    # we need some extra logic to get the latest "level" for each user; this is done using a window function to
    # partition by user and get latest "level" record.
    users_cleanup_ordering = row_number().over(Window.partitionBy().orderBy(desc('ts')))
    users_table = df.select(col('userId').alias('user_id'), col('firstName').alias('first_name'),
                            col('lastName').alias('last_name'), col('gender'), col('level'), col('ts')) \
                    .where(col('user_id').isNotNull()) \
                    .withColumn('rn', users_cleanup_ordering) \
                    .where(col('rn') == 1) \
                    .drop('rn', 'ts')

    # write users table to parquet files
    users_table.write.mode('overwrite').parquet(os.path.join(output_data, 'users'))

    # create timestamp column from original timestamp column
    # get_timestamp =
    # df =

    # create datetime column from original timestamp column
    get_datetime = udf(lambda ts: str(datetime.utcfromtimestamp(ts / 1000)))
    df = df.withColumn('start_time', get_datetime('ts').cast('timestamp')) \
           .drop('ts')

    # extract columns to create time table
    time_table = df.select('start_time') \
                   .distinct() \
                   .withColumn('hour', hour('start_time')) \
                   .withColumn('day', dayofmonth('start_time')) \
                   .withColumn('week', weekofyear('start_time')) \
                   .withColumn('month', month('start_time')) \
                   .withColumn('year', year('start_time')) \
                   .withColumn('weekday', dayofweek('start_time'))

    # write time table to parquet files partitioned by year and month
    time_table.write.mode('overwrite').partitionBy('year', 'month').parquet(os.path.join(output_data, 'time'))

    # read in song data to use for songplays table
    song_df = spark.read.json(os.path.join(input_data, 'song_data/*/*/*/*.json')) \
                   .select(col('title').alias('song'), col('duration').alias('length'),
                           col('artist_name').alias('artist'), col('song_id'), col('artist_id'))

    # extract columns from joined song and log datasets to create songplays table 
    songplays_table = df.select('userId', 'sessionId', 'location', 'userAgent', 'level',
                                'start_time', 'song', 'length', 'artist') \
                        .join(song_df, on=['song', 'length', 'artist']) \
                        .select(col('userId').alias('user_id'), col('song_id'), col('artist_id'), col('start_time'),
                                col('sessionId').alias('session_id'), col('location'),
                                col('userAgent').alias('user_agent'), col('level'),
                                year('start_time').alias('year'), month('start_time').alias('month'))

    # write songplays table to parquet files partitioned by year and month
    songplays_table.write.mode('overwrite').partitionBy('year', 'month').parquet(os.path.join(output_data, 'songplays'))


def main():
    """
    Runs the ETL pipeline.

    This function is responsible for reading the raw data, transforming it to the star schema tables and loading the
    transformed tables back to the data lake.
    """
    spark = create_spark_session()
    input_data = "s3a://udacity-dend/"
    output_data = 's3n://raposa-udacity-de-nanodegree/data-lake/'

    process_song_data(spark, input_data, output_data)
    process_log_data(spark, input_data, output_data)


if __name__ == "__main__":
    main()
