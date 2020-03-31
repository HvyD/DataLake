import configparser
from datetime import datetime
import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import udf, col
from pyspark.sql.functions import year, month, dayofmonth, hour, weekofyear, \
    dayofweek
from pyspark.sql.types import StructType as R, StructField as Fld, \
    IntegerType as Int, StringType as Str, DoubleType as Dbl, \
    LongType as Lng, TimestampType as Ts


config = configparser.ConfigParser()
config.read('dl.cfg')

os.environ['AWS_ACCESS_KEY_ID'] = config['AWS']['AWS_ACCESS_KEY_ID']
os.environ['AWS_SECRET_ACCESS_KEY'] = config['AWS']['AWS_SECRET_ACCESS_KEY']


def create_spark_session():
    """
    Creates Spark session
    """
    spark = SparkSession \
        .builder \
        .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:2.7.5") \
        .getOrCreate()
    return spark


def process_song_data(spark, input_data, output_data):
    """Process song data, transform the data into songs and artists tables
    and store it in parquet files on S3.
    Parameters
    ----------
    spark : SparkSession
        cursor to the sparkify database connection
    input_data : string
        input data prepend path
    output_data : string
        output data prepend path
    """
    # get filepath to song data file
    song_data = os.path.join(input_data, "song_data/*/*/*/*.json")

    song_schema = R([
        Fld("num_songs", Int()),
        Fld("artist_id", Str(), False),
        Fld("artist_latitude", Dbl()),
        Fld("artist_longitude", Dbl()),
        Fld("artist_location", Str()),
        Fld("artist_name", Str(), False),
        Fld("song_id", Str(), False),
        Fld("title", Str(), False),
        Fld("duration", Dbl(), False),
        Fld("year", Int())
    ])

    # read song data file
    df = spark.read.json(song_data, song_schema)

    # extract columns to create songs table
    songs_table = df.select([
        "song_id",
        "title",
        "artist_id",
        "year",
        "duration"
    ])

    # write songs table to parquet files partitioned by year and artist
    songs_table.write.mode("overwrite").partitionBy('year', 'artist_id') \
        .parquet(os.path.join(output_data, 'analytics/songs'))

    # extract columns to create artists table
    artists_table = df.select([
        "artist_id",
        "artist_name",
        "artist_location",
        "artist_latitude",
        "artist_longitude"
    ])
    artists_table = artists_table.withColumnRenamed("artist_name", "name") \
        .withColumnRenamed("artist_location", "location") \
        .withColumnRenamed("artist_latitude", "latitude") \
        .withColumnRenamed("artist_longitude", "longitude")

    # write artists table to parquet files
    artists_table.write.mode("overwrite") \
        .parquet(os.path.join(output_data, 'analytics/artists'))


def process_log_data(spark, input_data, output_data):
    """Process log data, transform the data into users, time and songplays tables
    and store it in parquet files on S3.
    Parameters
    ----------
    spark : SparkSession
        cursor to the sparkify database connection
    input_data : string
        input data prepend path
    output_data : string
        output data prepend path
    """
    # get filepath to log data file
    log_data = os.path.join(input_data, "log_data/*/*/*.json")

    log_schema = R([
        Fld("artist", Str(), False),
        Fld("auth", Str()),
        Fld("firstName", Str(), False),
        Fld("gender", Str(), False),
        Fld("itemInSession", Int()),
        Fld("lastName", Str(), False),
        Fld("length", Dbl(), False),
        Fld("level", Str()),
        Fld("location", Str()),
        Fld("method", Str()),
        Fld("page", Str()),
        Fld("registration", Dbl()),
        Fld("sessionId", Int()),
        Fld("song", Str(), False),
        Fld("status", Int()),
        Fld("ts", Lng(), False),
        Fld("userAgent", Str()),
        Fld("userId", Str(), False)
    ])

    # read log data file
    df = spark.read.json(log_data, log_schema)

    # filter by actions for song plays
    df = df.where(df["page"] == "NextSong")

    # extract columns for users table
    users_table = df.select([
        "userId",
        "firstName",
        "lastName",
        "gender",
        "level"
    ])
    users_table = users_table.withColumnRenamed("firstName", "first_name") \
        .withColumnRenamed("lastName", "last_name") \
        .withColumnRenamed("userId", "user_id").drop_duplicates()

    # write users table to parquet files
    users_table.write.mode("overwrite") \
        .parquet(os.path.join(output_data, 'analytics/users'))

    # create timestamp column from original timestamp column
    get_timestamp = udf(lambda x: datetime.fromtimestamp((x/1000.0)), Ts())
    df = df.withColumn("timestamp", get_timestamp(df.ts))

    # extract columns to create time table
    time_table = df.select("timestamp").drop_duplicates()
    time_table = time_table.withColumnRenamed("timestamp", "start_time")
    time_table = time_table.withColumn("hour", hour(col("start_time"))) \
        .withColumn("day", dayofmonth(col("start_time"))) \
        .withColumn("week", weekofyear(col("start_time"))) \
        .withColumn("month", month(col("start_time"))) \
        .withColumn("year", year(col("start_time"))) \
        .withColumn("weekday", dayofweek(col("start_time"))).drop_duplicates()

    # write time table to parquet files partitioned by year and month
    time_table.write.mode("overwrite").partitionBy('year', 'month') \
        .parquet(os.path.join(output_data, 'analytics/time'))

    # read in song data to use for songplays table
    song_df = spark.read.parquet(os.path.join(output_data, 'analytics/songs'))
    artist_df = spark.read \
        .parquet(os.path.join(output_data, 'analytics/artists'))
    song_artist_df = song_df \
        .join(artist_df, song_df.artist_id == artist_df.artist_id) \
        .select(song_df["song_id"], song_df["title"], artist_df["artist_id"],
                artist_df["name"], song_df["duration"])
    song_artist_df = song_artist_df.withColumnRenamed("name", "artist_name")

    # extract columns from joined song and log datasets to create songplays table
    conditions = [
        df.song == song_artist_df.title,
        df.artist == song_artist_df.artist_name,
        df.length == song_artist_df.duration
    ]
    songplays_table = df.join(song_artist_df, conditions) \
        .select(df["timestamp"], df["userId"], df["level"],
                song_artist_df["song_id"], song_artist_df["artist_id"],
                df["sessionId"], df["location"], df["userAgent"])

    songplays_table = songplays_table \
        .withColumnRenamed("timestamp", "start_time") \
        .withColumnRenamed("userId", "user_id") \
        .withColumnRenamed("sessionId", "session_id") \
        .withColumnRenamed("userAgent", "user_agent")
    songplays_table.show(5)

    # write songplays table to parquet files partitioned by year and month
    songplays_table.write.mode("overwrite").partitionBy('year', 'month') \
        .parquet(os.path.join(output_data, 'analytics/songplays'))


def main():
    """1. Creates Spark session.
    2. Process song data, transform it and store it in S3.
    3. Process log data, transform it and store it in S3.
    """
    spark = create_spark_session()
    input_data = "s3a://udacity-dend/"
    output_data = "s3a://aws-emr-resources-505593812338-us-west-2/notebooks/e-3D6FYGGXONLWLSJXY54QT8EQZ/"

    process_song_data(spark, input_data, output_data)
    process_log_data(spark, input_data, output_data)


if __name__ == "__main__":
    main()