import configparser
from datetime import datetime
import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import udf, col
from pyspark.sql.functions import year, month, dayofmonth, hour, weekofyear, date_format

config = configparser.ConfigParser()
# config.read('dl.cfg')
config.read_file(open("dl.cfg"))
os.environ['AWS_ACCESS_KEY_ID'] = config.get("AWS", "KEY")
os.environ['AWS_SECRET_ACCESS_KEY'] = config.get("AWS", "SECRET")

def create_spark_session():
    spark = SparkSession \
        .builder \
        .config("spark.jars.packages",
                "org.apache.hadoop:hadoop-aws:2.7.6,com.amazonaws:aws-java-sdk:1.7.4,net.java.dev.jets3t:jets3t:0.9.4") \
        .getOrCreate()
    return spark


def process_song_data(spark, input_data, output_data):
    """Read song data, transform to analytics schema, and write to parquet files.
    """
    # get filepath to song data file
    song_data = f"{input_data}song_data/*/*/*/*.json"
    
    # read song data file
    df = spark.read.json(song_data)

    # extract columns to create songs table
    df.createOrReplaceTempView("songs")
    songs_table = spark.sql("""
    SELECT DISTINCT song_id, title, artist_id, year, duration
    FROM songs
    """)
    
    # write songs table to parquet files partitioned by year and artist
    songs_table.write.partitionBy("year","artist_id").parquet(f"{output_data}songs_table", "overwrite")

    # extract columns to create artists table
    artists_table = spark.sql("""
    SELECT DISTINCT artist_id, artist_name, artist_location, artist_latitude, artist_longitude
    FROM songs
    ORDER BY artist_id
    """)
    
    # write artists table to parquet files
    artists_table.write.parquet(f"{output_data}artists_table")

# TODO: check data types
def process_log_data(spark, input_data, output_data):
    """Read log data, transform to analytics schema, and write to parquet files.
    """
    # get filepath to log data file
    log_data = f"{input_data}log_data/*.json"

    # read log data file
    df = spark.read.json(log_data)

    # extract columns for users table
    df.createOrReplaceTempView("logs")
    users_table = spark.sql("""
    SELECT DISTINCT userid, firstname, lastname, gender, level
    FROM logs
    WHERE userid IS NOT NULL
    """)
     
    # write users table to parquet files
    users_table.write.parquet(f"{output_data}users_table")

    # create timestamp column from original timestamp column
    get_timestamp = udf(lambda x: datetime.fromtimestamp(x / 1000.0))
    df = df.withColumn("timestamp", get_timestamp(df.ts))

    # create datetime column from original timestamp column
    get_datetime = udf(lambda x: datetime.fromtimestamp(x / 1000.0).strftime('%Y-%m-%d %H:%M:%S'))
    df = df.withColumn("datetime", get_datetime(df.ts))
    
    # extract columns to create time table
    df.createOrReplaceTempView("logs")
    time_table = spark.sql("""
    SELECT timestamp AS start_time
    ,hour(datetime) AS hour
    ,dayofmonth(datetime) AS day
    ,weekofyear(datetime) AS week
    ,month(datetime) AS month
    ,year(datetime) AS year
    ,weekday(datetime) AS weekday
    FROM logs
    """)
    
    # write time table to parquet files partitioned by year and month
    time_table.write.partitionBy("year","month").parquet(f"{output_data}time_table")

    # read in song data to use for songplays table
    song_df = spark.sql("""
    SELECT DISTINCT song_id, title, artist_id, year, duration
    FROM songs
    """)
    

    # extract columns from joined song and log datasets to create songplays table 
    songplays_table = spark.sql("""
    SELECT DISTINCT a.ts, year(datetime) AS year, month(datetime) AS month, a.userid, a.level, b.song_id, b.artist_id, a.sessionid, a.location, a.useragent
    FROM logs a
    INNER JOIN songs b
        ON a.artist = b.artist_name AND
        a.song = b.title
    WHERE page = 'NextSong'
    """)

    # write songplays table to parquet files partitioned by year and month
    songplays_table.write.partitionBy("year","month").parquet(f"{output_data}songplays_table")


def main():
    spark = create_spark_session()
    # input_data = "data/"
    input_data = "s3a://udacity-dend/"
    output_data = "s3a://dend-spark-bucket/analytics2/"
    # output_data = "data/output/"

    hadoop_conf = spark._jsc.hadoopConfiguration() 
    hadoop_conf.set('fs.s3.impl', 'org.apache.hadoop.fs.s3native.NativeS3FileSystem')
    hadoop_conf.set('fs.s3a.impl', 'org.apache.hadoop.fs.s3a.S3AFileSystem')

    process_song_data(spark, input_data, output_data)    
    process_log_data(spark, input_data, output_data)


if __name__ == "__main__":
    main()
