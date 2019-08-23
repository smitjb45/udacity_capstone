import configparser
from datetime import datetime
import os
import pandas as pd
from pyspark.sql import SparkSession
from pyspark.sql.functions import udf, col, lit, concat
from pyspark.sql.functions import year, month, dayofmonth, hour, weekofyear, date_format


config = configparser.ConfigParser()
config.read('dl.cfg')

os.environ['AWS_ACCESS_KEY_ID']=config.get('S3', 'AWS_ACCESS_KEY_ID')
os.environ['AWS_SECRET_ACCESS_KEY']=config.get('S3', 'AWS_SECRET_ACCESS_KEY')


def create_spark_session():
    spark = SparkSession \
        .builder \
        .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:2.7.0") \
        .getOrCreate()
    return spark

def process_vehicle_data(ticket_dataset, input_data, output_data):
    """
    pass in ticket data and create the vehicle table
    :ticket_dataset: the main ticket dataframe
    :param input_data: Input url
    :param output_data: Output location
    """
    
    df_vehicle_table = ticket_dataset.select(col('Plate ID').alias('plate_id'), col('Vehicle Make').alias('vehicle_make')\
                                    ,col('Vehicle Body Type').alias('vehicle_body_type'), col('Vehicle Color').alias('vehicle_color')\
                                    ,col('Vehicle Year').alias('vehicle_year'))

def process_registration_data(ticket_dataset, input_data, output_data):
    """
    pass in ticket data and create the registration table
    :ticket_dataset: the main ticket dataframe
    :param input_data: Input url
    :param output_data: Output location
    """
    
    df_registration_table = ticket_dataset.select(col('Plate ID').alias('plate_id'), col('Plate Type').alias('plate_type')\
                                         ,col('Registration State').alias('registration_state'), col('Vehicle Expiration Date').alias('registration_expired_date')\
                                        ,col('Unregistered Vehicle?').alias('unregistered_vehicle'))
    
def process_violation_location_data(ticket_dataset, input_data, output_data):
    """
    pass in ticket data and create the violation location table
    :ticket_dataset: the main ticket dataframe
    :param input_data: Input url
    :param output_data: Output location
    """
    
    df_violation_location_table = ticket_dataset.select(col('Street Code1').alias('street_code1'), col('Street Code2').alias('street_code2')\
                                         ,col('Street Code3').alias('street_code3'), col('Violation Precinct').alias('violation_precinct')\
                                        ,col('Violation County').alias('violation_county'),col('House Number').alias('house_number')
                                        ,col('Street Name').alias('street_name'),col('Days Parking In Effect    ').alias('parking_enforced_days')
                                        ,col('From Hours In Effect').alias('from_enforced_hours'),col('To Hours In Effect').alias('to_enforced_hours'))
    
    
    df_violation_location_table = df_violation_location_table.withColumn("street_code_key", \
                                    concat(col("street_code1"), lit('-'),col("street_code2"), lit('-'),col("street_code3"))) 
    

def process_violation_location_data(ticket_dataset, input_data, output_data):
    """
    pass in ticket data and create the violation location table
    :ticket_dataset: the main ticket dataframe
    :param input_data: Input url
    :param output_data: Output location
    """
    
    df_violation_location_table = ticket_dataset.select(col('Street Code1').alias('street_code1'), col('Street Code2').alias('street_code2')\
                                         ,col('Street Code3').alias('street_code3'), col('Violation Precinct').alias('violation_precinct')\
                                        ,col('Violation County').alias('violation_county'),col('House Number').alias('house_number')
                                        ,col('Street Name').alias('street_name'),col('Days Parking In Effect    ').alias('parking_enforced_days')
                                        ,col('From Hours In Effect').alias('from_enforced_hours'),col('To Hours In Effect').alias('to_enforced_hours'))

def process_song_data(spark, input_data, output_data):
    """
    Read song data and process it and save to provided output location
    :param spark: Spark session
    :param input_data: Input url
    :param output_data: Output location
    """
    
    # get filepath to song data file
    song_data = input_data + "song_data/*/*/*/*.json"
    
    # read song data file
    df = spark.read.json(song_data).dropDuplicates()

    # extract columns to create songs table
    global songs_table
    songs_table = df.select(['song_id', 'title', 'artist_id', 'year', 'duration'])
    
    # write songs table to parquet files partitioned by year and artist
    songs_table.write.partitionBy("year", "artist_id").parquet("output_data + SongTable.parquet")

    # extract columns to create artists table
    artists_table = df.select(['artist_id', 'artist_name', 'artist_location', 'artist_latitude', 'artist_longitude'])
    
    # write artists table to parquet files
    artists_table.write.parquet(output_data + "ArtistTable.parquet")


def process_log_data(spark, input_data, output_data):
    """
    Read log data and process it and save to provided output location
    :param spark: Spark session
    :param input_data: Input url
    :param output_data: Output location
    """
    # get filepath to log data file
    log_data = input_data + "log_data/*.json"

    # read log data file
    df = spark.read.json("data/log_data/*.json").dropDuplicates()
    
    # filter by NextSong actions for song plays
    df = df.filter('page = "NextSong"')

    # extract columns for users table    
    artists_table = df.select(['userId', 'firstName', 'lastName', 'gender', 'level']) 
    
    # write users table to parquet files
    artists_table.write.parquet(output_data + "UserTable.parquet")

    # create timestamp column from original timestamp column
    get_timestamp = udf( lambda x: datetime.fromtimestamp(x / 1000).strftime('%Y-%m-%d %H:%M:%S'))
    df = df.withColumn("time_stamp", get_time_stamp(dfLog.ts))
    
    # extract columns to create time table
    time_table = dfL.select('ts', hour('time_stamp').alias('hour'), dayofmonth('time_stamp').alias('day'), weekofyear('time_stamp').alias('week')
                            , month('time_stamp').alias('month'), year('time_stamp').alias('year'), date_format('time_stamp', 'EEEE').alias('weekday'))
    
    # write time table to parquet files partitioned by year and month
    time_table.write.partitionBy("year", "month").parquet(output_data + "SongTable.parquet")

    # read in song data to use for songplays table
    song_df = songs_table

    # extract columns from joined song and log datasets to create songplays table 
    songplays_table = song_df.join(dfLog).where((artist_and_song.title ==  dfLog.song) &  
                                                (artist_and_song.artist_name == dfLog.artist) & 
                                                (artist_and_song.duration == dfLog.length)).select('ts', 'userid', 'level', \
                                                                                                   'song_id', 'artist_id','sessionid', \
                                                                                                   'location', 'useragent')

    # write songplays table to parquet files partitioned by year and month
    songplays_table.join(time_table.select('ts', 'year', 'month')).where(songplays_table.ts ==  time_table.ts).write.partitionBy("year", "month").parquet(output_data + "songplaysTable.parquet")


def main():
    spark = create_spark_session()
    input_data = "s3a://udacity-dend/"
    output_data = ""
    
    process_song_data(spark, input_data, output_data)    
    process_log_data(spark, input_data, output_data)


if __name__ == "__main__":
    main()
