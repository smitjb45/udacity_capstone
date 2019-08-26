import configparser
from datetime import datetime
import os
import pandas as pd
from pyspark.sql import SparkSession
from pyspark.sql.functions import udf, col, lit, concat
from pyspark.sql.functions import year, month, dayofmonth, hour, weekofyear, date_format


config = configparser.ConfigParser()
config.read('dl.cfg')

# os.environ['AWS_ACCESS_KEY_ID']=config.get('S3', 'AWS_ACCESS_KEY_ID')
# os.environ['AWS_SECRET_ACCESS_KEY']=config.get('S3', 'AWS_SECRET_ACCESS_KEY')


def create_spark_session():
    spark = SparkSession \
            .builder \
            .appName("Capstone Cluster") \
            .getOrCreate()
    return spark

def clean_json(json, spark):
    code_list = []
    definition_list = []
    
    for data in json.toPandas().data[0]:
        code_list.append(data[8])
        definition_list.append(data[9])
    
    df_codes = pd.DataFrame(columns=['Code','Definition'])
    
    df_codes['Code'] = code_list
    df_codes['Definition'] = definition_list
    
    df_codes_spark  = spark.createDataFrame(df_codes)
    
    return df_codes_spark

def is_data_quality_good(table_dataset, table_name):
   
    if table_dataset.count() == 0:
        print(r"{} Empty Table").format(table_name)
        return False
    
    
    if table_dataset.count() > ticket_fact_df.dropDuplicates().count():
        print(r"{} Table has Duplicates").format(table_name)
        return False
    
    return True
    
    

def process_vehicle_data(ticket_dataset, output_data):
    """
    pass in ticket data and create the vehicle table
    :ticket_dataset: the main ticket dataframe
    :param input_data: Input url
    :param output_data: Output location
    """
    
    df_vehicle_table = ticket_dataset.select(col('Plate ID').alias('plate_id')
                                             ,col('Vehicle Make').alias('vehicle_make')\
                                             ,col('Vehicle Body Type').alias('vehicle_body_type')
                                             ,col('Vehicle Color').alias('vehicle_color')\
                                             ,col('Vehicle Year').alias('vehicle_year')).dropDuplicates()
    
    df_vehicle_table.write.parquet(output_data + "VehicleTable.parquet")
    
    return df_vehicle_table

def process_registration_data(ticket_dataset, output_data):
    """
    pass in ticket data and create the registration table
    :ticket_dataset: the main ticket dataframe
    :param input_data: Input url
    :param output_data: Output location
    """
    
    df_registration_table = ticket_dataset.select(col('Plate ID').alias('plate_id')\
                                                  ,col('Plate Type').alias('plate_type')\
                                                  ,col('Registration State').alias('registration_state')\
                                                  ,col('Vehicle Expiration Date').alias('registration_expired_date')\
                                                  ,col('Unregistered Vehicle?').alias('unregistered_vehicle')).dropDuplicates()
    
    df_registration_table.write.parquet(output_data + "RegistrationTable.parquet")
    
    
    return df_registration_table
    
def process_violation_location_data(ticket_dataset, output_data):
    """
    pass in ticket data and create the violation location table
    :ticket_dataset: the main ticket dataframe
    :param input_data: Input url
    :param output_data: Output location
    """
    
    df_violation_location_table = ticket_dataset.select(col('Street Code1').alias('street_code1')
                                                        ,col('Street Code2').alias('street_code2')\
                                                        ,col('Street Code3').alias('street_code3')
                                                        ,col('Violation Precinct').alias('violation_precinct')\
                                                        ,col('Violation County').alias('violation_county')\
                                                        ,col('House Number').alias('house_number')\
                                                        ,col('Street Name').alias('street_name') \
                                                        ,col('Days Parking In Effect    ').alias('parking_enforced_days')\
                                                        ,col('From Hours In Effect').alias('from_enforced_hours')\
                                                        ,col('To Hours In Effect').alias('to_enforced_hours')).dropDuplicates()
    
    
    df_violation_location_table = df_violation_location_table.withColumn("street_code_key", \
                                    concat(col("street_code1")\
                                           ,lit('-')\
                                           ,col("street_code2")\
                                           ,lit('-')\
                                           ,col("street_code3"))).dropDuplicates()
    
    df_violation_location_table.write.parquet(output_data + "ViolationLocationTable.parquet")
    
    return df_violation_location_table
    
def process_codes_data(ticket_dataset, df_codes_spark, output_data):
    """
    pass in ticket data and create the violation location table
    :ticket_dataset: the main ticket dataframe
    :param input_data: Input url
    :param output_data: Output location
    """
    
    df_codes_joined_spark = df_codes_spark.join(\
                            ticket_dataset.select(col('Law Section').alias('law_section'),\
                                             col('Sub Division').alias('sub_division'),\
                                             col('Violation Code').alias('violation_code')))\
                            .where(ticket_dataset['Violation Code'] == df_codes_spark['Code']).dropDuplicates()
    
    df_codes_joined_spark.write.parquet(output_data + "CodesTable.parquet")
    
    
def create_fact_ticket_location_data(ticket_dataset, df_violation_location_table, output_data):
    """
    pass in ticket data and create the violation location table
    :ticket_dataset: the main ticket dataframe
    :param input_data: Input url
    :param output_data: Output location
    """
    
    ticket_fact_df = ticket_dataset.join(df_violation_location_table)\
    .where((df_ticket['Street Code1'] == df_violation_location_table['street_code1']) \
    & (df_ticket['Street Code2'] == df_violation_location_table['street_code2']) \
    & (df_ticket['Street Code3'] == df_violation_location_table['street_code3'])).dropDuplicates()
    
    ticket_fact_df.write.parquet(output_data + "TicketTable.parquet")


def main():
    spark = create_spark_session()
    ticket_dataset = spark.read.format("csv").option("header", "true").load("parking-violations-issued-fiscal-year-2018.csv")
    df_ticket_code = spark.read.json("parking_violation codes.json", multiLine=True)
    S3_bucket = "dump/"
    
    cleaned_json = clean_json(df_ticket_code, spark)
    process_vehicle_data(ticket_dataset,  S3_bucket)
    process_registration_data(ticket_dataset,  S3_bucket)
    df_violation_location_table = process_violation_location_data(ticket_dataset,  S3_bucket)
    process_codes_data(ticket_dataset, cleaned_json, S3_bucket)
    create_fact_ticket_location_data(ticket_dataset, df_violation_location_table, S3_bucket)

if __name__ == "__main__":
    main()
