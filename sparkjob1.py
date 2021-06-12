from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *

spark = SparkSession \
    .builder \
    .master('local') \
    .appName('sparkjob-to-bq') \
    .getOrCreate()
bucket = "week-3-johan"
spark.conf.set('temporaryGcsBucket', bucket)
df = spark.read.csv("gs://week-3-johan/data/data_output/*.csv",
                    header=True,
                    sep=",",
                    schema="flight_date string, \
    airline_code string, \
    flight_num integer, \
    source_airport string, \
    destination_airport string, \
    departure_time integer, \
    departure_delay integer, \
    arrival_time integer, \
    arrival_delay integer, \
    airtime integer, \
    distance integer, \
    id integer"
                    )


def percentage_not_delay(time):
    if time > 0:
        res = 'delay'
    else:
        res = 'early'
    return res


convert1 = udf(percentage_not_delay, StringType())


def good_or_bad(a, b):
    if a == 'early' and b == 'early':
        res = 'good flight'
    else:
        res = 'bad flight'
    return res


convert2 = udf(good_or_bad, StringType())

df = df.withColumn('departure_delay_or_early',
                   convert1(df.departure_delay))
df = df.withColumn('arrival_delay_or_early',
                   convert1(df.arrival_delay))
df = df.withColumn('good_or_bad_flight', convert2(
    df.departure_delay_or_early, df.arrival_delay_or_early))
df = df.select(col("flight_date"), col("flight_num"), col("source_airport"), col("destination_airport"),
               col("departure_delay"), col(
    "departure_delay_or_early"), col("arrival_delay"),
    col("arrival_delay_or_early"), col("good_or_bad_flight"))

df.write.format("bigquery") \
    .option('table', "week-2-de-blank-space-johan.week_3.flights_data") \
    .save()

df.write.mode('overwrite').partitionBy("flight_date") \
    .format('parquet') \
    .save('gs://week-3-johan/output/flight_data.parquet')
