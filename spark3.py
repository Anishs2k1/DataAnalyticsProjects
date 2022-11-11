from pyspark.sql import SparkSession
from pyspark.sql import DataFrame
from pyspark.sql.types import FloatType
from pyspark.sql.types import IntegerType
from pyspark.sql.functions import sum
from pyspark.sql.functions import round 
from pyspark.sql.functions import *
from pyspark.sql.functions import avg, col, desc
import pyspark.sql.functions as F
from pyspark.sql.types import DoubleType
from pyspark.sql.types import StringType
from pyspark.sql.functions import regexp_replace

def task3(input_file, output_file):
    spark = SparkSession.builder.appName('spark2').getOrCreate()
    df = spark.read.option("header", "true").option("inferSchema" , "true").csv(input_file)
    df = df.filter(df['trip_distance'] > 2)
    df = df.withColumn("date_type", to_date("tpep_pickup_datetime"))
    newdf = df.select(df['date_type'], df['tpep_pickup_datetime'])
    xdf = newdf.groupBy('date_type').agg(count(lit(1)).alias('count'))
    datedf = xdf.withColumn("day", date_format(xdf["date_type"], "EEEE"))
    count_of_days = datedf.groupBy('day').agg(count(lit(1)).alias('count'))
    newtotal = datedf.groupBy("day").agg(sum("count").alias("totalcount")) 
    newtotal = newtotal.withColumnRenamed('day','day1')
    finaldf = newtotal.join(count_of_days,newtotal["day1"] == count_of_days["day"])
    finaldf = finaldf.withColumn("avg", finaldf['totalcount'] / finaldf['count'])
    y = finaldf.orderBy(desc("avg"))
    y = y.select(y['day'])
    x = y.limit(3)
    x.write.csv(output_file)
   

    #df.withColumn('tpep_pickup_datetime', to_timestamp('input_timestamp').cast('date')).show(truncate=False)




def main():

    task3(input_file = sys.argv[1], output_file = sys.argv[2])

if __name__ == "__main__": 
    main()
