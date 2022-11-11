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



def task1(input_file, output_file): 
    spark = SparkSession.builder.appName('spark1').getOrCreate()

#df = spark.read.csv('/Users/zaha/spark_project/data.csv')
    df = spark.read.option("header", "true").option("inferSchema" , "true").csv(input_file)
    #"/Users/zaha/spark_project/data.csv"
    #"/Users/zaha/spark_project/output001.csv"

    data_df = df.withColumn("trip_distance", df["trip_distance"].cast(FloatType()))
    data1_df = data_df.withColumn("PULocationID", data_df["PULocationID"].cast(IntegerType()))



    data2 = data1_df.filter(data1_df['trip_distance'] > 2)

    finaldf = data2.groupBy("PULocationID").agg(sum("trip_distance").alias("tripsum"))

    x = finaldf.withColumn("tripsum", round("tripsum", 2))

    t = x.withColumn("tripsum", (format_number(('tripsum'),2 )))

    t = t.withColumn('tripsum', regexp_replace('tripsum', "\'", ""))
    t = t.withColumn('tripsum', regexp_replace('tripsum', "\,", ""))


    z = t.orderBy(t['PULocationID'])



    z.write.csv(output_file)


def main():

    task1(input_file = sys.argv[1], output_file = sys.argv[2])

if __name__ == "__main__": 
    main()

