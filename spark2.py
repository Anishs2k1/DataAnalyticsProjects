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

def task2(input_file, output_file):
    spark = SparkSession.builder.appName('spark2').getOrCreate()

#df.groupBy('startID', 'endID').count().show()
    df = spark.read.option("header", "true").option("inferSchema" , "true").csv(input_file)
    df = df.withColumn("trip_distance", df["trip_distance"].cast(FloatType()))
    df = df.filter(df['trip_distance'] > 2)
    x1 = df.groupBy('PULocationID').agg(count(lit(1)).alias('count'))
    #x1 = x1.withColumnRenamed('PULocationID', 'DOLocationID')
    x1 = x1.withColumnRenamed('count', 'count1')

    #x1.show()
    x2 = df.groupBy('DOLocationID').agg(count(lit(1)).alias('count'))
    #x2.show()

    z = x1.join(x2,x1["PULocationID"] == x2["DOLocationID"])
    df1= z.withColumn("sum", col("count")+col("count1"))
    #index = 1
    #df1 = df1.withColumnRenamed(df1.columns[index-1],'A')
    df1 = df1.select(df1['PULocationID'], df1['sum'])
    y = df1.orderBy(desc("sum"))
    y = y.select(y['PULocationID'])
    x = y.limit(10)
    x.write.csv(output_file)



    #x = x.filter(x['PULocationID'] == x['DOLocationID'])
    #z = x.orderBy(desc("count"))
    #y = z.select(z['PULocationID'])
    #z.show()



def main():

    task2(input_file = sys.argv[1], output_file = sys.argv[2])

if __name__ == "__main__": 
    main()

