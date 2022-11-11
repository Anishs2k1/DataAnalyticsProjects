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
from pyspark.sql.window import Window
from pyspark.sql.functions import col, row_number


def task4(input_file, map, output_file):
    spark = SparkSession.builder.appName('spark2').getOrCreate()
    df = spark.read.option("header", "true").option("inferSchema" , "true").csv(input_file)
    df = df.filter(df['trip_distance'] > 2)
    mapdf = spark.read.option("header", "true").option("inferSchema" , "true").csv(map)
    combdf = df.join(mapdf,df["PULocationID"] == mapdf["LocationID"])
    combdf = combdf.withColumn("hour", hour(combdf["tpep_pickup_datetime"]))
    #combdf.select(combdf['tpep_pickup_datetime'], combdf['hour']).show()
    selecdf = combdf.select(combdf['PULocationID'],combdf['hour'], combdf['Zone'], combdf['Borough'])
    x = selecdf.groupBy('Borough', 'Zone', 'hour').agg(count(lit(1)).alias('count'))
    x = x.where(x.Borough=='Brooklyn')
    windowDept = Window.partitionBy("hour").orderBy(col("count").desc(), col("Zone"))
    z = x.withColumn("row",row_number().over(windowDept)).filter(col("row") == 1).drop("row")
    z = z.select(z['hour'], z['zone'])
    z = z.withColumn('hour', format_string("%02d", col('hour').cast('int')))

    #z.show()
    z.write.csv(output_file)









    #z = x.groupBy("hour").agg({'count':'max'})

    #index = 2
    #z = z.withColumnRenamed(z.columns[index-1],'max')
    #z.show()
    #resdf = z.join(x, z.max == x.count)
    #resdf.show()











    #selecdf.show()
    #dropdf = df.groupBy('DOLocationID').agg(count(lit(1)).alias('count'))  
    #combdf = dropdf.join(mapdf,dropdf["DOLocationID"] == mapdf["LocationID"])  
    #y = combdf.orderBy(desc("count"))
    #bordf = selecdf.groupBy('hour', "Zone").agg(count(lit(1)).alias('count'))
    #temp  = bordf.orderBy('hour')
    #temp.write.csv(output_file)
    #bordf.write.csv(output_file)
    #bordf.show()

    #x = bordf.groupBy("hour").agg(max("count").alias('max'))


    #x = x.withColumnRenamed('hour','hour1')

    #new = bordf.join(x,bordf["count"] == x["max"])

    #new.orderBy("hour").show()


    #z.join(new, z[])
    #new.show()
    #y = new.select(new['hour'], new['Zone'])
    #y = y.orderBy("hour")
    #y = finaldf.orderBy(desc("avg"))
    #resdf = selecdf.join(x, selecdf("count") == x("max"))
    #windowDept = Window.partitionBy("hour")
    #bordf.withColumn('maxcount', F.max('Count').over(windowDept)).where(F.col('Count') == F.col('maxcount')).drop('maxcount').show()
    #bordf.withColumn("row",row_number().over(windowDept)).filter(col("row") == 1).drop("row").show()
    #finaldf = finaldf.groupBy('hour').agg(F.max('count'))
    #dd = finaldf.groupby('hour').count().sort(finaldf['count'].desc())
    #finaldf.show()
    #y.show()



def main():

    task4(input_file = sys.argv[1], map = sys.argv[2], output_file = sys.argv[3])

if __name__ == "__main__": 
    main()