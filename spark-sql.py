from pyspark.sql import SparkSession, Row
import collections


def mapper(line):
    fields = line.split(',')
    return Row(ID=int(fields[0]), name=fields[1], age=int(fields[2]), numFriends=int(fields[3]))


spark = SparkSession.builder.config("spark.sql.warehouse.dir", "file:///c:/temp").appName("SparkSQL").getOrCreate()
lines = spark.sparkContext.textFile("fakefriends.csv")
people = lines.map(mapper)

schemaPeople = spark.createDataFrame(people).cache()
schemaPeople.createOrReplaceTempView("people")

teenagers = spark.sql("SELECT * from people where age >= 13 and age <=19")

for teen in teenagers.collect():
    print(teen)

schemaPeople.groupBy("age").count().orderBy("age").show()
