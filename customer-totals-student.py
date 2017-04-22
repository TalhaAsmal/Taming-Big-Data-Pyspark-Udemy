from pyspark import SparkConf, SparkContext
conf = SparkConf().setMaster("local").setAppName("Assignment_1")
sc = SparkContext(conf = conf)
def parseLine(line):
    fields = line.split(',')
    customer_id = int(fields[0])
    cost = float(fields[2])
    return (customer_id, cost)
lines = sc.textFile("file:///SparkCourse/customer-orders.csv")
rdd = lines.map(parseLine)
totalsById = rdd.reduceByKey(lambda x,y: x+y)
results = totalsById.collect()
for result in results:
    print(result)