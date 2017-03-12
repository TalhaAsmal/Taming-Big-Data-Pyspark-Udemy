from pyspark import SparkConf, SparkContext
import collections


def parse_data(line):
    line_split = line.split(",")
    age = int(line_split[2])
    num_friends = int(line_split[3])
    return (age, num_friends)

conf = SparkConf().setMaster("local").setAppName("FriendsByAge")
sc = SparkContext(conf=conf)

lines = sc.textFile("file:///SparkCourse/fakefriends.csv")
rdd = lines.map(parse_data)
total_by_age = rdd.mapValues(lambda x: (x, 1)).reduceByKey(lambda x, y: (x[0] + y[0], x[1] + y[1]))
ave_by_age = total_by_age.mapValues(lambda x: x[0] / x[1])
results = ave_by_age.collect()

for result in results:
    print(result)
