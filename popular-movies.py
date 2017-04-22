from pyspark import SparkConf, SparkContext
import collections

conf = SparkConf().setMaster("local").setAppName("RatingsHistogram")
sc = SparkContext(conf=conf)

lines = sc.textFile("file:///SparkCourse/ml-100k/u.data")
popular_movies = lines.map(lambda x: (int(x.split()[1]), 1)).reduceByKey(lambda x, y: x+y)
popular_movies_flipped = popular_movies.map(lambda x: (x[1], x[0]))
sorted = popular_movies_flipped.sortByKey(ascending=False)

sortedResults = sorted.collect()
for result in sortedResults:
    print(result)
