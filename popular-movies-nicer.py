from pyspark import SparkConf, SparkContext
import collections


def load_movie_names():
    movie_names = {}
    with open("ml-100k/u.item") as f:
        for line in f:
            line_split = line.split("|")
            movie_names[int(line_split[0])] = line_split[1]

    return movie_names

conf = SparkConf().setMaster("local[*]").setAppName("RatingsHistogram")
sc = SparkContext(conf=conf)

nameDict = sc.broadcast(load_movie_names())

lines = sc.textFile("file:///SparkCourse/ml-100k/u.data")
popular_movies = lines.map(lambda x: (nameDict.value[int(x.split()[1])], 1)).reduceByKey(lambda x, y: x+y)
popular_movies_flipped = popular_movies.map(lambda x: (x[1], x[0]))
sorted = popular_movies_flipped.sortByKey(ascending=False)

sortedResults = sorted.collect()
for result in sortedResults:
    print(result)
