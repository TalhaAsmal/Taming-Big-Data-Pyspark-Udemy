from pyspark import SparkConf, SparkContext
from math import sqrt
import sys


def load_movie_names():
    movie_names = {}
    with open("ml-100k/u.item") as f:
        for line in f:
            line_split = line.split("|")
            movie_names[int(line_split[0])] = line_split[1]

    return movie_names


def process_line(line):
    line_split = line.strip().split("\t")
    return int(line_split[0]), (int(line_split[1]), float(line_split[2]))


def map_movies_keys(line):
    # input format: (user_id, ((movie1, rating1), (movie2, rating2))
    rating_group_0 = line[1][0]
    rating_group_1 = line[1][1]
    return (rating_group_0[0], rating_group_1[0]), (rating_group_0[1], rating_group_1[1])


def get_cosine_measure(ratings):
    sum_xx = 0
    sum_yy = 0
    sum_xy = 0
    pairs = len(ratings)
    for rating in ratings:
        sum_xx += rating[0] * rating[0]
        sum_yy += rating[1] * rating[1]
        sum_xy += rating[0] * rating[1]

    denom = sqrt(sum_xx) * sqrt(sum_yy)
    score = sum_xy / float(denom) if denom else 0
    return (score, pairs)


conf = SparkConf().setMaster("local[6]").setAppName("MovieSimilarities")
sc = SparkContext(conf=conf)

print("Loading Movie Names")
nameDict = load_movie_names()

nameDict = sc.broadcast(load_movie_names())

lines = sc.textFile("file:///SparkCourse/ml-100k/u.data")
popular_movies = lines.map(process_line)  # (user_id, (movie, rating))
popular_movies_joined = popular_movies.join(popular_movies).filter(lambda x: x[1][0][0] < x[1][1][0])  # (user_id, ((movie1, rating1), (movie2, rating2))
popular_movies_mapped = popular_movies_joined.map(map_movies_keys).groupByKey()  # (movie1, movie2), (rating1, rating2)
popular_movies_similarity = popular_movies_mapped.mapValues(get_cosine_measure).cache() # (movie1, movie2), (similarity, num_occurances)


if len(sys.argv) > 1:
    score_threshold = 0.97
    co_occurance_threshold = 50

    movie_id = int(sys.argv[1])

    filtered_results = popular_movies_similarity.filter(lambda x: (x[0][0] == movie_id or x[0][1] == movie_id) and
                                                                  (x[1][0] >= score_threshold) and
                                                                  (x[1][1] >= co_occurance_threshold)) # (movie1, movie2), (similarity, num_occurances)

    sorted_filtered_results = filtered_results.map(lambda x: (x[1], x[0])).sortByKey(ascending=False).take(10)

    print("Top 10 similar movies:\n")
    for result in sorted_filtered_results:
        (scores, movies) = result
        similar_movie = nameDict.value[movies[0] if movies[0] != movie_id else movies[1]]
        print("{}\tscore: {}\tstrength: {}".format(similar_movie, scores[0], scores[1]))
