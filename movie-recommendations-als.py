import sys
from pyspark.mllib.recommendation import ALS, Rating
from pyspark import SparkConf, SparkContext

user_id = int(sys.argv[1])

def load_movie_names():
    movie_names = {}
    with open("ml-100k/u.item") as f:
        for line in f:
            line_split = line.split("|")
            movie_names[int(line_split[0])] = line_split[1]

    return movie_names


conf = SparkConf().setMaster("local[*]").setAppName("MovieSimilarities")
sc = SparkContext(conf=conf)

print("Loading Movie Names")
movie_names = load_movie_names()

data = sc.textFile("file:///SparkCourse/ml-100k/u.data")

ratings = data.map(lambda line: line.split()).map(lambda l: Rating(int(l[0]), int(l[1]), float(l[2]))).cache()

rank = 10
num_iters = 15
model = ALS.train(ratings, rank, num_iters)

user_ratings = ratings.filter(lambda x: x[0] == user_id).collect()

print("User {} ratings:".format(user_id))
for rating in user_ratings:
    print("{}, score: {}".format(movie_names[int(rating[1])], rating[2]))

recs = model.recommendProducts(user_id, 10)

print("Top 10 recommendations:")
for rec in recs:
    print("{}, score: {}".format(movie_names[int(rec[1])], rec[2]))


