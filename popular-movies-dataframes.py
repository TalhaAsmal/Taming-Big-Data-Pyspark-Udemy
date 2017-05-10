from pyspark.sql import SparkSession, Row, functions


def movie_name_mapper(line):
    line_split = line.split("|")
    return Row(movie_id=int(line_split[0]), movie_name=line_split[1])

spark = SparkSession.builder.config("spark.sql.warehouse.dir", "file:///c:/temp").appName("PopularMoviesDF").getOrCreate()

name_lines = spark.sparkContext.textFile("file:///SparkCourse/ml-100k/u.item")
movie_names = name_lines.map(movie_name_mapper)
movie_names_df = spark.createDataFrame(movie_names)

lines = spark.sparkContext.textFile("file:///SparkCourse/ml-100k/u.data")
movies = lines.map(lambda x: Row(movie_id=int(x.split()[1])))
movie_dataset = spark.createDataFrame(movies)

top_movie_ids = movie_dataset.join(movie_names_df, on="movie_id").groupBy("movie_name").count().orderBy("count", ascending=False).cache()
top_movie_ids.show()

spark.stop()
