from pyspark.ml.regression import LinearRegression
from pyspark.sql import SparkSession
from pyspark.sql.functions import col
from pyspark.ml.linalg import Vectors

if __name__ == '__main__':
    spark = SparkSession.builder.config('spark.sql.warehouse.dir', 'file:///c:/temp').appName("MLLibLinearRegression").getOrCreate()
    df = spark.read.csv('file:///SparkCourse/regression.txt')
    replacements = {"_c0": "labels", "_c1": "features"}
    df = df.select([col(c).alias(replacements.get(c, c)) for c in df.columns])

    trainTest = df.randomSplit([0.5, 0.5])
    train = trainTest[0]
    test = trainTest[1]

    lr = LinearRegression(maxIter=10, regParam=0.3, elasticNetParam=0.8)
    model = lr.fit(train)

    fullPredictions = model.transform(test).cache()
    predictions = fullPredictions.select("prediction").rdd.map(lambda x: x[0])
