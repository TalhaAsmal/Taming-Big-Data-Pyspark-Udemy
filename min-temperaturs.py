from pyspark import SparkConf, SparkContext
import collections


def parse_data(line):
    line_split = line.split(",")
    station = line_split[0]
    type = line_split[2]
    temp = float(line_split[3]) / 10.0 * (9.0/5.0) + 32.0
    return (station, type, temp)

conf = SparkConf().setMaster("local").setAppName("FriendsByAge")
sc = SparkContext(conf=conf)

lines = sc.textFile("file:///SparkCourse/1800.csv")
parsed_lines = lines.map(parse_data)
min_temps = parsed_lines.filter(lambda x: 'TMIN' in x[1])
station_temps = min_temps.map(lambda x: (x[0], x[2]))
min_station_temps = station_temps.reduceByKey(lambda x, y: min(x, y))
results = min_station_temps.collect()


for result in results:
    print(result)
