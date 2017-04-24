from pyspark import SparkConf, SparkContext


def parse_data(line):
    line_split = line.strip().split(" ")
    return line_split[0], len(line_split) - 1


def load_superhero_names():
    superhero_names = {}
    with open("Marvel-Names.txt") as f:
        for line in f:
            line_split = line.strip().split("\"")
            superhero_names[int(line_split[0])] = line_split[1]

    return superhero_names


def parse_superhero_names(line):
    line_split = line.strip().split("\"")
    return int(line_split[0]), line_split[1]


conf = SparkConf().setMaster("local").setAppName("MostPopulatSuperhero")
sc = SparkContext(conf=conf)

superhero_dict = sc.broadcast(load_superhero_names())

superhero_lines = sc.textFile('file:///SparkCourse/Marvel-Names.txt')
superhero_rdd = superhero_lines.map(parse_superhero_names)

graph_lines = sc.textFile('file:///SparkCourse/Marvel-Graph.txt')
summed_graph = graph_lines.map(parse_data).reduceByKey(lambda x, y: x+y)
flipped_sum = summed_graph.map(lambda x: (x[1], int(x[0])))
max_connections = flipped_sum.max()

print(superhero_rdd.lookup(max_connections[1])[0])
print(superhero_dict.value[max_connections[1]])



