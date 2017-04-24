from pyspark import SparkConf, SparkContext
from os import environ

environ["PYTHONHASHSEED"] = "1234"


def init_node(line):
    line_split = line.strip().split()
    hero_id = int(line_split[0])
    connections = [int(a) for a in line_split[1:]]

    colour = 'WHITE'
    distance = 9999

    if hero_id == start_hero_id:
        colour = 'GREY'
        distance = 0

    return hero_id, (connections, distance, colour)


def mapper(node):
    # input format: hero_id (connection_id1 connection_id2...connection_idx distance oolour)
    hero_id = node[0]
    connections = node[1][0]
    colour = node[1][2]
    distance = node[1][1]

    res = []
    if colour == "GREY":
        res = [(int(connection), ([], distance+1, 'GREY')) for connection in connections]

        if target_hero_id in connections:
            hitCounter.add(1)

        colour = 'BLACK'

    res.append((hero_id, (connections, distance, colour)))
    return res


def reducer(data1, data2):
    # input format: (connections, distance, colour)
    col_key = ["BLACK", "GREY", "WHITE"]
    edges1 = data1[0]
    edges2 = data2[0]
    dist1 = data1[1]
    dist2 = data2[1]
    col1 = data1[2]
    col2 = data2[2]

    edges = []
    if len(edges1) > 0:
        edges.extend(edges1)

    if len(edges2) > 0:
        edges.extend(edges2)

    dist = min(dist1, dist2)
    if col1 == "WHITE" or col2 == "BLACK":
        colour = col2
    elif col1 == "GREY" and col2 == "BLACK":
        colour = "BLACK"
    elif col1 == "BLACK":
        colour = col1
    else:
        colour = "GREY"

    return (edges, dist, colour)


conf = SparkConf().setMaster("local").setAppName("DegreesOfSeparation")
sc = SparkContext(conf=conf)

graph_lines = sc.textFile('file:///SparkCourse/Marvel-Graph.txt')
iteration_rdd = graph_lines.map(init_node)

start_hero_id = 5306  # SpiderMan
target_hero_id = 14  # ADAM 3,031 (who?)
hitCounter = sc.accumulator(0)

for i in range(0, 10):
    mapped = iteration_rdd.flatMap(mapper)
    print("Processing {} values".format(mapped.count()))
    iteration_rdd = mapped.reduceByKey(reducer)

    if hitCounter.value > 0:
        print("Hit the target!\n")
        break




print("Target {} is {} degrees away from {}".format(target_hero_id, iteration_rdd.lookup(target_hero_id)[0][1:], start_hero_id))



