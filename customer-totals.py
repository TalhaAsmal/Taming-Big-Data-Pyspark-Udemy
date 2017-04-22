from pyspark import SparkConf, SparkContext

def parse_lines(line):
    line = line.split(",")
    return (int(line[0]), float(line[2]))


conf = SparkConf().setMaster('local').setAppName("CustomerTotals")
sc = SparkContext(conf=conf)

lines = sc.textFile('file:///SparkCourse/customer-orders.csv')
costs = lines.map(parse_lines)
totals_per_customer = costs.reduceByKey(lambda x,y: x+y)

for result in totals_per_customer.collect():
    print("Customer {:2d}:\t\t{:.2f}".format(result[0], result[1]))
