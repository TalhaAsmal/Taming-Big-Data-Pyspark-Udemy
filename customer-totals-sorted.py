from pyspark import SparkConf, SparkContext

def parse_lines(line):
    line = line.split(",")
    return (int(line[0]), float(line[2]))


conf = SparkConf().setMaster('local').setAppName("CustomerTotals")
sc = SparkContext(conf=conf)

lines = sc.textFile('file:///SparkCourse/customer-orders.csv')
costs = lines.map(parse_lines)
totals_per_customer = costs.reduceByKey(lambda x,y: x+y)
totals_by_amount = totals_per_customer.map(lambda x: (x[1], x[0])).sortByKey(ascending=False).collect()

for result in totals_by_amount:
    print("Customer {:2d}:\t\t{:.2f}".format(result[1], result[0]))
