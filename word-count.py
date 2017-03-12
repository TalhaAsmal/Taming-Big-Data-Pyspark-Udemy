from pyspark import SparkConf, SparkContext

conf = SparkConf().setMaster("local").setAppName("WordCount")
sc = SparkContext(conf=conf)

lines = sc.textFile("file:///SparkCourse/Book.txt")
words = lines.flatMap(lambda x: x.split())
word_counts = words.countByValue()



for word, count in word_counts.items():
    clean_word = word.encode('ascii', 'ignore')
    if clean_word:
        print('{}, {}'.format(clean_word, count))