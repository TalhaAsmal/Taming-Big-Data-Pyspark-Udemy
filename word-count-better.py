from pyspark import SparkConf, SparkContext
import re


def normalize_words(text):
    return re.compile(r'\W+', re.UNICODE).split(text.lower())


conf = SparkConf().setMaster("local").setAppName("WordCount")
sc = SparkContext(conf=conf)

lines = sc.textFile("file:///SparkCourse/Book.txt")
words = lines.flatMap(normalize_words)
word_counts = words.countByValue()

for word, count in word_counts.items():
    clean_word = word.encode('ascii', 'ignore')
    if clean_word:
        print('{}, {}'.format(clean_word, count))