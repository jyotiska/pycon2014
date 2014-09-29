import sys
from operator import add

from pyspark import SparkContext

# Intialize the Spark Context with app name
sc = SparkContext(appName="WordCountDemo")

# Get the lines from the textfile
lines = sc.textFile(sys.argv[1], 1)

# Filter out the lines which are empty
lines_nonempty = lines.filter( lambda x: len(x) > 0 )

# Get all words and their frequencies and sort them in descending order
words = lines_nonempty.flatMap(lambda x: x.split())
wordcounts = words.map(lambda x: (x, 1)).reduceByKey(lambda x,y:x+y).map(lambda x:(x[1],x[0])).sortByKey(False)

# Show the top 5 words by frequency
print wordcounts.take(5)

sc.stop()
