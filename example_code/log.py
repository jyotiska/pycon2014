import sys
from operator import add

from pyspark import SparkContext

# Intialize the Spark Context with app name
sc = SparkContext(appName="LogProcessingDemo")

# Get the lines from the textfile
lines = sc.textFile(sys.argv[1], 1)

# Filter out the lines which contain post requests
post_requests = lines.filter(lambda x: "POST" in x)

# Filter out the lines with status code as 404
valid_responses = lines.filter(lambda x: "200" in x)

# Count the number of lines and show them on output
print "Number of POST requests are %s" % (post_requests.count())
print "Number of 200 responses are %s" % (valid_responses.count())

sc.stop()
