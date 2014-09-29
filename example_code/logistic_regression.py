from math import exp
import sys

import numpy as np
from pyspark import SparkContext
from pyspark.mllib.regression import LabeledPoint
from pyspark.mllib.classification import LogisticRegressionWithSGD


def parsePoint(line):
    """
    Parse a line of text into an MLlib LabeledPoint object.
    """
    values = [float(s) for s in line.strip().split(' ')]
    if values[0] == -1:   # Convert -1 labels to 0 for MLlib
        values[0] = 0
    return LabeledPoint(values[0], values[1:])


sc = SparkContext(appName="LogisticRegressionDemo")

# Convert lines into LabeledPoint object
points = sc.textFile(sys.argv[1]).map(parsePoint)

# Get the number of iterations
iterations = int(sys.argv[2])

# Train the model with labeled points
model = LogisticRegressionWithSGD.train(points, iterations)

# Print output in stdout
print "Final weights %s" % str(model.weights)
print "Final intercept %s" % str(model.intercept)
    
sc.stop()
