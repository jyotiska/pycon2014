import sys

from pyspark import SparkContext
from pyspark.sql import SQLContext

# Intialize the Spark Context with app name
sc = SparkContext(appName="PythonSQLDemo")

# Create the SQL context from the existing Spark Context
sqlContext = SQLContext(sc)

# Create a SchemaRDD from the JSON file
people = sqlContext.jsonFile(sys.argv[1])

# Print the schema of the JSON file
people.printSchema()

# Register this SchemaRDD as a table.
people.registerAsTable("people")

# SQL statements can be run by using the sql methods provided by sqlContext.
teenagers = sqlContext.sql("SELECT name FROM people WHERE age >= 13 AND age <= 19")

# Get the names of the teenagers and store in the list
teenagers_names = [str(each[0]) for each in teenagers.collect()]

# Print the names of the teenagers to the stdout
print teenagers_names

sc.stop()
