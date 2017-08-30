import argparse
from pyspark import SparkConf, SparkContext, HiveContext

# parse arguments
parser = argparse.ArgumentParser()
parser.add_argument("arg1") 	# naming it "echo"
args = parser.parse_args()	# returns data from the options specified (echo)
print(args.arg1)

sc = SparkContext(conf=SparkConf().setAppName("test"))
sqlContext = HiveContext(sc)

sqlContext.sql("SHOW DATABASES").show()

from util import util
print util.add(1,2)


