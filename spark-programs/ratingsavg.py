from pyspark import SparkConf,SparkContext
import collections

conf=SparkConf().setMaster("local").setAppName("Avg Ratings")
sc=SparkContext(conf=conf)

rd=sc.textFile("ml-100K/u.data")
ratings=rd.map(lambda x: x.split()[2])
avg_rating=ratings.countByValue()

sortedResults = collections.OrderedDict(sorted(avg_rating.items()))

for k,v in sortedResults.items():
    print("%s %i" % (k, v))
