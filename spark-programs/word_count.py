from pyspark import SparkConf,SparkContext

#Define configuration
conf=SparkConf().setMaster("local").setAppName("Word Count")
sc=SparkContext(conf=conf)

book=sc.textFile("book.txt")
rdd=book.flatMap(lambda x:x.split())
countrd=rdd.countByValue()
for k,v in countrd.items():
    clean=k.encode("ascii","ignore")
    if clean:
        print(clean,v)