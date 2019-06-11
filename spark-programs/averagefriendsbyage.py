from pyspark import SparkConf,SparkContext

def getkv(line):
    val=line.split(',')
    age=int(val[2])
    friends=int(val[3])
    return (age,friends)

conf=SparkConf().setMaster("local").setAppName("Average Friends By Age")
sc=SparkContext(conf=conf)

rdd=sc.textFile("fakefriends.csv")
agerdd=rdd.map(getkv)
agekv=agerdd.mapValues(lambda x:(x,1)).reduceByKey(lambda x,y:(x[0]+y[0],x[1]+y[1]))
avgfriend=agekv.mapValues(lambda x:x[0]//x[1])
result=avgfriend.collect()
result.sort()
for i in result:
    print(i)
 