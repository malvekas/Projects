from pyspark import SparkConf,SparkContext

#Define configuration
conf=SparkConf().setMaster("local").setAppName("Maximum Temperature")
sc=SparkContext(conf=conf)

def parse_file(line):
    l=line.split(",")
    station = l[0]
    entry_type=l[2]
    temp=float(l[3])*0.1*(9.0/5.0) + 32.0
    return station,entry_type,temp

#First read file
readfile=sc.textFile("1800.csv")
parse_rdd=readfile.map(parse_file)
stationrdd=parse_rdd.filter(lambda x: x[1]=="TMAX").map(lambda x: (x[0],x[2]))
resultrdd=stationrdd.reduceByKey(lambda x,y:max(x,y))
result=resultrdd.collect()
for i in result:
    print(i[0]+"\t{:.2f}F".format(i[1]))