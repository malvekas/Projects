from pyspark import SparkConf,SparkContext


conf=SparkConf().setMaster("local").setAppName("Minimum Temperature")
sc=SparkContext(conf=conf)

def parse_file(line):
    l=line.split(',')
    station=l[0]
    entry_type=l[2]
    temp=float(l[3])*0.1*(9.0/5.0) + 32.0
    return station,entry_type,temp


rdd=sc.textFile("1800.csv")
r=rdd.map(parse_file)
min_temp=r.filter(lambda x: 'TMIN' in x[1])
station_temp=min_temp.map(lambda x:(x[0],x[2]))
resultrdd=station_temp.reduceByKey(lambda x,y:min(x,y))
result=resultrdd.collect()
for i in result:
    print(i[0]+ "\t{:.2f}F".format(i[1]))