from pyspark import SparkContext,SparkConf
from pyspark.sql import SparkSession
import nltk
from nltk.parse import stanford
import os
from nltk.tokenize import RegexpTokenizer

################################################################

def check(w,q,tokc):
	ma=max(tokc)
	if(ma<w):
		return False
	else:
		count=0
		for i in range(w,ma+1):
			count+=tokc.count(i)
		if(count>=q):
			return True
		else:
			return False

###################################################################

conf = SparkConf().setAppName("App_Name").setMaster("local[*]")
sc = SparkContext(conf=conf)
#sparkSession = SparkSession.builder.appName("first app").getOrCreate()
URI = sc._gateway.jvm.java.net.URI
Path = sc._gateway.jvm.org.apache.hadoop.fs.Path
FileSystem = sc._gateway.jvm.org.apache.hadoop.fs.FileSystem
Configuration = sc._gateway.jvm.org.apache.hadoop.conf.Configuration
fs = FileSystem.get(URI("hdfs://localhost:9000"), Configuration())
status = fs.listStatus(Path('/output'))
files=[]
for fileStatus in status:
	files.append(str(fileStatus.getPath()))

########################################################################
tokenizer = RegexpTokenizer(r'\w+')
expdict={}
for ii in files:
	exp=ii+"/exp/part-00000"
	data = sc.textFile(exp)
	llist=data.collect()
	for ij in llist:
		it=eval(ij)
		if(it[0] in expdict):
			expdict[it[0]]+=tokenizer.tokenize(it[1])
		else:
			expdict[it[0]]=tokenizer.tokenize(it[1])
w=2
q=2
impdict=[]
c=0
for i in files:
	exp=ii+"/imp/part-00000"
	data = sc.textFile(exp)
	llist=data.collect()
	for ij in llist:
		c=c+1
		tokcou=[]
		it=eval(ij)
		if it[0] in expdict:
			li=expdict[it[0]] #list of words
			tok=tokenizer.tokenize(it[1])
			for i in tok:
				tokcou.append(li.count(i))
			if(check(w,q,tokcou)):
				impdict.append((it[0],it[1]))
#output
out=[]
for ii in files:
	exp=ii+"/exp/part-00000"
	data = sc.textFile(exp)
	llist=data.collect()
	for ij in llist:
		it=eval(ij)
		out.append(it[1])
for i in impdict:
	out.append(i[1])
rdd = sc.parallelize(out)
rdd.coalesce(1, shuffle = True).saveAsTextFile("hdfs://localhost:9000/fo")
print(impdict)
print(c)
print(len(impdict))
