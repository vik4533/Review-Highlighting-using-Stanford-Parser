from pyspark import SparkContext,SparkConf
from pyspark.sql import SparkSession
import nltk
from nltk.parse import stanford
import os
from nltk.tokenize import RegexpTokenizer

conf = SparkConf().setAppName("App_Name").setMaster("local[*]")
sc = SparkContext(conf=conf)
#sparkSession = SparkSession.builder.appName("first app").getOrCreate()
URI = sc._gateway.jvm.java.net.URI
Path = sc._gateway.jvm.org.apache.hadoop.fs.Path
FileSystem = sc._gateway.jvm.org.apache.hadoop.fs.FileSystem
Configuration = sc._gateway.jvm.org.apache.hadoop.conf.Configuration
fs = FileSystem.get(URI("hdfs://localhost:9000"), Configuration())
status = fs.listStatus(Path('/reviews'))
files=[]
for fileStatus in status:
	files.append(str(fileStatus.getPath()))
count=0
for iiii in files:
	print(iiii)
	data = sc.textFile(iiii)
	llist=data.collect()
	tokens=[]
	for i in llist:
		tokenizer = RegexpTokenizer(r'\w+')
		count+=len(tokenizer.tokenize(i))
########################################################\
count1=0
data = sc.textFile("hdfs://localhost:9000/fo/part-00000")
llist=data.collect()
tokens=[]
parsed_sent=[]
for i in llist:
	if(i not in parsed_sent):
		tokenizer = RegexpTokenizer(r'\w+')
		count1+=len(tokenizer.tokenize(i))
		parsed_sent.append(i)
print("number of words in reviews : ",count)
print("number of words in reviews after processing: ",count1)
####################################################################
print("percentage reduction : ",(100*count1)/count)