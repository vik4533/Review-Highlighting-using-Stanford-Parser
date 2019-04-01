from pyspark import SparkContext,SparkConf
from pyspark.sql import SparkSession
import nltk
from nltk.parse import stanford
import os
from nltk.tokenize import RegexpTokenizer

os.environ['STANFORD_PARSER'] = 'C:/Users/VIK/Downloads/stanford-parser-full-2018-10-17/stanford-parser-full-2018-10-17/stanford-parser.jar'
os.environ['STANFORD_MODELS'] ='C:/Users/VIK/Downloads/stanford-parser-full-2018-10-17/stanford-parser-full-2018-10-17/stanford-parser-3.9.2-models.jar'

def calc1(st,tok):
	beta=0
	alpha=''
	stl=st.split(",")
	for i in stl:
		k=i.count(')')
		if(k>beta):
			beta=k
			alpha=i
	alpha=alpha.split("'")[1]
	lam=tok.index(alpha)
	if(lam>beta):
		extset=tok[(-1)*(beta+1):]
	else:
		extset=tok
	print(alpha,beta,lam,' '.join(extset))
	return ' '.join(extset)

parser = stanford.StanfordParser(model_path="C:/Users/VIK/Downloads/stanford-parser-full-2018-10-17/stanford-parser-full-2018-10-17/edu/stanford/nlp/models/lexparser/englishPCFG.ser.gz")
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
#################################################################################
for iiii in files:
	print(iiii)
	data = sc.textFile(iiii)
	llist=data.collect()
	dict1={}
	tokens=[]
	for i in llist:
		tokenizer = RegexpTokenizer(r'\w+')
		tokens=tokenizer.tokenize(i)
		for j in nltk.pos_tag(tokens):
			wo=j[0].lower()
			if(j[1]=="NN" or j[1]=="NNS" or j[1]=="NNP" or j[1]=="NNPS"):
				if wo in dict1.keys():
					dict1[wo]=dict1[wo]+1
				else:
				 	dict1[wo]=1
	sorted_x = sorted(dict1.items(), key=lambda kv: kv[1])
	sorted_x=sorted_x[::-1]
	print(sorted_x[:5])
	keys=[i[0] for i in sorted_x[:5]]
	di={}
	for i in keys:
		wor=[]
		for j in llist:
			tokenizer = RegexpTokenizer(r'\w+')
			tokens1=tokenizer.tokenize(j)
			tokens1=[w.lower() for w in tokens1]
			if(i in tokens1):
				sst=' '.join(tokens1)
				ss=str(list(parser.raw_parse(sst)))
				wor.append(calc1(ss,tokens1))
		di[i]=wor
	implicit =[]
	explicit=[]
	tokenizer = RegexpTokenizer(r'\w+')
	for key in di:
		for ele in di[key]:
			if(key in tokenizer.tokenize(ele)):
				explicit.append((key,ele))
			else:
				implicit.append((key,ele))
	rdd = sc.parallelize(explicit)
	rdd1 = sc.parallelize(implicit)
	#implicit reviews
	#explicit reviews
	rdd.coalesce(1, shuffle = True).saveAsTextFile("hdfs://localhost:9000/output/"+iiii.split('/')[-1].split(".txt")[0]+"/exp")
	rdd1.coalesce(1, shuffle = True).saveAsTextFile("hdfs://localhost:9000/output/"+iiii.split('/')[-1].split(".txt")[0]+"/imp")
	print("new file new file new file new file new  file")
# #############################################################################