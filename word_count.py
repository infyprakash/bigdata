from IPython.core.splitinput import LineInfo
from pyspark.sql import SparkSession
spark = SparkSession.builder\
        .master("local")\
        .appName("Colab")\
        .config('spark.ui.port', '4050')\
        .getOrCreate()

spark.conf.set("spark.sql.repl.eagerEval.enabled", True)
sc = spark.sparkContext
words = sc.textFile("wordcount.txt")
words = words.flatMap(lambda line: line.split(" "))
word_map = words.map(lambda word:(word,1))
word_count = word_map.reduceByKey(lambda a,b:a+b)


word_count.saveAsTextFile("output")
    
