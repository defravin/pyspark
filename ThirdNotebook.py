# Databricks notebook source
# Download the whole book of Alice in Wonderland available in the project Guthenberg library
url = "http://ia801405.us.archive.org/18/items/alicesadventures19033gut/19033.txt"

# Upload the whole text such as a string
import requests
aliceText = requests.get(url).text

#data distribution
AliceWordsRDD=sc.parallelize([aliceText])

EnglishStopWords = ["and", "to", "the", "into", "on", "of", "by", "or", "in", "a", "with", "that", "she", "it", "i", "you", "he", "we", "they", "her", "his", "its", "this", "that", "at", "as", "for", "not"]


# COMMAND ----------

# Data Cleaning

#a. Split the text in words
AliceWordsRDD= AliceWordsRDD.flatMap(lambda x: x.split(" "))
AliceWordsRDD.take(10)


# COMMAND ----------

import re
AliceWordsRDD = AliceWordsRDD.map(lambda x: re.sub("[^A-Za-z0-9]","",x))
# Anything except 0..9, a..z and A..Z
# replaced with nothing
# in this string

#lowercase conversion
AliceWordsRDD = AliceWordsRDD.map(lambda x: x.lower())

#filter out "blank" word
AliceWordsRDD = AliceWordsRDD.filter(lambda x: len(x)>0 )

#filter out stopwords
AliceWordsRDD = AliceWordsRDD.filter(lambda x: x not in EnglishStopWords)


# COMMAND ----------

#Perform the WordCount algorithm using the MapReduce paradigm.
AliceWordsRDD = AliceWordsRDD.map(lambda x: (x,1))
AliceWordsRDD = AliceWordsRDD.reduceByKey(lambda a,b: a+b)

# COMMAND ----------

#Take the top-10 words written in the romance
AliceWordsRDD.takeOrdered(10,key = lambda x: -x[1])
