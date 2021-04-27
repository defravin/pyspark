# Databricks notebook source
animal = ['cat','elephant','rat','rat','cat']
wordsRDD = sc.parallelize(animal)
wordsRDD.collect()

# COMMAND ----------

# Compute the number of words’ occurrences in wordsRDD
wordsPairRDD = wordsRDD.map(lambda x: (x,1))
frequenciesOfWordsRDD = wordsPairRDD.reduceByKey(lambda a,b: a+b)
frequenciesOfWordsRDD.collect()

# COMMAND ----------

#Starting from frequenciesOfWordsRDD, count how many words in wordsRDD are different from cat
filteredRDD = frequenciesOfWordsRDD.filter(lambda x: x[0] != 'cat')
filteredRDD.reduce(lambda a,b : ('total', a[1]+b[1]))
