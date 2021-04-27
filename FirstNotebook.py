# Databricks notebook source
sc

# COMMAND ----------

# Create a RDD wordsRDD with these words: cat, elephant, rat, rat and cat and visualize its values
animal = ['cat','elephant','rat','rat','cat']
wordsRDD = sc.parallelize(animal)
wordsRDD.collect()

# COMMAND ----------

# Transform in plural the words in wordsRDD adding a 's' to the end of the word (concatenation)
pluralWordsRDD = wordsRDD.map(lambda x: x+'s')
pluralWordsRDD.collect()

# COMMAND ----------

# Create the RDD lengthWordsRDD with the length of the plural words
lenghtWordsRDD = pluralWordsRDD.map(lambda x: len(x))
lenghtWordsRDD.collect()

# COMMAND ----------

# Compute the sum of the lengths of the words
sumOfLenghts=lenghtWordsRDD.sum()

# COMMAND ----------

# Combine the result with the RDD's size to compute the average words' length
averageLenght = sumOfLenghts / lenghtWordsRDD.count()

print("The avarage lenght of plural words is: ", int(averageLenght))
