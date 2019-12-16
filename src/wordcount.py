import matplotlib.pyplot as plt
import numpy as np
import re
import sys

from operator import add
from pyspark import SparkContext

"""
Word count for input file.
"""

sc = SparkContext("local", "word count")

infile = sc.textFile(sys.argv[1])
print("Number of lines in the file: %s" %  infile.count())

# Get the words.
words = infile.flatMap(lambda line: re.split("\W+", line.lower().strip()))
words = words.filter(lambda x: len(x) > 3) # words more than 3 characters
words = words.map(lambda w: (w,1)) # count 1 per word
words = words.reduceByKey(add) # Sum count all words.

# Change order of tuple (count, word) to (word, count).
words = words.map(lambda x:(x[1], x[0]))
w = words.take(25)

"""
Plot a histogram of word frequency.
"""
count = list(map(lambda x: x[1], w))
word = list(map(lambda x: x[0], w))
x = range(len(count))
plt.barh(x, count, color="gray")
plt.yticks(x, word)
plt.savefig("output/mobydick.png")
