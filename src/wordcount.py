import matplotlib.pyplot as plt
import re
import sys

from operatore import add

"""
Word count for input file.
"""

infile = sys.argv[1]

print("Number of lines in the file: %s" %  infile.count())

# Get the words.
words = infile.flatMap(lambda line: re.split("\W+", line.lower().strip()))
words = words.filter(lambda x: len(x) > 3) # words more than 3 characters
words = words.map(lambda w: (w,1)) # count 1 per word
words = words.reduceByKey(add) # Sum count all words.

def hist(words):
    """
    Plot a histogram of word frequency.
    """
    count = map(lambda x: x[1], words)
    word = map(lambda x: x[0], words)
    plt.barh(range(len(count)), count, color="gray")
    plt.yticks(range(len(count)), word)

# Change order of tuple (count, word) to (word, count).
