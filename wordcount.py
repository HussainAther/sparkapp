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
worsd = words.reduceByKey(add) # Sum count all words.
