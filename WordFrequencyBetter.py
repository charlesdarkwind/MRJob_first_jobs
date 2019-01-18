# basic word counter

from mrjob.job import MRJob
import re

WORD_REGEXP = re.compile(r"[\w]+")

class WordFrequencyBetter(MRJob):
    def mapper(self, _, line):
        words = WORD_REGEXP.findall(line)
        for word in words:
            yield word.lower(), 1
            
    def reducer(self, word, occurences):
        yield word, sum(occurences)
        
if __name__ == '__main__':
    WordFrequencyBetter.run()
