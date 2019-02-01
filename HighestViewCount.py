from mrjob.job import MRJob
from mrjob.step import MRStep

# Ouput only the movie with highest view count
class MRMovieViewCount(MRJob):
    def steps(self):
        return [
        MRStep (mapper= self.mapper_get_views,
                reducer=self.reducer_count_views),
        MRStep (mapper= self.mapper_make_counts_key,
                reducer=self.reducer_output_movieID),
        ]

    def mapper_get_views(self, _, line):
        (userID, movieID, rating, timestamp) = line.split('\t')
        yield movieID, 1
        
    def reducer_count_views(self, key, values):
        yield None, (sum(values), key)
        
    # This mapper does nothing, its there to avoid bugs with some version of 
    # MRJob related to "non-script steps"
    def mapper_make_counts_key(self, key, value):
        yield key, value
        
    def reducer_output_movieID(self, key, values):
        yield max(values)
        
if __name__ == '__main__':
    MRMovieViewCount.run()
