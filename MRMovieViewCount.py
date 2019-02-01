from mrjob.job import MRJob
from mrjob.step import MRStep

# Sort by movieID with highest view count
# Needs a second set of map/red
class MRMovieViewCount(MRJob):
    def steps(self):
        return [
        MRStep (mapper= self.mapper_get_views,
                reducer=self.reducer_count_views),
        MRStep (mapper= self.mapper_make_counts_key,
                reducer=self.reducer_output_movieID)
        ]

    def mapper_get_views(self, key, line):
        (userID, movieID, rating, timestamp) = line.split('\t')
        yield movieID, 1
        
    def reducer_count_views(self, movieID, counts):
        yield movieID, sum(counts)
        
    def mapper_make_counts_key(self, movieID, count):
        yield '%04d'%int(count), movieID # Just switch it
        
    def reducer_output_movieID(self, count, movieIDs):
        for movie in movieIDs:
            yield count, movie
        
if __name__ == '__main__':
    MRMovieViewCount.run()
