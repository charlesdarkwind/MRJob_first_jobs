# Most visited airport, use a ditributed cache file as a lookup table for airports name and codes


from mrjob.job import MRJob
from mrjob.step import MRStep

class MostPopularAirport(MRJob):
    
    def configure_options(self):
        super(MostPopularAirport, self).configure_options()
        self.add_file_option('--airports', help='Path to airports.csv')
        
    def steps(self):
        return [
        MRStep (mapper= self.mapper_get_flights,
                reducer_init=self.reducer_init,
                reducer=self.reducer_count_flights),
        MRStep (mapper= self.mapper_make_counts_key,
                reducer=self.reducer_output_airport_name),
        ]
        
    def mapper_get_flights(self, _, line):
        fields = line.split(',')
        flight_dest = fields[18]
        cancelled = fields[22]
        # Skip header & if not cancelled
        if flight_dest != 'Origin' and cancelled == '0':
            yield(flight_dest, 1)
        
    def reducer_init(self):
        self.airport_names = {}
        # airport_names = Dict with airport codes as key and names as val
        with open('airports.csv') as f:
            next(f)
            for line in f:
                fields = line.split(',') 
                self.airport_names[fields[0].replace('"', '')] = fields[1].replace('"', '')
                
    def reducer_count_flights(self, key, values):
        yield None, (sum(values), self.airport_names[key])
        
    # This mapper does nothing, its there to avoid bugs with some version of 
    # MRJob related to "non-script steps"
    def mapper_make_counts_key(self, key, value):
        yield key, value
        
    def reducer_output_airport_name(self, key, values):
        yield max(values)
        
        
if __name__ == '__main__':
    MostPopularAirport.run()
