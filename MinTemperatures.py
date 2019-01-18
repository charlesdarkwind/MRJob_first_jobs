# Find min temperature per station
# data:
# input:    ITE00100554,18000101,TMAX,-75,,,E, ...
# output:   "EZE00100082"   -135 ...

from mrjob.job import MRJob

class MinTemperatures(MRJob):
    def mapper(self, key, line):
        (station, time, category, value) = line.split(',')[:4]
        if category == 'TMIN':
            yield station, int(value)
            
    def reducer(self, station, temps):
        yield station, min(temps)
            
if __name__ == '__main__':
    MinTemperatures.run()
