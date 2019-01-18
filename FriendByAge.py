# data:
# userID, name, age, friendsCount
# Find average number of friends per age group

from mrjob.job import MRJob

class FriendByAge(MRJob):
    def mapper(self, key, line):
        (userID, name, age, friendsCount) = line.split(',')
        yield age, friendsCount
        
    def reducer(self, age, friendsCount):
        i, friendsTotal, count = 0,0,0
        for i in friendsCount:
            count += 1
            friendsTotal += int(i)
        yield age, friendsTotal / count
        
if __name__ == '__main__':
    FriendByAge.run()
