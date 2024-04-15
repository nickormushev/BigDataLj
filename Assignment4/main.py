from mrjob.job import MRJob
import xml.etree.ElementTree as ET
import ast
from mrjob.step import MRStep


D = 0
s = 0.85
node_count = 0

class MRPageRank(MRJob):

    def steps(self):
        return [
            MRStep(mapper=self.ip_mapper,
                     reducer=self.ip_reducer),
            MRStep(mapper=self.mapper,
                   reducer=self.reducer)
        ]

    def ip_mapper(self, _, line):
        idx, line = line.split(':', 1)
        key = int(idx)
        global node_count, D
        D = 0
        if key + 1 > node_count:
            node_count = key + 1

        parsed_line = ast.literal_eval(line)
        p_self = parsed_line[0]
        neighbor_count = parsed_line[1]
        neighbors = parsed_line[2]

        if neighbor_count == 0: yield 1, [p_self]

        yield key, (p_self, neighbor_count, neighbors) 
    
    def ip_reducer(self, key, values):
        global D
        for value in values:
            if len(value) == 1:
                D += float(value[0])
                continue
            
            yield key, value


    def mapper(self, key, vals):
        p_self, neighbor_count, neighbors = vals
        for neigh in neighbors:
            yield neigh, p_self/neighbor_count

        yield key, (0.0, p_self, neighbor_count, neighbors)


    def reducer(self, key, values):
        global D, s, node_count
        probSum = 0
        for val in values:
            if not isinstance(val, float):
                p_old = val[1]
                neighbor_count = val[2]
                neighbors = val[3]
            else:
                probSum += val
        p_new = s * probSum + (s * D  + 1.0 - s) / node_count
        yield key, [p_new, neighbor_count, neighbors, p_old]


if __name__ == '__main__':
    MRPageRank.run()
