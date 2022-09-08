# -*- coding: utf-8 -*-
"""
Created on Mon Feb 14 14:15:40 2022

@author: hufflaur
"""

from mrjob.job import MRJob
from mrjob.step import MRStep

class Amount (MRJob):
    def steps(self):
        return [
            MRStep(mapper=self.mapper_customer,
                   reducer=self.reducer_average),
            MRStep(mapper=self.mapper_least,
                   reducer=self.reducer_sort)
            ]
    def mapper_customer(self, _, line):
        (CustomerID, ItemID, Amount) = line.split(',')
        yield CustomerID, float(Amount)
        
    def reducer_average(self, CustomerID, Amount):
        total = 0
        numElements = 0
        for x in Amount:
            total += x
            numElements += 1
            average = total / numElements
        yield CustomerID, average
        
        
    def mapper_least(self, CustomerID, average):
        yield '%04.02f'%float(average), CustomerID
        
        
    def reducer_sort(self, average, CustomerID):
        for CustomerID in CustomerID:
            yield average, CustomerID

        
            
if __name__ == '__main__':
    Amount.run()
    
# !python Week5BD.py Data3.csv > Week5BD.txt