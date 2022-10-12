
from functools import lru_cache
from constant import COL_NAME, COL_WIDTH, COL_DATATYPE, TYPE_TEXT, TYPE_BOOLEAN, TYPE_INTEGER
from typing import List
import logging
import csv

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')

class Spec:
    def __init__(self, name:str, width:int, dataType:str) -> None:
        self.allowedDType = set([TYPE_TEXT, TYPE_BOOLEAN, TYPE_INTEGER])

        if not name:
            raise Exception('unexpected name on spec!')
        if width <= 0:
            raise Exception('unexpected width on spec!')
        if dataType.upper() not in self.allowedDType:
            raise Exception('unexpected dataType on spec!')

        self.name = name
        self.width = width
        self.dataType = dataType.upper()

    def format(self, input:str):
        if self.dataType == TYPE_TEXT:
            return input
        if self.dataType == TYPE_BOOLEAN:
            return True if input == '1' else False
        if self.dataType == TYPE_INTEGER:
            return int(input)
        raise Exception('unexpected format!')

class SpecFormatter:
    def __init__(self, spec:Spec) -> None:
        self.spec = spec

    def format(self, input:str):
        if self.spec.dataType == TYPE_TEXT:
            return input
        if self.spec.dataType == TYPE_BOOLEAN:
            return True if input == '1' else False
        if self.spec.dataType == TYPE_INTEGER:
            return int(input)
        raise Exception('unexpected format!') 

class SpecReader:
    def __init__(self, dir = './spec', ext = '.csv') -> None:
        self.dir = dir
        self.ext = ext
        self.logger = logging.getLogger(__name__)
        self.mapping = set([
            COL_NAME,
            COL_WIDTH,
            COL_DATATYPE
        ])   

    @lru_cache(10)
    def read(self, filename:str) -> List[Spec]:
        filePath = ''.join([self.dir, '/', filename, self.ext])
        with open(filePath, newline='') as csvfile:
            spamreader = csv.reader(csvfile, delimiter=',', quotechar='|')
            rowIdx = 0
            header = {}
            specs = []
            for row in spamreader:
                if len(row) < len(self.mapping):
                    raise Exception('invalid row format!')

                if rowIdx == 0:
                    for colIdx in range(len(row)):
                        colName = row[colIdx].strip().replace('"','')
                        # loose control on number of col, instead valid the required col exist or not
                        if colName in self.mapping:
                            header[colName] = colIdx

                    if len(self.mapping.intersection(header)) != len(self.mapping):
                        raise Exception('invalid header column!')
                else: 
                    # col data relied on header index reference
                    specs.append(Spec(row[header[COL_NAME]], int(row[header[COL_WIDTH]]), row[header[COL_DATATYPE]]))
                rowIdx += 1
        return specs
    
    def getExt(self):
        return self.ext
    
    def getDir(self):
        return self.dir