from typing import Any, List
import re
from spec_reader import Spec, SpecReader
from data_reader import DataReader
import json
import logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')

class Builder:
    def __init__(self, specReader:SpecReader, dataReader:DataReader,  dir = './output', outputFmt = '.ndjson') -> None:
        self.dir = dir
        self.dataReader = dataReader
        self.specReader = specReader
        fileExt = dataReader.getExt()
        self.specFileNameFmt = r'^(.+?)_(\d{4})-(0[1-9]|1[0-2]|[1-9])-([1-9]|0[1-9]|[1-2]\d|3[0-1])'+fileExt+'$'
        self.dataFileNameFmt = r'^(.+?)'+fileExt+'$'
        self.outputFmt = outputFmt

    def getFileName(self, filename:str, pattern:Any):
        rule = re.compile(pattern)
        if match := rule.search(filename):
            name = match.group(1)
            return name
        raise Exception(f'unexpected filename {filename}!')

    def getSpecFilename(self, filename:str) -> str:
        return self.getFileName(filename, self.specFileNameFmt)

    def getDataFilename(self, filename:str) -> str:
        return self.getFileName(filename, self.dataFileNameFmt)
    
    def build(self,  filename):
        
        specName = self.getSpecFilename(filename)
        dataFileName = self.getDataFilename(filename)

        specs = self.specReader.read(specName)
       
        lines = self.dataReader.read(dataFileName)

        filePath = ''.join([self.dir, '/', dataFileName, self.outputFmt])
        
        with open(filePath , 'w') as fh:
            for i in range(len(lines)):
                line = lines[i]
                data = self.buildLine(line, specs)
                newLine = '\n' if i != len(lines) - 1 else ''
                fh.write(f'{json.dumps(data)}{newLine}')

    def buildLine(self, line:str, specs:List[Spec]):
        json = {}
        idx = 0
        for spec in specs:
            take = spec.width
            json[spec.name] = spec.format(line[idx:idx+take].strip())
            idx += take
        return json

