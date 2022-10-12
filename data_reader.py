from typing import  List
from time import time
import logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
  
class DataReader:
    def __init__(self, dir = './data', ext = '.txt') -> None:
        self.dir = dir
        self.ext = ext
        self.logger = logging.getLogger(__name__)

    def read(self, filename) -> List:
        filePath = ''.join([self.dir, '/', filename, self.ext])
        with open(filePath, 'r') as fh:
            lines = fh.readlines()
            return lines
    
    def getExt(self):
        return self.ext
    
    def getDir(self):
        return self.dir
