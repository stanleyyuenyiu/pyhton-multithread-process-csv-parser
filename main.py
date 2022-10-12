
import threading
import os
from queue import Queue
from spec_reader import SpecReader
from data_reader import DataReader
from builder import Builder
from datetime import datetime, timedelta
from time import sleep, time
import sys
import traceback
import logging
import random

from concurrent.futures import ProcessPoolExecutor, ThreadPoolExecutor, as_completed

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')

def generateTestFile(counter:int):
    current_date = datetime.now()
    for i in range(1, counter+1):
        end_date = current_date + timedelta(days=i)
        end_date_formatted = end_date.strftime('%Y-%m-%d')
        date = end_date_formatted
        with open(f'./data/fileformat1_{date}.txt' , 'w') as fh:
            for i in range(1, counter+1):
                num = random.randint(1, 999)
                boolean = random.randint(0,1)
                fh.write(f'Stroke    {str(boolean)}{str(num)}\n')
      
class Main:    
    def __init__(self) -> None:
        self.specReader = SpecReader()
        self.dataReader = DataReader()
        self.lock = threading.Lock()
        self.builder = Builder(self.specReader, self.dataReader)
        self.logger = logging.getLogger(__name__)
        self.q = Queue()
        self.executor = ProcessPoolExecutor(max_workers=4)

    def taskProcesser(self, file):
        with self.lock:
            self.logger.info('Queueing {}'.format(file))
            self.builder.build(file)
    
    def threadProcesser(self):
        # With queue, it become concurrent into 1 thread
        while True:
            file = self.q.get()
            try:
                self.taskProcesser(file)
            except Exception as e:
                type, msg, traceBack = sys.exc_info()
                tracebacks = traceback.extract_tb(traceBack)
                stack = []
                for trace in tracebacks:
                    stack.append("File : %s , Line : %d, Func.Name : %s, Message : %s" % (trace[0], trace[1], trace[2], trace[3]))

                self.logger.info('Fail to process {}\n Exception : {}\n Stack trace : {}'.format(
                    file,
                    msg,
                    ''.join(stack)
                    )
                )
            finally:
                # Not to block the queue even exception
                self.q.task_done()
    
    def initWorker(self, workersCount = 4):
        # Multi worker depends on your cpu core
        # Using thread due to IO heavy task
        for _ in range(workersCount):
            t = threading.Thread(target = self.threadProcesser)
            t.daemon=True
            t.start()
    
    def start(self):
        ts = time()
        self.initWorker()
      
        for file in os.listdir(self.dataReader.getDir()):
            if file.endswith(self.dataReader.getExt()):
                self.q.put(file)
        
        self.q.join()
        logging.info('Took %s', time() - ts)
    
class Main2:
  builder = None
  specReader = None
  dataReader = None

  def __init__(self) -> None:
        self.logger = logging.getLogger(__name__)
  
  @staticmethod 
  def getBuilder():
    if not Main2.builder:
        Main2.specReader = SpecReader()
        Main2.dataReader = DataReader()
        Main2.builder = Builder(Main2.specReader, Main2.dataReader)
    return Main2.builder

  @staticmethod
  def threadExecuter(file:str, lock):
      with lock:
            #logging.info('Processing {}'.format(file))
            Main2.getBuilder().build(file)

  @staticmethod
  def processExcutor(batch, threadSize = 8):
    lock = threading.Lock()
    with ThreadPoolExecutor(threadSize) as exe:
        for file in batch:
            exe.submit(Main2.threadExecuter, file, lock)

  def start(self, processerCount = 8):
    ts = time()
    Main2.getBuilder()
    files = []
    for file in os.listdir(Main2.dataReader.getDir()):
        if file.endswith(Main2.dataReader.getExt()):
            files.append(file)

    chunksize = round(len(files) / processerCount)
    with ProcessPoolExecutor(processerCount) as process:
        for i in range(0, len(files), chunksize):
            batch = files[i:(i + chunksize)]
            process.submit(Main2.processExcutor, batch)

    logging.info('Took %s', time() - ts)

if __name__ == '__main__':
    #generateTestFile(5000)
    main = Main2()
    main.start()