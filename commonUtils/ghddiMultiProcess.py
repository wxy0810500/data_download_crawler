from multiprocessing import Process, JoinableQueue as Queue
import os
from .ghddiMysqlConnPool import getCHGGI208MysqlSingleConnection


class GHDDIProcess(Process):

    def __init__(self, target, database=None, inputQueue=None, outputQueue=None):
        args = (target, inputQueue, outputQueue)
        Process.__init__(self, target=self._processWorker, args=args)
        if database is not None:
            self._mysqlConn = getCHGGI208MysqlSingleConnection(database)
        else:
            self._mysqlConn = None

    def _processWorker(self, target, inputQueue: Queue, outputQueue: Queue):
        while True:
            inputArg = inputQueue.get()
            if inputArg is None:
                continue
            print(f'process is working : {inputArg}')
            ret = target(inputArg, self._mysqlConn)
            outputQueue.put(ret)

    def __del__(self):
        print('process del')
        if self._mysqlConn is not None:
            self._mysqlConn.close()


class GHDDIMultiProcessPool:

    def __init__(self, target, database=None):
        self._inputQueue = Queue()
        self._outputQueue = Queue()
        jobs = []
        for i in range(0, os.cpu_count()):
            jobs.append(GHDDIProcess(target, database, self._inputQueue, self._outputQueue))
        self._jobs = jobs

    def __del__(self):
        print('processPool del')
        self._inputQueue.join()
        self._outputQueue.join()

        self._inputQueue.close()
        self._outputQueue.close()
        for p in self._jobs:
            p.terminate()
            p.close()

    def startAll(self):
        for p in self._jobs:
            p.start()

    def finishAll(self):
        pass

    def putTask(self, taskArgs, block=True, timeout=None):
        self._inputQueue.put(taskArgs, block=block, timeout=timeout)

    def getTaskRet(self, block=True, timeout=None):
        return self._outputQueue.get(block=block, timeout=timeout)
