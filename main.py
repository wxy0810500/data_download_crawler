from pharmaPendium.taxonomyTree import TaxonomyTree
from pharmaPendium.sourceCfg import Source
from pharmaPendium.safetyData.generateConditions import getAERSQueryConditions
from commonUtils import ghddiMultiProcess
import time


def target(args, conn):
    print(args)
    time.sleep(2)


if __name__ == '__main__':
    pool = ghddiMultiProcess.GHDDIMultiProcessPool(target)

    pool.startAll()

    for i in range(1, 10):
        pool.putTask((i, i+1))





