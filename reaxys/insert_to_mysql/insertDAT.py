from commonUtils.ghddiMultiProcess import GHDDIMultiProcessPool
from reaxys.insert_to_mysql.rxInsertUtils import InsertFromXML
import os

INSERT_TABLE_DATIDS = '''
replace into DATIDS (DATIDS_TID,DATIDS_TSUBSYN,DATIDS_TUNIPROT,DATIDS_TPDB) VALUES(%s,%s,%s,%s)
'''
INSERT_TABLE_DAT = '''
replace into DAT(DAT_ID,DAT_citation,DAT_CIT,DAT_CID,DAT_CTYPE,DAT_MRN,DAT_MNAME,DAT_MROUTE,DAT_MREGIM,DAT_MDOSE,DAT_CLPHASE,DAT_SRN,DAT_SNAME,DAT_SDOSE,DAT_MID,DAT_VTYPE,DAT_VLIMIT,DAT_AID,DAT_ANAME,DAT_AFTYPE,DAT_CATEG,DAT_MODEL,DAT_PATHO,DAT_ACTTRG,DAT_ADESC,DAT_EFFECT,DAT_BID,DAT_BCELL,DAT_BPART,DAT_BTISSUE,DAT_BSTATE,DAT_BSPECIE,DAT_TID,DAT_TNAME,DAT_TSUBUNIT,DAT_TSPECIE,DAT_TNATURE,DAT_TDETAILS,DAT_TKEY,DAT_TSKEY,DAT_TROLE,DAT_TTRANSFECT,DAT_ASPECIE,DAT_CTL,DAT_CFLAG,DAT_TEXT,DAT_EXACT,DAT_VALUE,DAT_DEV,DAT_UNIT,DAT_PAUREUS,DAT_PAUORIG,DAT_SIGNIF,DAT_PVALUE,DAT_MCOUNT,DAT_ED,DAT_BIND,DAT_PVD,DAT_UPD,DAT_SRC) 
VALUES(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
'''
TARGET_NAME = 'dpitem'

DAT_ID_KEY = 'DAT.ID'
DAT_KEY_NAME = ['DAT.citation', 'DAT.CIT', 'DAT.CID', 'DAT.CTYPE', 'DAT.MRN', 'DAT.MNAME', 'DAT.MROUTE', 'DAT.MREGIM',
                'DAT.MDOSE', 'DAT.CLPHASE', 'DAT.SRN', 'DAT.SNAME', 'DAT.SDOSE', 'DAT.MID', 'DAT.VTYPE', 'DAT.VLIMIT',
                'DAT.AID', 'DAT.ANAME', 'DAT.AFTYPE', 'DAT.CATEG', 'DAT.MODEL', 'DAT.PATHO', 'DAT.ACTTRG', 'DAT.ADESC',
                'DAT.EFFECT', 'DAT.BID', 'DAT.BCELL', 'DAT.BPART', 'DAT.BTISSUE', 'DAT.BSTATE', 'DAT.BSPECIE',
                'DAT.TID',
                'DAT.TNAME', 'DAT.TSUBUNIT', 'DAT.TSPECIE', 'DAT.TNATURE', 'DAT.TDETAILS', 'DAT.TKEY', 'DAT.TSKEY',
                'DAT.TROLE', 'DAT.TTRANSFECT', 'DAT.ASPECIE', 'DAT.CTL', 'DAT.CFLAG', 'DAT.TEXT', 'DAT.EXACT',
                'DAT.VALUE',
                'DAT.DEV', 'DAT.UNIT', 'DAT.PAUREUS', 'DAT.PAUORIG', 'DAT.SIGNIF', 'DAT.PVALUE', 'DAT.MCOUNT', 'DAT.ED',
                'DAT.BIND', 'DAT.PVD', 'DAT.UPD', 'DAT.SRC']

DATIDS_ID_KEY = 'DATIDS.TID'
DATIDS_KEY_NAME = ['DATIDS.TSUBSYN', 'DATIDS.TUNIPROT', 'DATIDS.TPDB']


def doTask(args: tuple, conn):
    basic_path = args[0]
    fileName = args[1]
    try:
        # DAT table
        DATInsertTM = InsertFromXML(TARGET_NAME, DAT_ID_KEY, basic_path, True)
        DATInsertTM.insertFromXMLTree(fileName, INSERT_TABLE_DAT, DAT_KEY_NAME, conn)

        # # DATIDS table
        # DATIDSInsertTM = InsertFromXML(TARGET_NAME, DATIDS_ID_KEY, basic_path, True)
        # DATIDSInsertTM.insertFromXMLTree(fileName, INSERT_TABLE_DATIDS, DATIDS_KEY_NAME, conn)
    except Exception as e:
        print(e)
    # os.remove(os.path.join(BATH_PATH, fileName))
    print(fileName, 'success')


def process(rawFilePath):
    processPool = GHDDIMultiProcessPool(doTask, 'reaxys')

    processPool.startAll()

    map(processPool.putTask, [(rawFilePath, fileName) for fileName in os.listdir(rawFilePath)])


if __name__ == '__main__':
    rawFilePath = ('D:\code\download-files\\reaxys\dat\\20-07-15')
    processPool = GHDDIMultiProcessPool(doTask, 'reaxys')

    processPool.startAll()

    # map(processPool.putTask, [(rawFilePath, fileName) for fileName in os.listdir(rawFilePath)])
    for arg in [(rawFilePath, fileName) for fileName in os.listdir(rawFilePath)]:
        processPool.putTask(arg)
    print('finished')
