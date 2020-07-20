from reaxys.insert_to_mysql.rxInsertUtils import InsertFromXML
from commonUtils import ghddiMysqlConnPool
import os

DROP_TABLE_TARGET = '''
DROP TABLE IF EXISTS TARGET
'''
DROP_TABLE_TOVERW = '''
DROP TABLE IF EXISTS TOVERW
'''
DROP_TABLE_SUBUNIT = '''
DROP TABLE IF EXISTS SUBUNIT
'''
CREATE_TABLE_TARGET = '''
create table if not exists TARGET(TARGET_ID INT, TARGET_CIT DOUBLE, TARGET_SRC VARCHAR(256), TARGET_ASSAY VARCHAR(1024), TARGET_ATYPE VARCHAR(256), TARGET_SITE VARCHAR(256), TARGET_ROLE VARCHAR(256), TARGET_NATURE VARCHAR(256), TARGET_DETAILS VARCHAR(2048), TARGET_KEY VARCHAR(2048), TARGET_SKEY VARCHAR(1024), TARGET_ED VARCHAR(256), TARGET_UPD VARCHAR(256), PRIMARY KEY(TARGET_ID))
'''
CREATE_TABLE_TOVERW = '''
create table if not exists TOVERW(TARGET_ID INT, TOVERW_NCOMP DOUBLE, TOVERW_NBACT DOUBLE, TOVERW_NREF DOUBLE, TOVERW_MAXARN DOUBLE, TOVERW_MAXVAL VARCHAR(256), PRIMARY KEY(TARGET_ID))
'''
CREATE_TABLE_SUBUNIT = '''
create table if not exists SUBUNIT(TARGET_ID INT, SUBUNIT_PROTP VARCHAR(256), SUBUNIT_SYNONYM VARCHAR(2048), SUBUNIT_PROTN VARCHAR(256), SUBUNIT_UNIPROT TEXT, SUBUNIT_PDB TEXT, SUBUNIT_SPECIE VARCHAR(256), SUBUNIT_NATURE VARCHAR(256), SUBUNIT_TRANSFECT VARCHAR(256), SUBUNIT_STOICHIO DOUBLE, SUBUNIT_ACTSITE DOUBLE, SUBUNIT_PMID VARCHAR(256), SUBUNIT_PMPROT VARCHAR(256), SUBUNIT_PMSPECIE VARCHAR(256), SUBUNIT_DETAILS VARCHAR(2048), SUBUNIT_BIND VARCHAR(256), SUBUNIT_ORDER INT, PRIMARY KEY(TARGET_ID))
'''

g_connPool = ghddiMysqlConnPool.GHDDI208MysqlConnPool(10, "reaxys_202001")


def dropAndCreateTable():
    db = g_connPool.getDhddiConn()
    cursor = db.cursor()
    cursor.execute(DROP_TABLE_TARGET)
    cursor.execute(DROP_TABLE_TOVERW)
    cursor.execute(DROP_TABLE_SUBUNIT)
    db.commit()
    cursor.execute(CREATE_TABLE_TARGET)
    cursor.execute(CREATE_TABLE_TOVERW)
    cursor.execute(CREATE_TABLE_SUBUNIT)
    db.commit()
    cursor.close()
    db.close()


INSERT_TARGET = '''
insert into TARGET (TARGET_ID,TARGET_CIT,TARGET_SRC,TARGET_ASSAY,TARGET_ATYPE,TARGET_SITE,TARGET_ROLE,TARGET_NATURE,TARGET_DETAILS,TARGET_KEY,TARGET_SKEY,TARGET_ED,TARGET_UPD) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
'''
INSERT_TOVERW = '''
insert into TOVERW (TARGET_ID,TOVERW_NCOMP,TOVERW_NBACT,TOVERW_NREF,TOVERW_MAXARN,TOVERW_MAXVAL) VALUES(%s,%s,%s,%s,%s,%s)
'''
INSERT_SUBUNIT = '''
insert into SUBUNIT(TARGET_ID, SUBUNIT_PROTP, SUBUNIT_SYNONYM, SUBUNIT_PROTN, SUBUNIT_UNIPROT, SUBUNIT_PDB, SUBUNIT_SPECIE, SUBUNIT_NATURE, SUBUNIT_TRANSFECT, SUBUNIT_STOICHIO, SUBUNIT_ACTSITE, SUBUNIT_PMID, SUBUNIT_PMPROT, SUBUNIT_PMSPECIE, SUBUNIT_DETAILS, SUBUNIT_BIND, SUBUNIT_ORDER) VALUES(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
'''
TARGET_KEY = ['TARGET.CIT', 'TARGET.SRC', 'TARGET.ASSAY', 'TARGET.ATYPE', 'TARGET.SITE', 'TARGET.ROLE', 'TARGET.NATURE',
              'TARGET.DETAILS', 'TARGET.KEY', 'TARGET.SKEY', 'TARGET.ED', 'TARGET.UPD']
TOVERW_KEY = ['TOVERW.NCOMP', 'TOVERW.NBACT', 'TOVERW.NREF', 'TOVERW.MAXARN', 'TOVERW.MAXVAL']
SUBUNIT_KEY = ['SUBUNIT.PROTP', 'SUBUNIT.SYNONYM', 'SUBUNIT.PROTN', 'SUBUNIT.UNIPROT', 'SUBUNIT.PDB', 'SUBUNIT.SPECIE',
               'SUBUNIT.NATURE', 'SUBUNIT.TRANSFECT', 'SUBUNIT.STOICHIO', 'SUBUNIT.ACTSITE', 'SUBUNIT.PMID',
               'SUBUNIT.PMPROT', 'SUBUNIT.PMSPECIE', 'SUBUNIT.DETAILS', 'SUBUNIT.BIND', 'SUBUNIT.ORDER']
TARGET_NAME = 'tgitem'
ID_KEY = 'TARGET.ID'

INSERT_LIST = [INSERT_TARGET, INSERT_TOVERW, INSERT_SUBUNIT]
KEY_LIST = [TARGET_KEY, TOVERW_KEY, SUBUNIT_KEY]
successfulNumber = 0
failedNumber = 0


def doProcess(fileName):
    insertTM = InsertFromXML.InsertTM(TARGET_NAME, ID_KEY, BASE_FILE_PATH)
    docTree = insertTM.getTree(fileName)
    global g_connPool
    conn = g_connPool.getDhddiConn()
    try:
        for i in range(3):
            insertTM.dealXMLByTree(docTree, INSERT_LIST[i], KEY_LIST[i], conn)
    except Exception as e:
        print(e)
        print(fileName, 'failed')
        global failedNumber
        failedNumber += 1
        return
    finally:
        conn.close()
    # os.rename(os.path.join(BASE_FILE_PATH, fileName), os.path.join(HANDLED_FILE_PATH, fileName))
    print(fileName, 'success')
    global successfulNumber
    successfulNumber += 1


if __name__ == '__main__':
    dropAndCreateTable()

    # BASE_FILE_PATH = './reaxys-target-202001'
    # HANDLED_FILE_PATH = './handled'
    BASE_FILE_PATH = '/GHDDI/download/reaxys/target'
    HANDLED_FILE_PATH = './handled'
    if not os.path.exists(HANDLED_FILE_PATH):
        os.makedirs(HANDLED_FILE_PATH)
    # parseXML('data_111_55001.txt')
    listDir = os.listdir(BASE_FILE_PATH)

    for filename in listDir:
        doProcess(filename)
    # with ThreadPoolExecutor(10) as exector:
    #     exector.map(process, listDir)
