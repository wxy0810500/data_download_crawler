from reaxys.insert_to_mysql.rxInsertUtils import InsertFromXML
from commonUtils.ghddiMysqlConnPool import getCHGGI208MysqlSingleConnection
import os

DROP_TABLE = '''
DROP TABLE IF EXISTS YY_SLB;
'''

CREATE_TABLE = '''CREATE TABLE `reaxys`.`YY_SLB` (
  `IDE_XRN` int NOT NULL,
  `YY_STR_V2000` mediumtext,
  `YY_STR_V3000` mediumtext,
  `YY_SIMSORT` varchar(512) DEFAULT NULL,
  `YY_MARKUSH` varchar(512) DEFAULT NULL,
  PRIMARY KEY (`IDE_XRN`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8;
'''

DATABASE = 'reaxys'


def dropAndCreateTable():
    db = getCHGGI208MysqlSingleConnection(DATABASE)
    cursor = db.cursor()
    cursor.execute(DROP_TABLE)
    db.commit()
    cursor.execute(CREATE_TABLE)
    db.commit()
    cursor.close()
    db.close()


INSERT_SQL_FORMAT = '''INSERT INTO `reaxys`.`YY_SLB`
(`IDE_XRN`,
`YY_STR_V2000`,
`YY_STR_V3000`,
`YY_SIMSORT`,
`YY_MARKUSH`)
VALUES
(%s, %s, %s, %s, %s)
'''
XML_KEYS = ['YY.STR', 'YY.SIMSORT', 'YY.MARKUSH']
TARGET_NAME = 'substances'
ID_KEY = 'TARGET.ID'

successfulNumber = 0
failedNumber = 0


def doProcess(fileName, conn):

    insertTM = InsertFromXML(TARGET_NAME, ID_KEY, BASE_FILE_PATH)
    docTree = insertTM.getTree(fileName)
    try:
        insertTM.insertFromXMLTree(docTree, INSERT_SQL_FORMAT, XML_KEYS, conn)
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
