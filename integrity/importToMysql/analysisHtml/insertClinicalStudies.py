from bs4 import BeautifulSoup
import mysql.connector
import os
from concurrent.futures import ThreadPoolExecutor
import time
import sys

base_path = os.path.dirname(os.path.abspath(__file__)) + '/../../../'  
sys.path.append(base_path)

from commonUtils.ghddiMysqlConnPool import GHDDI208MysqlConnPool


DROP_TABLE = 'DROP TABLE IF EXISTS clinical_studies'
CREATE_TABLE = '''
CREATE TABLE IF NOT EXISTS clinical_studies(id INT NOT NULL AUTO_INCREMENT,study_name VARCHAR(1024), therapeutic_group VARCHAR(128), `condition` VARCHAR(128),
design VARCHAR(128), treatment longtext, pop_no VARCHAR(1024), conclusions_objectives VARCHAR(1024), reference VARCHAR(1024), entry_date VARCHAR(45), PRIMARY KEY(id))
'''
INSERT_TABLE = '''
INSERT INTO clinical_studies(study_name, therapeutic_group,  `condition`, design, treatment, pop_no, conclusions_objectives, reference, entry_date) 
VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s)
'''

g_connPool = GHDDI208MysqlConnPool(10, "integrity")


def dropAndCreateTable():
    db = g_connPool.getDhddiConn()
    cursor = db.cursor()
    cursor.execute(DROP_TABLE)
    cursor.execute(CREATE_TABLE)
    db.commit()
    cursor.close()
    db.close()

gSuccessfulNum = 0
gFailedNum = 0

def doProcess(fileNames):
    global g_connPool
    conn = g_connPool.getDhddiConn()
    cursor = conn.cursor()
    try:
        sourceFile = fileNames[0]
        # print('{} ||| start---'.format(sourceFile))
        with open(sourceFile, 'r') as f:
             strings = ''.join(line for line in f)
        soup = BeautifulSoup(strings, features='lxml')
        trList = soup.find_all('table')[0].contents
        insertList = []
        entry_date = time.strftime('%Y-%m-%d', time.localtime(time.time()))
        for row in trList[1:]:
            value = []
            for child in row.children:
                value.append(child.text)
            arguNum = len(value)
            if arguNum != 8:
                for i in range(arguNum, 8):
                    value.append('')
                if len(value) != 8:
                    print(len(value))
            value.append(entry_date)
            insertList.append(value)
            if len(insertList) == itemNumPerFile:
                cursor.executemany(INSERT_TABLE, insertList)
                conn.commit()
                insertList.clear()
        if len(insertList) > 0:
            cursor.executemany(INSERT_TABLE, insertList)
            conn.commit()
    except Exception as e:
        global gFailedNum
        gFailedNum += 1
        print(sourceFile, 'failed : ', e.__str__())
        return
    finally:
        cursor.close()
        conn.close()

    handledDir = fileNames[1]
    if not os.path.exists(handledDir):
        os.makedirs(handledDir)
    os.rename(sourceFile, os.path.join(handledDir, fileNames[2]))
    print(sourceFile, 'success')
    global gSuccessfulNum
    gSuccessfulNum += 1

if __name__ == '__main__':
    dropAndCreateTable()
    # 文件中包含record的数量
    itemNumPerFile = 500
    source_file_parent_dir = '/GHDDI/download/integrity/cliniacl_study'
    handled_file_parent_dir = '/GHDDI/download/integrity/handled/cliniacl_study'
    if not os.path.exists(handled_file_parent_dir):
        os.makedirs(handled_file_parent_dir)
    dirList = os.listdir(source_file_parent_dir)
    fileNameList = []
    for dirName in dirList:
        fileList = os.listdir(os.path.join(source_file_parent_dir, dirName))
        for fileName in fileList:
            fileNameList.append((os.path.join(source_file_parent_dir, dirName, fileName),
                                 os.path.join(handled_file_parent_dir, dirName),  fileName))
    print("total file number : ", len(fileNameList))
    with ThreadPoolExecutor(10) as exector:
        exector.map(doProcess, fileNameList)
    print("end. sucessful number : {} , failed number : {} ".format(gSuccessfulNum, gFailedNum) )
        
    