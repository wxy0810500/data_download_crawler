from bs4 import BeautifulSoup
import mysql.connector
import os
from concurrent.futures import ThreadPoolExecutor
import mysql.connector.pooling as connectorPooling
import time

gConnPool = ""
gSuccessfulNum = 0
gFailedNum = 0

def ghddiConnection():
    global gConnPool
    if gConnPool == "":
        conf = {
            "host": "172.17.10.208",
            "user": "root",
            "passwd": "123456",
            "database": "integrity",
            "pool_size": 10,
            "pool_name": "ghddiPool",
            "auth_plugin": "mysql_native_password"
        }
        gConnPool = connectorPooling.MySQLConnectionPool(**conf)
    while True:
        try:
            return gConnPool.get_connection()
        except Exception as e:
            print("waiting for conn : " + e.__str__())
            time.sleep(1)

CREATE_TABLE = '''
create table if not exists pk(id INT NOT NULL AUTO_INCREMENT,administered_product VARCHAR(1024),measured_product VARCHAR(128),measured_product_entry_number INT, model VARCHAR(128),
parameter VARCHAR(32), string_value VARCHAR(128),mean_min_value VARCHAR(64), pk_range CHAR(1), s_e_max_value VARCHAR(128), units VARCHAR(16), compartment VARCHAR(64), measured_as VARCHAR(32),
interacting_agent VARCHAR(64), conditions VARCHAR(256), pk_reference VARCHAR(1024), entry_date VARCHAR(45),PRIMARY KEY(id))
'''
INSERT_TABLE = '''
INSERT INTO pk(administered_product, measured_product, measured_product_entry_number, model, parameter, string_value, mean_min_value, 
pk_range, s_e_max_value, units, compartment, measured_as, interacting_agent, conditions, pk_reference, entry_date) 
VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s,%s, %s, %s, %s, %s, %s,%s)
'''
# 不包括entry_date
gtotalFieldsNum=15

def dropAndCreateTable():
    db = ghddiConnection()
    cursor = db.cursor()
    cursor.execute('drop table if exists pk')
    cursor.execute(CREATE_TABLE)
    db.commit()
    cursor.close()
    db.close()

def dealHtml(fileNames):
       
    conn = ghddiConnection()
    cursor = conn.cursor()
    try:
        sourceFile = fileNames[0]
        # print('{} ||| start---'.format(sourceFile))
        with open(sourceFile, 'r') as f:
            strings = ''.join(line for line in f).replace('<br />',';').replace('&#177;','±').replace('&#183;','·')
        soup = BeautifulSoup(strings, features='lxml')
        trList = soup.find_all('table')[0].contents
        insertList = []
        entry_date = time.strftime('%Y-%m-%d', time.localtime(time.time()))
        for row in trList[1:]:
            value = []
            for child in list(row.children)[0:-1]:
                value.append(child.text)
            arguNum = len(value)
            if arguNum != gtotalFieldsNum:
                for i in range(arguNum, gtotalFieldsNum):
                    value.append('')
                if len(value) != gtotalFieldsNum:
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
    # 文件中包含record的数量
    itemNumPerFile = 500
    source_file_parent_dir = '/GHDDI/download/integrity/pk'
    handled_file_parent_dir = '/GHDDI/download/integrity/handled/pk'
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
        exector.map(dealHtml, fileNameList)
    print("end. sucessful number : {} , failed number : {} ".format(gSuccessfulNum, gFailedNum) )
