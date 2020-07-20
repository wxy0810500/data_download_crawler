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
create table if not exists target(id INT NOT NULL AUTO_INCREMENT,target_name VARCHAR(256),last_updated_date VARCHAR(64),type VARCHAR(32), function_description TEXT,
links VARCHAR(1024), related_names TEXT,conditions VARCHAR(128), status VARCHAR(64), include_number_of_linked_drugs SMALLINT, metaCore VARCHAR(2048),PRIMARY KEY(id))
'''
INSERT_TABLE = '''
INSERT INTO target(target_name,last_updated_date,type,function_description,links,related_names,conditions,status,include_number_of_linked_drugs,metaCore) 
VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s,%s)
'''
# 不包括entry_date
gtotalFieldsNum=10

def dropAndCreateTable():
    db = ghddiConnection()
    cursor = db.cursor()
    cursor.execute('drop table if exists target')
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
             strings = ''.join(line for line in f)
        soup = BeautifulSoup(strings, features='lxml')
        trList = soup.find_all('table')[0].contents
        insertList = []
        for row in trList[1:]:
            value = []
            for child in row.children:
                if '' != str(child.text):
                    value.append(child.text)
                else:
                    value.append(None)
            if len(value) != 10:
                print(fileName,value)
                continue
            if value[4] is not None:
                link = str(value[4]).strip()
                links = link.split('\n')
                if len(links) == 2:
                    pdb = links[0].split(':')[1].strip()
                    newpdb = ','.join([pdb[i:i + 4] for i in range(0,len(pdb), 4)])
                    value[4] = 'PDB:' + newpdb + ';' + links[1] 
                else:
                    value[4] = link
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
    source_file_parent_dir = '/GHDDI/download/integrity/target'
    handled_file_parent_dir = '/GHDDI/download/integrity/handled/target'
    if not os.path.exists(handled_file_parent_dir):
        os.makedirs(handled_file_parent_dir)
    # dirList = os.listdir(source_file_parent_dir)
    # fileNameList = []
    # for dirName in dirList:
    #     fileList = os.listdir(os.path.join(source_file_parent_dir, dirName))
    #     for fileName in fileList:
    #         fileNameList.append((os.path.join(source_file_parent_dir, dirName, fileName),
    #                              os.path.join(handled_file_parent_dir, dirName),  fileName))
    fileList = os.listdir(source_file_parent_dir)
    fileNameList = []
    for fileName in fileList:
        fileNameList.append((os.path.join(source_file_parent_dir, fileName),
                    handled_file_parent_dir,  fileName))
    print("total file number : ", len(fileNameList))
    with ThreadPoolExecutor(10) as exector:
        exector.map(dealHtml, fileNameList)
    print("end. sucessful number : {} , failed number : {} ".format(gSuccessfulNum, gFailedNum) )
