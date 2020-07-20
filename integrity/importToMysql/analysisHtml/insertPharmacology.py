from bs4 import BeautifulSoup
import mysql.connector
import mysql.connector.pooling as connectorPooling
import os
import time

from concurrent.futures import ThreadPoolExecutor

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
create table if not exists experimental_pharmacology(id INT NOT NULL AUTO_INCREMENT,entry_number INT,drug_name VARCHAR(128),mechanism_of_action VARCHAR(1024),experimental_activity VARCHAR(512),experimental_pharmacology_system VARCHAR(128),activity_effect VARCHAR(128),
target_condition_toxicity VARCHAR(256),pharmacological_activity VARCHAR(256),exp_model_name VARCHAR(256),material VARCHAR(128),method VARCHAR(128),parameter VARCHAR(128),operator VARCHAR(128),mean VARCHAR(128),
var VARCHAR(128),min_value VARCHAR(128),max_value VARCHAR(128),string_value VARCHAR(128),unit VARCHAR(128),soucrce VARCHAR(2048),smiles TEXT, PRIMARY KEY(id))
'''
INSERT_TABLE = '''
INSERT INTO experimental_pharmacology(entry_number,drug_name ,mechanism_of_action ,experimental_activity ,experimental_pharmacology_system ,activity_effect ,target_condition_toxicity,
pharmacological_activity ,exp_model_name ,material ,method ,parameter ,operator ,mean ,var ,min_value ,max_value ,string_value ,unit, soucrce ,smiles, entry_date) 
VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s,%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
'''


def dropAndCreateTable():
    db = mysql.connector.connect(
        host="172.17.10.208",
        user="root",
        passwd="123456",
        database="integrity",
        auth_plugin='mysql_native_password'
    )
    cursor = db.cursor()
    cursor.execute('drop table if exists experimental_pharmacology')
    cursor.execute(CREATE_TABLE)
    db.commit()
    cursor.closeSession()
    db.closeSession()


def dealHtml(fileNames):
       
    conn = ghddiConnection()
    cursor = conn.cursor()
    try:
        sourceFile = fileNames[0]

        # print('{} ||| start---'.format(sourceFile))
        with open(sourceFile, 'r') as f:
            strings = ''.join(line for line in f).replace('<br>', ';')
        soup = BeautifulSoup(strings, features='lxml')
        trList = soup.find_all('table')[0].contents
        insertList = []
        entry_date = time.strftime('%Y-%m-%d', time.localtime(time.time()))
        for row in trList[1:]:
            value = []
            for child in list(row.children)[0:-1]:
                value.append(child.text)
            arguNum = len(value)
            if arguNum != 21:
                for i in range(arguNum, 21):
                    value.append('')
                if len(value) != 21:
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
    source_file_parent_dir = '/GHDDI/download/integrity/experimental-pharmacology/condtion-infections'
    handled_file_parent_dir = '/GHDDI/download/integrity/handled/experimental-pharmacology/condtion-infections'
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
    with ThreadPoolExecutor(5) as exector:
        exector.map(dealHtml, fileNameList)
    print("end. sucessful number : {} , failed number : {} ".format(gSuccessfulNum, gFailedNum) )
    # print(fileNameList)
    # for files in fileNameList:
    #     dealExcel(files)
