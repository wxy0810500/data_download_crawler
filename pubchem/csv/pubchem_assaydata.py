import csv
import mysql.connector
import os
from concurrent.futures import ThreadPoolExecutor

BATH_PATH = '/data/file/pubchem/Bioassay/data-csv-import'
CREATE_TABLE = '''
CREATE TABLE IF NOT EXISTS pubchem_assaydata(id INT NOT NULL AUTO_INCREMENT, aid INT, sid INT, cid INT, label VARCHAR(128), score VARCHAR(128), PRIMARY KEY(id))
'''
INSERT_TABLE = '''
INSERT INTO pubchem_assaydata(aid,sid,cid,label,score) 
VALUES(%s,%s,%s,%s,%s)
'''
db = mysql.connector.connect(
host="localhost",
user="root",
passwd="123456",
database="pubchem",
auth_plugin='mysql_native_password'
)
cursor = db.cursor()
#cursor.execute('DROP TABLE IF EXISTS pubchem_assaydata') 
cursor.execute(CREATE_TABLE)
db.commit()
cursor.closeSession()
db.closeSession()

def dealCSV(fileName):
    db = mysql.connector.connect(
    host="localhost",
    user="root",
    passwd="123456",
    database="pubchem",
    auth_plugin='mysql_native_password'
    )
    cursor = db.cursor()
    insertList = []
    with open(os.path.join(BATH_PATH,fileName),'r') as openFile:
        aid = str(fileName).split('/')[-1].split('.')[0]
        reader = csv.reader(openFile)
        for row in reader:
            value = [aid]
            if str(row[0]).isdecimal():
                for r in row[1:5]:
                    if r is not None and r == '':
                        r = None
                    value.append(r)
                insertList.append(value)
        cursor.executemany(INSERT_TABLE,insertList)
        db.commit()
    os.remove(os.path.join(BATH_PATH,fileName))
    print(fileName,'success')
    cursor.closeSession()
    db.closeSession()

if __name__ == '__main__':
    # dealCSV('/home/wangjm/software/temp/1158001_1159000/1158009.csv')
    fileList = []
    dirList = os.listdir(BATH_PATH)
    for dirName in dirList:
        fileNameList = os.listdir(os.path.join(BATH_PATH,dirName))
        for fileName in fileNameList:
            fileList.append(os.path.join(BATH_PATH,dirName,fileName))
    print(len(fileList))
    #for filen in fileList:
    #    dealCSV(filen)
    #with ThreadPoolExecutor(20) as executor:
      #  executor.map(dealCSV,fileList)