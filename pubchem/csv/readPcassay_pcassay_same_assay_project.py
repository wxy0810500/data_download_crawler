import csv
import mysql.connector
import os
from concurrent.futures import ThreadPoolExecutor

BATH_PATH = '/data/file/pubchem/Bioassay/AssayNeighbors'
INSERT_TABLE = '''
INSERT INTO pubchem_assaydesc (a_id,project_group) 
VALUES(%s,%s) on duplicate key update project_group = values(project_group)
'''

def dealCSV(fileName):
    db = mysql.connector.connect(
    host="localhost",
    user="root",
    passwd="123456",
    database="pubchem",
    auth_plugin='mysql_native_password'
    )
    cursor = db.cursor()
    insertMap = {}
    insertList = []
    with open(fileName,'r') as openFile:
        reader = csv.reader(openFile)
        for row in reader:
            rows =  row[0].replace('AID','').split('\t')
            if rows[0] in insertMap:
                insertMap[rows[0]].append(rows[1])
            else:
                insertMap[rows[0]] = [rows[1]]
        for key in insertMap.keys():
            values = ';'.join(insertMap[key])
            insertList.append([key,values])
        #     insertList.append(row[0].split('\t'))
        cursor.executemany(INSERT_TABLE,insertList)
        db.commit()
    # os.remove(os.path.join(BATH_PATH,fileName))
    print(fileName,'success')
    cursor.closeSession()
    db.closeSession()

if __name__ == '__main__':
    # dealCSV('/home/wangjm/software/temp/1158001_1159000/1158009.csv')
    dealCSV(os.path.join(BATH_PATH,'pcassay_pcassay_same_assay_project'))