import csv
import mysql.connector
import os
from concurrent.futures import ThreadPoolExecutor

BATH_PATH = '/data/file/pubchem/Bioassay/Extras'
INSERT_TABLE = '''
INSERT INTO pubchem_assaydesc (a_id,data_resource) 
VALUES(%s,%s) on duplicate key update data_resource = values(data_resource)
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
    insertList = []
    with open(fileName,'r') as openFile:
        reader = csv.reader(openFile)
        next(reader)
        for row in reader:
            insertList.append(row[0].split('\t'))
        cursor.executemany(INSERT_TABLE,insertList)
        db.commit()
    # os.remove(os.path.join(BATH_PATH,fileName))
    print(fileName,'success')
    cursor.closeSession()
    db.closeSession()

if __name__ == '__main__':
    # dealCSV('/home/wangjm/software/temp/1158001_1159000/1158009.csv')
    dealCSV(os.path.join(BATH_PATH,'Aid2DepositorName'))