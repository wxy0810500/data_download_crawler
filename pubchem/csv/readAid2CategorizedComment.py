import csv
import mysql.connector
import os
from concurrent.futures import ThreadPoolExecutor

BATH_PATH = '/data/file/pubchem/Bioassay/Extras'
INSERT_TABLE = '''
INSERT INTO pubchem_assaydesc (a_id,cell_type,stage) 
VALUES(%s,%s,%s) on duplicate key update cell_type = values(cell_type), stage = values(stage)
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
            insertValue = []
            for value in row[0].split('\t'):
                if value != '':
                    insertValue.append(value)
                else:
                    insertValue.append(None)
            insertList.append(insertValue)
        cursor.executemany(INSERT_TABLE,insertList)
        db.commit()
    # os.remove(os.path.join(BATH_PATH,fileName))
    print(fileName,'success')
    cursor.closeSession()
    db.closeSession()

if __name__ == '__main__':
    # dealCSV('/home/wangjm/software/temp/1158001_1159000/1158009.csv')
    dealCSV(os.path.join(BATH_PATH,'Aid2CategorizedComment'))