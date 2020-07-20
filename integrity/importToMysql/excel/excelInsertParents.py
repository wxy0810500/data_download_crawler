import xlrd
import mysql.connector
import os
from concurrent.futures import ThreadPoolExecutor

BATH_PATH = ''
DROP_TABLE_PATENTS = 'DROP TABLE IF EXISTS genomics'
CREATE_TABLE_GENOMICS = '''
CREATE TABLE IF NOT EXISTS genomics(id INT NOT NULL AUTO_INCREMENT,gene_name VARCHAR(128), organism VARCHAR(128), condition VARCHAR(128),
study_type VARCHAR(128), model VARCHAR(128), summary VARCHAR(128), PRIMARY KEY(id))
'''
INSERT_TABLE_GENOMICS = '''
INSERT INTO genomics(gene_name, organism, condition, study_type, model, summary) 
VALUES (%s,%s,%s,%s,%s,%s,%s,%s)
'''
db = mysql.connector.connect(
    host="localhost",
    user="root",
    passwd="123456",
    database="ghddi",
    auth_plugin='mysql_native_password'
)
cursor = db.cursor()
cursor.execute(DROP_TABLE_GENOMICS)
cursor.execute(CREATE_TABLE_GENOMICS)
db.commit()
cursor.close()
db.close()

def dealExcel(fileName):
    workbook = xlrd.open_workbook(fileName)
    sheet = workbook.sheets()[0]
    insertList = []
    for row in range(1,sheet.nrows):
        valueList = []
        for col in range(sheet.ncols):
            value = sheet.row_values(row)[col]
            valueList.append(value)
            insertList.append(valueList)
    db = mysql.connector.connect(
    host="localhost",
    user="root",
    passwd="123456",
    database="ghddi",
    auth_plugin='mysql_native_password'
    )
    cursor = db.cursor()
    cursor.executemany(INSERT_TABLE_GENOMICS,insertList)  
    db.commit()
    cursor.close()
    db.close()

if __name__ == '__main__':
    dirList = os.listdir(BATH_PATH)
    with ThreadPoolExecutor(20) as exector:
        exector.map(dealExcel,dirList)
        
    