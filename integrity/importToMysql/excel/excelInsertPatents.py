import xlrd
import mysql.connector
import os
from concurrent.futures import ThreadPoolExecutor

BATH_PATH = ''
DROP_TABLE_PATENTS = 'DROP TABLE IF EXISTS patents'
CREATE_TABLE_PATENTS = '''
CREATE TABLE IF NOT EXISTS patents(id INT NOT NULL AUTO_INCREMENT, company_name VARCHAR(128), website VARCHAR(128), year VARCHAR(128),
annual_sales_(M) VARCHAR(128), PRIMARY KEY(id))
'''
INSERT_TABLE_PATENTS = '''
INSERT INTO patents(company_name, website, year, annual_sales_(M)) 
VALUES (%s,%s,%s,%s)
'''
db = mysql.connector.connect(
    host="localhost",
    user="root",
    passwd="123456",
    database="ghddi",
    auth_plugin='mysql_native_password'
)
cursor = db.cursor()
cursor.execute(DROP_TABLE_PATENTS)
cursor.execute(CREATE_TABLE_PATENTS)
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
    cursor.executemany(INSERT_TABLE_PATENTS,insertList)  
    db.commit()
    cursor.close()
    db.close()

if __name__ == '__main__':
    dirList = os.listdir(BATH_PATH)
    with ThreadPoolExecutor(20) as exector:
        exector.map(dealExcel,dirList)
        
    