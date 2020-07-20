import csv
import mysql.connector
import os
from concurrent.futures import ThreadPoolExecutor

BATH_PATH = '/data/file/pubchem/Bioassay/Extras'
INSERT_TABLE = '''
INSERT INTO pubchem_assaydesc (a_id,format,type,detection_method) 
VALUES(%s,%s,%s,%s) on duplicate key update format = values(format), type = values(type), detection_method = values(detection_method)
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
        next(reader)
        for row in reader:
            rows =  row[0].split('\t')
            key = str(rows[1]).strip()
            if rows[0] in insertMap:
                tempList = insertMap[rows[0]]
                if key == 'Assay Format':
                    if tempList[0] is not None:
                        tempList[0] += ';' + rows[2]
                    else:
                        tempList[0] = rows[2]
                elif key == 'Assay Type':
                    if tempList[1] is not None:
                        tempList[1] += ';' + rows[2]
                    else:
                        tempList[1] = rows[2]
                elif key == 'Assay Detection Method':
                    if tempList[2] is not None:
                        tempList[2] += ';' + rows[2]
                    else:
                        tempList[2] = rows[2]
                else:
                    print('wrong')
                insertMap[rows[0]] = tempList
            else:
                tempList = [None,None,None]
                if key == 'Assay Format':
                    tempList[0] = rows[2]
                elif key == 'Assay Type':
                    tempList[1] = rows[2]
                elif key == 'Assay Detection Method':
                    tempList[2] = rows[2]
                else:
                    print('wrong')
                insertMap[rows[0]] = tempList
        for key in insertMap.keys():
            tempList = [key]
            tempList.extend(insertMap[key])
            insertList.append(tempList)
        cursor.executemany(INSERT_TABLE,insertList)
        db.commit()
    # os.remove(os.path.join(BATH_PATH,fileName))
    print(fileName,'success')
    cursor.closeSession()
    db.closeSession()

if __name__ == '__main__':
    # dealCSV('/home/wangjm/software/temp/1158001_1159000/1158009.csv')
    dealCSV(os.path.join(BATH_PATH,'Aid2Annotation'))