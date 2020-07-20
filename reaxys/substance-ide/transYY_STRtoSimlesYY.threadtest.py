import mysql.connector as connector
import mysql.connector.pooling as connectorPooling
import pprint
from functools import reduce
from time import sleep
from concurrent.futures import ThreadPoolExecutor
from rdkit import rdBase
from rdkit import Chem
from rdkit.Chem.rdmolfiles import SmilesWriter
import sys
import os

f_log = open('/home/ubuntu/software/github/GHDDI/python/ide/log.txt', 'a')
f_error = open('/home/ubuntu/software/github/GHDDI/python/ide/error.txt', 'a')
sys.stdout = f_log

def printerror(*value):
    print(value,file=f_error)

gConnPool = ""
def ghddiConnection() :
    global gConnPool
    if gConnPool == "":
        conf = {
            "host":"localhost",
            "user":"root",
            "passwd":"123456",
            "database":"ghddi",
            "pool_size":6,
            "pool_name":"ghddiPool",
            "auth_plugin":"mysql_native_password"
        }
        gConnPool = connectorPooling.MySQLConnectionPool(**conf)
    while True:
        try:
            return gConnPool.get_connection()
        except Exception as e:
            sleep(1)

def selectAllNewIDE_XRN() :

    try:
        conn = ghddiConnection()
        cursor = conn.cursor()
        selectSql = "select IDE_XRN from IDE_NEW where IDE_XRN"
        cursor.execute(selectSql)
        rows = cursor.fetchall()
        return list(map(lambda x:x[0], rows))
    except Exception as e:
        printerror(e)
    finally: # must close cursor and conn!!
            cursor.closeSession()
            conn.closeSession()


def doTask(param) :
    newIDEXRNStr = param[0]
    
    print("thread start : ",param[1])

    gdb = ghddiConnection()
    cursor = gdb.cursor()
    selectSql = ("select IDE_XRN,YY_STR from YY where IDE_XRN in (%s) and YY_STR is not null" % newIDEXRNStr)
    cursor.execute(selectSql)
    #if cursor.rowcount > 0 :
    transYY(cursor)
    cursor.closeSession()
    gdb.closeSession()

def transYY(selectYYCursor):
    updateConn = ghddiConnection()
    updateCursor = updateConn.cursor()
    line = selectYYCursor.fetchone()

    while line != None:

        stringValue = str(line[1]).split('HDR')
        try:
            yyValue = '$$$$\n' + stringValue[0].lstrip() + 'HDR\n' + stringValue[2][1:]
        except IndexError as identifier:
            printerror("IndexError", line[0])
            line = selectYYCursor.fetchone()
            continue
        m = Chem.MolFromMolBlock(yyValue)
        if m == None:
            line = selectYYCursor.fetchone()
            continue
        try:
            isoSmiles = Chem.MolToSmiles(m)
            smiles = Chem.MolToSmiles(m,isomericSmiles=False)
        except RuntimeError as error:
            printerror("MolToSimles failed,XRD:%d" % line[0], error)
            line = selectYYCursor.fetchone()
            continue
        updateSql = ("update GHDDI_SList set SMILES_ISO_YY = '%s',SMILES_YY='%s' where IDE_XRN = %s" % (str(isoSmiles),str(smiles),line[0]))
        updateCursor.execute(updateSql)
        updateConn.commit()
        line = selectYYCursor.fetchone()
    
    updateCursor.closeSession()
    updateConn.closeSession()


if __name__ == '__main__':
    
    allNewIDE_XRNList = selectAllNewIDE_XRN()
    paramList = [(reduce(lambda x,y:x + ',' + y, map(str, allNewIDE_XRNList[i:i + 500])), i) for i in range(0, len(allNewIDE_XRNList), 500)]


    #paramList = [[reduce(lambda x,y:x + ',' + y, map(str, allNewIDE_XRNList[i:i + 10])), i] for i in range(0, len(allNewIDE_XRNList), 10)]

    print(len(paramList))
    #task(reduce(lambda x,y:x + ',' + y, map(str, allNewIDE_XRNList)), 1)
    #with ThreadPoolExecutor(2) as exector:
    #        exector.map(doTask, paramList)
    for param in paramList : 
        doTask(param)

    print("finished")


# for line in yyStr:ca
#     stringValue = str(line[1]).split('HDR')
#     yyValue = '$$$$\n' + stringValue[0].lstrip() + 'HDR\n' + stringValue[2][1:]
#     m = Chem.MolFromMolBlock(yyValue)
#     smiles = Chem.MolToSmiles(m)
#     print(str(smiles),line[0])
#     cursor.execute(updateSql,[str(smiles),line[0]])
#     gdb.commit()
#     i += 1
#     print(i)
# with open('yyw.sdf','w') as wYy:
#     for line in yyStr:
#         print(line)
#         wYy.writelines(str(line[0]).lstrip() + '\n')
#         stringValue = str(line[1]).split('HDR')
#         if len(stringValue) == 3:
#             wYy.writelines(stringValue[0].lstrip())
#             wYy.writelines('HDR\n')
#             wYy.write(stringValue[2][1:])
#             wYy.write('$$$$\n')
#         else:
#             print(line[0])

