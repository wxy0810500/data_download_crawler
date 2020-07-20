import mysql.connector
from xml.dom.minidom import parse
import xml.dom.minidom
import os

BASE_PATH = ''
TARGET_NAME = ''
ID_KEY = ''
INSERT_SQL = ''
KEY_LIST = []

def setValue(basePath,targetName,idKey,insertSql,keyList):
    global BASE_PATH
    BASE_PATH = basePath
    global TARGET_NAME
    TARGET_NAME = targetName
    global ID_KEY
    ID_KEY = idKey
    global INSERT_SQL
    INSERT_SQL = insertSql
    global KEY_LIST
    KEY_LIST = keyList

def setBasicValue(basePath,targetName):
    global BASE_PATH
    BASE_PATH = basePath
    global TARGET_NAME
    TARGET_NAME = targetName

def setSqlValue(idKey,insertSql,keyList):
    global ID_KEY
    ID_KEY = idKey
    global INSERT_SQL
    INSERT_SQL = insertSql
    global KEY_LIST
    KEY_LIST = keyList

def printG():
    print(BASE_PATH,TARGET_NAME,ID_KEY,INSERT_SQL,KEY_LIST)

def dealXML(fileName):
    tdb = mysql.connector.connect(
    host="localhost",
    user="root",
    passwd="123456",
    database="ghddi",
    auth_plugin='mysql_native_password'
    )
    tcursor = tdb.cursor()
    with open(os.path.join('%s/%s' % (BASE_PATH, fileName)),'r') as file:
        fileLine = ''.join(line for line in file)
        if fileLine.count('<hi>') != fileLine.count('</hi>'):
            print(fileName)
            return
        fileLine = fileLine.replace('<hi>','').replace('</hi>','')
        domTree = xml.dom.minidom.parseString(fileLine)
        collection = domTree.documentElement
        nodeList = collection.getElementsByTagName(TARGET_NAME)
        insert = []
        for node in nodeList:
            if len(node.getElementsByTagName(ID_KEY)) == 0:
                continue
            tId = node.getElementsByTagName(ID_KEY)[0].childNodes[0].nodeValue
            if tId == None:
                continue
            res = deal(tId,node)
            insertJude = False
            for resValue in res[1:]:
                if resValue != None:
                    insertJude = True
                    break
            if insertJude:
                insert.append(res)
    tcursor.executemany(INSERT_SQL,insert)
    tdb.commit()
    os.remove(os.path.join('%s/%s' % (BASE_PATH, fileName)))
    print(fileName,'success')
    tcursor.closeSession()
    tdb.closeSession()

def getTree(fileName):
    with open(os.path.join('%s/%s' % (BASE_PATH, fileName)),'r') as file:
        fileLine = ''.join(line for line in file)
        if fileLine.count('<hi>') != fileLine.count('</hi>'):
            print(fileName)
            return
        fileLine = fileLine.replace('<hi>','').replace('</hi>','')
        domTree = xml.dom.minidom.parseString(fileLine)
        return domTree


def dealXMLByTree(domTree):
    tdb = mysql.connector.connect(
    host="localhost",
    user="root",
    passwd="123456",
    database="ghddi",
    auth_plugin='mysql_native_password'
    )
    tcursor = tdb.cursor()
    collection = domTree.documentElement
    nodeList = collection.getElementsByTagName(TARGET_NAME)
    insert = []
    for node in nodeList:
        if len(node.getElementsByTagName(ID_KEY)) == 0:
            continue
        tId = node.getElementsByTagName(ID_KEY)[0].childNodes[0].nodeValue
        if tId == None:
            continue
        res = deal(tId,node)
        insertJude = False
        for resValue in res[1:]:
            if resValue != None:
                insertJude = True
                break
        if insertJude:
            insert.append(res)
    tcursor.executemany(INSERT_SQL,insert)
    tdb.commit()
    tcursor.closeSession()
    tdb.closeSession()

def deal(tId,node):
    dList = [tId]
    for key in KEY_LIST:
        dList.append(arrange(node.getElementsByTagName(key)))
    return dList

def arrange(nodeList):
    if nodeList == None or len(nodeList) == 0:
        return None
    strValue = ''
    for node in nodeList:
        if node.childNodes[0].nodeValue != None:
            strValue += node.childNodes[0].nodeValue + ';'
    if len(strValue) > 0:
        strValue = strValue[:-1]
    return strValue