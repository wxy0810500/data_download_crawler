import mysql.connector
from xml.dom.minidom import parse
import xml.dom.minidom
import os

TARGET_NAME = ''
ID_KEY = ''
INSERT_SQL = ''
KEY_LIST = []


def setValue(targetName, idKey, insertSql, keyList):
    global TARGET_NAME
    TARGET_NAME = targetName
    global ID_KEY
    ID_KEY = idKey
    global INSERT_SQL
    INSERT_SQL = insertSql
    global KEY_LIST
    KEY_LIST = keyList


def setBasicValue(basePath, targetName, idKey):
    global BASE_PATH
    BASE_PATH = basePath
    global TARGET_NAME
    TARGET_NAME = targetName
    global ID_KEY
    ID_KEY = idKey


def setSqlValue(insertSql, keyList):
    global INSERT_SQL
    INSERT_SQL = insertSql
    global KEY_LIST
    KEY_LIST = keyList


def printG():
    print(TARGET_NAME, ID_KEY, INSERT_SQL, KEY_LIST)


def dealXML(xmlStr):
    tdb = mysql.connector.connect(
        host="localhost",
        user="root",
        passwd="123456",
        database="ghddi",
        auth_plugin='mysql_native_password'
    )
    tcursor = tdb.cursor()
    if xmlStr.count('<hi>') != xmlStr.count('</hi>'):
        print('fail')
        return
    fileLine = xmlStr.replace('<hi>', '').replace('</hi>', '')
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
        insert.append(deal(tId, node))
    tcursor.executemany(INSERT_SQL, insert)
    tdb.commit()
    tcursor.closeSession()
    tdb.closeSession()


def getTree(fileName):
    with open(os.path.join('%s/%s' % (BASE_PATH, fileName)), 'r') as file:
        fileLine = ''.join(line for line in file)
        if fileLine.count('<hi>') != fileLine.count('</hi>'):
            return
    if fileLine.count('<hi>') != fileLine.count('</hi>'):
        return
    fileLine = fileLine.replace('<hi>', '').replace('</hi>', '')
    domTree = xml.dom.minidom.parseString(fileLine)
    return domTree


def dealXMLByTree(domTree):
    tdb = mysql.connector.connect(
        host="localhost",
        user="root",
        passwd="123456",
        database="ghddi_spe",
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
        if tId is None:
            continue
        res = deal(tId, node)
        insertJude = False
        for resValue in res[1:]:
            if resValue is not None:
                insertJude = True
                break
        if insertJude:
            insert.append(res)
    tcursor.executemany(INSERT_SQL, insert)
    tdb.commit()
    tcursor.closeSession()
    tdb.closeSession()


def deal(tId, node):
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
