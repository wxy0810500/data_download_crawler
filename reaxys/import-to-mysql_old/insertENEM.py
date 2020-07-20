import mysql.connector
from commonUtils import insertTM
from xml.dom.minidom import parse
import xml.dom.minidom

TARGET_NAME = 'substance'
ID_KEY = 'IDE.XRN'
INSERT_INTO_TABLE_ENEM = '''
INSERT INTO ENEM(IDE_XRN,ENEM_L,ENEM_CITATION,ENEM_SOURCE,ENEM_KW,ENEM_SOL,ENEM_T,ENEM_P,ENEM_ED,ENEM_LCN,ENEM_TAG,ENEM_COM) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
'''
INSERT_INTO_TABLE_ENEM_P = '''
INSERT INTO ENEM_P(IDE_XRN,ENEM_PA,ENEM_PB) VALUES (%s,%s,%s)
'''
KEY_NAME = ['ENEM.L', 'ENEM.citation', 'ENEM.SOURCE','ENEM.KW','ENEM.SOL','ENEM.T','ENEM.P', 'ENEM.ED', 'ENEM.LCN', 'ENEM.TAG', 'ENEM.COM']

def dealXML(xmlStr):
    dDb = mysql.connector.connect(
    host="localhost",
    user="root",
    passwd="123456",
    database="ghddi",
    auth_plugin='mysql_native_password'
    )
    dCursor = dDb.cursor()
    domTree = xml.dom.minidom.parseString(xmlStr)
    collection = domTree.documentElement
    nodeList = collection.getElementsByTagName(TARGET_NAME)
    insertList = []
    for node in nodeList:
        tId = node.getElementsByTagName(ID_KEY)[0].childNodes[0].nodeValue
        if tId == None:
            continue
        nodeList2 = node.getElementsByTagName('ENEM01')
        for node2 in nodeList2:
            pa = node2.getElementsByTagName('ENEM.PA')[0].childNodes[0].nodeValue
            pbList = node2.getElementsByTagName('ENEM.PB')
            if len(pbList) > 0:
                pb = pbList[0].childNodes[0].nodeValue
            else :
                pb = None
            insertList.append([tId,pa,pb])
        dCursor.executemany(INSERT_INTO_TABLE_ENEM_P,insertList)
        dDb.commit()
        dCursor.closeSession()
        dDb.closeSession()
    insertTM.setValue(TARGET_NAME, ID_KEY, INSERT_INTO_TABLE_ENEM, KEY_NAME)
    insertTM.dealXML(xmlStr)