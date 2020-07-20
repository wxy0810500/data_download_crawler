import mysql.connector
from commonUtils import insertTM
from xml.dom.minidom import parse
import xml.dom.minidom

TARGET_NAME = 'substance'
ID_KEY = 'IDE.XRN'
INSERT_INTO_TABLE_AZE = '''
INSERT INTO AZE(IDE_XRN,AZE_L,AZE_CITATION,AZE_SOURCE,AZE_T,AZE_P,AZE_C,AZE_ED,AZE_LCN,AZE_TAG,AZE_COM) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
'''
INSERT_INTO_TABLE_AZE_P = '''
INSERT INTO AZE_P(IDE_XRN,AZE_PA,AZE_PB) VALUES (%s,%s,%s)
'''
KEY_NAME = ['AZE.L', 'AZE.citation', 'AZE.SOURCE','AZE.T','AZE.P','AZE.C', 'AZE.ED', 'AZE.LCN', 'AZE.TAG', 'AZE.COM']

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
        nodeList2 = node.getElementsByTagName('AZE01')
        for node2 in nodeList2:
            pa = node2.getElementsByTagName('AZE.PA')[0].childNodes[0].nodeValue
            pbList = node2.getElementsByTagName('AZE.PB')
            if len(pbList) > 0:
                pb = pbList[0].childNodes[0].nodeValue
            else :
                pb = None
            insertList.append([tId,pa,pb])
        dCursor.executemany(INSERT_INTO_TABLE_AZE_P,insertList)
        dDb.commit()
        dCursor.closeSession()
        dDb.closeSession()
    insertTM.setValue(TARGET_NAME, ID_KEY, INSERT_INTO_TABLE_AZE, KEY_NAME)
    insertTM.dealXML(xmlStr)
       

# if __name__ == '__main__':
#     insertTM.setValue(BASE_PATH,TARGET_NAME,ID_KEY,INSERT_INTO_TABLE_AZE,KEY_NAME)
#     #dealXMLS('AZE.txt')
#     listDir = os.listdir(BASE_PATH)
#     with ThreadPoolExecutor(20) as exector:
#         exector.map(dealXMLS,listDir)