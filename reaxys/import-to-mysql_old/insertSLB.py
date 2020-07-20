import mysql.connector
from xml.dom.minidom import parse
import xml.dom.minidom
import os
from concurrent.futures import ThreadPoolExecutor

INSERT_INTO_SLB = '''
INSERT INTO SLB(IDE_XRN,SLB_L,SLB_CITATION,SLB_SOURCE,SLB_SLB,SLB_SAT,SLB_T,SLB_SOL,SLB_RAT,SLB_ED,SLB_LCN,SLB_TAG,SLB_COM) VALUES(%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
'''
INSERT_INTO_SLBP = '''
INSERT INTO SLBP(IDE_XRN,SLBP_L,SLBP_CITATION,SLBP_SOURCE,SLBP_SLBP,SLBP_T,SLBP_SOL,SLBP_RAT,SLBP_ED,SLBP_LCN,SLBP_TAG,SLBP_COM) VALUES(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
'''
INSERT_INTO_SOLM = '''
INSERT INTO SOLM(IDE_XRN,SOLM_L,SOLM_CITATION,SOLM_SOURCE,SOLM_KW,SOLM_PB,SOLM_PA,SOLM_SOL,SOLM_T,SOLM_P,SOLM_ED,SOLM_LCN,SOLM_TAG,SOLM_COM) VALUES(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
'''
SLB_KEY = ['SLB.L','SLB.CITATION','SLB.SOURCE','SLB.SLB','SLB.SAT','SLB.T','SLB.SOL','SLB.RAT','SLB.ED','SLB.LCN','SLB.TAG','SLB.COM']
SLBP_KEY = ['SLBP.L','SLBP.CITATION','SLBP.SOURCE','SLBP.SLBP','SLBP.T','SLBP.SOL','SLBP.RAT','SLBP.ED','SLBP.LCN','SLBP.TAG','SLBP.COM']
SOLM_KEY = ['SOLM.L','SOLM.CITATION','SOLM.SOURCE','SOLM.KW','SOLM.PB','SOLM.PA','SOLM.SOL','SOLM.T','SOLM.P','SOLM.ED','SOLM.LCN','SOLM.TAG','SOLM.COM']
KEY_NAME = 'substance'

def dealXML(domStr):
    print(domStr)
    tdb = mysql.connector.connect(
    host="localhost",
    user="root",
    passwd="123456",
    database="ghddi",
    auth_plugin='mysql_native_password'
    ) 
    tcursor = tdb.cursor()
    domTree = xml.dom.minidom.parseString(domStr)
    collection = domTree.documentElement
    nodeList = collection.getElementsByTagName(KEY_NAME)
    insertSLB = []
    insertSLBP = []
    insertSOLM = []
    for node in nodeList: 
        ideXrn = node.getElementsByTagName('IDE.XRN')[0].childNodes[0].data
        if ideXrn == None:
            continue
        insertSLB.append(dealSLB(ideXrn,node))
        insertSLBP.append(dealSLBP(ideXrn,node))
        insertSOLM.append(dealSOLM(ideXrn,node))
    tcursor.executemany(INSERT_INTO_SLB,insertSLB)
    tcursor.executemany(INSERT_INTO_SLBP,insertSLBP)
    tcursor.executemany(INSERT_INTO_SOLM,insertSOLM)
    tdb.commit()
    tcursor.closeSession()
    tdb.closeSession()
    

def dealSLB(ideXrn,node):
    slb = [ideXrn]
    for key in SLB_KEY:
        slb.append(arrange(node.getElementsByTagName(key)))
    return slb

def dealSLBP(ideXrn,node):
    slbp = [ideXrn]
    for key in SLBP_KEY:
        slbp.append(arrange(node.getElementsByTagName(key)))
    return slbp

def dealSOLM(ideXrn,node):
    solm = [ideXrn]
    for key in SOLM_KEY:
        solm.append(arrange(node.getElementsByTagName(key)))
    return solm

def arrange(nodeList):
    if nodeList == None or len(nodeList) == 0:
        return None
    strValue = ''
    for node in nodeList:
        strValue = strValue + node.childNodes[0].data + ';'
    if len(strValue) > 0:
        strValue = strValue[:-1]
    return strValue

# if __name__ == '__main__':
#     # dealXML('data_503_251001.txt')
#     listDir = os.listdir('/home/wangjm/software/python/others-example/case/case/SLB-1')
#     print('start')
#     with ThreadPoolExecutor(20) as exector:
#         exector.map(dealXML,listDir)
#     print('end')

