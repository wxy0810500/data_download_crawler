import mysql.connector
from xml.dom.minidom import parse
import xml.dom.minidom
import os
from concurrent.futures import ThreadPoolExecutor
# from rdkit import rdBase
# from rdkit import Chem
# from rdkit.Chem.rdmolfiles import SmilesWriter

INSERT_TABLE_RX = '''
INSERT INTO RX(RX_ID,RX_BLA,RX_BLB,RX_BLC,RX_NVAR,RX_BIN,RX_BFREQ,RX_BRANGE,RX_BNAME,RX_RXNFILE,RX_REG,RX_RANK,RX_MYD,RX_SKW,RX_RTYP,RX_TNAME,RX_RAVAIL,RX_PAVAIL,RX_MAXPUB,RX_NUMREF,RX_MAXPMW,RX_ED,RX_UPD,RX_TRANS,RX_BCODE,RX_MCODE,RX_NCODE,RX_QRY)
VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
'''
INSERT_TABLE_RX_RXRN = '''
INSERT INTO RX_RXRN(RX_ID, RX_RXRN, RX_RCT) VALUES (%s,%s,%s)
'''
INSERT_TABLE_RX_PXRN = '''
INSERT INTO RX_PXRN(RX_ID, RX_PXRN, RX_PRO) VALUES (%s,%s,%s)         
'''
INSERT_TABLE_RXNLINK = '''
INSERT INTO RXNLINK(RX_ID, RXNLINK_NAME, RXNLINK_OTH_RXN,RXNLINK_OWN_SUB,RXNLINK_OTH_SUB) VALUES (%s,%s,%s,%s,%s)
'''
INSERT_TABLE_RXD = '''
INSERT INTO RXD(RX_ID,RXD_L,RXD_CITATION,RXD_SOURCE,RXD_CL,RXD_SCO,RXD_LB,RXD_TI,RXD_TXT,RXD_STP,RXD_MID,RXD_MTEXT,RXD_YXRN,RXD_YPRO,RXD_YD,RXD_NYD,RXD_YDO,RXD_SNR)
VALUES(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
'''
INSERT_TABLE_RXD_STG = '''
INSERT INTO RXD_STG(RX_ID,RXD_STG,RXD_RGTCAT,RXD_SPH,RXD_TIM,RXD_T,RXD_P,RXD_PH,RXD_COND,RXD_TYP,RXD_SUB,RXD_PRT,RXD_NAME,RXD_PARENTLINK,RXD_PARENTREF,RXD_RXDES,RXD_DED,RXD_LCN,RXD_TAG,RXD_COM)
VALUES(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
'''
INSERT_TABLE_RXD_SXRN = '''
INSERT INTO RXD_SXRN(RX_ID,RXD_STG, RXD_SXRN, RXD_SRCT) VALUES(%s,%s,%s,%s)
'''
INSERT_TABLE_RXD_RGTXRN = '''
INSERT INTO RXD_RGTXRN(RX_ID,RXD_STG, RXD_RGTXRN, RXD_RGT) VALUES(%s,%s,%s,%s)
'''
INSERT_TABLE_RXD_SOLXRN = '''
INSERT INTO RXD_SOLXRN(RX_ID,RXD_STG, RXD_SOLXRN, RXD_SOL) VALUES(%s,%s,%s,%s)
'''
INSERT_TABLE_RXD_CATXRN = '''
INSERT INTO RXD_CATXRN(RX_ID,RXD_STG, RXD_CATXRN, RXD_CAT) VALUES(%s,%s,%s,%s)
'''
# INSERT_TABLE_RXD_RY = '''
# INSERT INTO RXD_RY(YY_ID, YY_C_STR) VALUES(%s,%s)
# '''
KEY_NAME = 'reaction'
RX_KEY = ['RX.BLA','RX.BLB','RX.BLC','RX.NVAR','RX.BIN','RX.BFREQ','RX.BRANGE','RX.BNAME','RX.RXNFILE','RX.REG','RX.RANK','RX.MYD','RX.SKW','RX.RTYP','RX.TNAME','RX.RAVAIL','RX.PAVAIL','RX.MAXPUB','RX.NUMREF','RX.MAXPMW','RX.ED','RX.UPD','RX.TRANS','RX.BCODE','RX.MCODE','RX.NCODE']
RXNLINK_KEY = [' RXNLINK.NAME',' RXNLINK.OTH.RXN','RXNLINK.OWN.SUB','RXNLINK.OTH.SUB']
RXD_KEY = ['RXD.L','RXD.CITATION','RXD.SOURCE','RXD.CL','RXD.SCO','RXD.LB','RXD.TI','RXD.TXT','RXD.STP','RXD.MID','RXD.MTEXT','RXD.YXRN','RXD.YPRO','RXD.YD','RXD.NYD','RXD.YDO','RXD.SNR']
RXD_STG_KEY = ['RXD.RGTCAT','RXD.SPH','RXD.TIM','RXD.T','RXD.P','RXD.PH','RXD.COND','RXD.TYP','RXD.SUB','RXD.PRT','RXD.NAME','RXD.PARENTLINK','RXD.PARENTREF','RXD.RXDES','RXD.DED','RXD.LCN','RXD.TAG','RXD.COM']
db = mysql.connector.connect(
    host="localhost",
    user="root",
    passwd="123456",
    database="ghddi",
    auth_plugin='mysql_native_password'
) 
cursor = db.cursor()
# cursor.execute('DROP TABLE IF EXISTS RX')
# cursor.execute('DROP TABLE IF EXISTS RX_RXRN')
# cursor.execute('DROP TABLE IF EXISTS RX_PXRN')
# cursor.execute('DROP TABLE IF EXISTS RXNLINK')
# cursor.execute('DROP TABLE IF EXISTS RXD')
# cursor.execute('DROP TABLE IF EXISTS RXD_STG')
# cursor.execute('DROP TABLE IF EXISTS RXD_SXRN')
# cursor.execute('DROP TABLE IF EXISTS RXD_RGTXRN')
# cursor.execute('DROP TABLE IF EXISTS RXD_SOLXRN')
# cursor.execute('DROP TABLE IF EXISTS RXD_CATXRN')
# # cursor.execute('DROP TABLE IF EXISTS RXD_RY')
# db.commit()
# cursor.execute(CREATE_TABLE_RX)
# cursor.execute(CREATE_TABLE_RX_RXRN)
# cursor.execute(CREATE_TABLE_RX_PXRN)
# cursor.execute(CREATE_TABLE_RXNLINK)
# cursor.execute(CREATE_TABLE_RXD)
# cursor.execute(CREATE_TABLE_RXD_STG)
# cursor.execute(CREATE_TABLE_RXD_SXRN)
# cursor.execute(CREATE_TABLE_RXD_RGTXRN)
# cursor.execute(CREATE_TABLE_RXD_SOLXRN)
# cursor.execute(CREATE_TABLE_RXD_CATXRN)
# cursor.execute(CREATE_TABLE_RXD_RY)
# db.commit()
cursor.closeSession()
db.closeSession()

def dealXML(xmlStr):
    tdb = mysql.connector.connect(
    host="localhost",
    user="root",
    passwd="123456",
    database="ghddi",
    auth_plugin='mysql_native_password'
    ) 
    tcursor = tdb.cursor()
    if xmlStr.count('<i>') != xmlStr.count('</i>'):
        return
    fileLine = xmlStr.replace('<i>','').replace('</i>','')
    domTree = xml.dom.minidom.parseString(fileLine)
    collection = domTree.documentElement
    nodeList = collection.getElementsByTagName(KEY_NAME)
    rXValue = []
    rxRxrnValue = []
    rxpxrnValue = []
    rxnLinkValue = []
    rxdValue = []
    rxdStgValue = []
    rxdSxrnValue = []
    rxdRgtXrnValue = []
    rxdCatXrnValue = []
    rxdSolXrnValue = []
    for node in nodeList: 
        rxId = node.getElementsByTagName('RX.ID')[0].childNodes[0].data
        if rxId == None:
            continue
        rXValue.append(dealRX(rxId,node))
        rxRxrnValue.extend(dealRxRxRn(rxId,node))
        rxpxrnValue.extend(dealRxPxRn(rxId,node))
        rxnLinkValue.append(dealRxnLink(rxId,node))
        rxdValue.append(dealRxd(rxId,node))
        rxdRe = dealRxdStage(rxId,node)
        addArr(rxdStgValue,rxdRe[0])
        addArr(rxdSxrnValue,rxdRe[1])
        addArr(rxdRgtXrnValue,rxdRe[2])
        addArr(rxdCatXrnValue,rxdRe[3])
        addArr(rxdSolXrnValue,rxdRe[4])
    tcursor.executemany(INSERT_TABLE_RX,rXValue)
    tdb.commit()
    tcursor.executemany(INSERT_TABLE_RX_RXRN,rxRxrnValue)
    tdb.commit()
    tcursor.executemany(INSERT_TABLE_RX_PXRN,rxpxrnValue)
    tdb.commit()
    tcursor.executemany(INSERT_TABLE_RXNLINK,rxnLinkValue)
    tdb.commit()
    tcursor.executemany(INSERT_TABLE_RXD,rxdValue)
    tdb.commit()
    tcursor.executemany(INSERT_TABLE_RXD_STG,rxdStgValue)
    tdb.commit()
    tcursor.executemany(INSERT_TABLE_RXD_SXRN,rxdSxrnValue)
    tdb.commit()
    tcursor.executemany(INSERT_TABLE_RXD_RGTXRN,rxdRgtXrnValue)
    tdb.commit()
    tcursor.executemany(INSERT_TABLE_RXD_CATXRN,rxdCatXrnValue)
    tdb.commit()
    tcursor.executemany(INSERT_TABLE_RXD_SOLXRN,rxdSolXrnValue)
    tdb.commit()
        # tcursor.executemany(INSERT_TABLE_RXD_RY,dealYy(collection))
        # tdb.commit()
    tcursor.closeSession()
    tdb.closeSession()



def addArr(arr, subArr):
    if len(subArr) > 0:
        arr.extend(subArr)

def dealRX(rxId, node):
    rx = [rxId]
    for key in RX_KEY:
        rx.append(arrange(node.getElementsByTagName(key)))
    i = 0
    qry = ''
    while len(node.getElementsByTagName('RX.QRY' + str(i))) >= 1:
        qry = qry + node.getElementsByTagName('RX.QRY' + str(i))[0].childNodes[0].data + ';'
        i += 1
    if len(qry) > 0:
        qry = qry[:-1]
        rx.append(qry)
    else:
        rx.append(None)
    return rx

def dealRxnLink(rxId, node):
    rxLink = [rxId]
    for key in RXNLINK_KEY:
        rxLink.append(arrange(node.getElementsByTagName(key)))
    return rxLink

def dealRxd(rxId, node):
    rxd = [rxId]
    for key in RXD_KEY:
        rxd.append(arrange(node.getElementsByTagName(key)))
    return rxd

def dealRxRxRn(rxId, node):
    rxrxrn = []
    rx01 = node.getElementsByTagName('RX01')
    for node in rx01:
        value = [rxId]
        value.append(arrange(node.getElementsByTagName('RX.RXRN')))
        value.append(arrange(node.getElementsByTagName('RX.RCT')))
        rxrxrn.append(value)
    return rxrxrn

def dealRxPxRn(rxId, node):
    rxpxrn = []
    rx02 = node.getElementsByTagName('RX02')
    for node in rx02:
        value = [rxId]
        value.append(arrange(node.getElementsByTagName('RX.PXRN')))
        value.append(arrange(node.getElementsByTagName('RX.PRO')))
        rxpxrn.append(value)
    return rxpxrn

def dealRxdStage(rxId, node):
    rxds = node.getElementsByTagName('RXDS01')
    stage = '1'
    stageRe = []
    sxrnRe = []
    rgtxrnRe = []
    catxrnRe = []
    solxrnRe = []
    for node in rxds:
        if len(node.getElementsByTagName('RXD.STG')) >= 1:
            stage = node.getElementsByTagName('RXD.STG')[0].childNodes[0].data
        stageList = [rxId,stage]
        value = False
        for key in RXD_STG_KEY:
            tvalue = arrange(node.getElementsByTagName(key))
            if tvalue != None:
                value = True
            stageList.append(tvalue)
        if value:
            stageRe.append(stageList)
        for subNode in node.getElementsByTagName('RXD02'):
            sXrn = [rxId,stage]
            sXrn.append(arrange(subNode.getElementsByTagName('RXD.SXRN')))
            sXrn.append(arrange(subNode.getElementsByTagName('RXD.SRCT')))
            sxrnRe.append(sXrn)
        for subNode in node.getElementsByTagName('RXD03'):
            rgtXrn = [rxId,stage]
            rgtXrn.append(arrange(subNode.getElementsByTagName('RXD.RGTXRN')))
            rgtXrn.append(arrange(subNode.getElementsByTagName('RXD.RGT')))
            rgtxrnRe.append(rgtXrn)
        for subNode in node.getElementsByTagName('RXD04'):
            catXrn = [rxId,stage]
            catXrn.append(arrange(subNode.getElementsByTagName('RXD.CATXRN')))
            catXrn.append(arrange(subNode.getElementsByTagName('RXD.CAT')))
            catxrnRe.append(catXrn)
        for subNode in node.getElementsByTagName('RXD05'):
            solXrn = [rxId,stage]
            solXrn.append(arrange(subNode.getElementsByTagName('RXD.SOLXRN')))
            solXrn.append(arrange(subNode.getElementsByTagName('RXD.SOL')))
            solxrnRe.append(solXrn)
    return stageRe,sxrnRe,rgtxrnRe,catxrnRe,solxrnRe

# def dealYy(node):
#     value = []
#     addArr(value,dealSubYy(node.getElementsByTagName('RY.STR')))
#     addArr(value,dealSubYy(node.getElementsByTagName('RY.RCT')))
#     addArr(value,dealSubYy(node.getElementsByTagName('RY.PRO')))
#     return value

# def dealSubYy(nodeList):
#     value = []
#     for node in nodeList:
#         id = node.getAttribute('rn')
#         yy = node.childNodes[0].data
#         # m = Chem.MolFromMolBlock(yy)
#         # isoSmiles = Chem.MolToSmiles(m)
#         # smiles = Chem.MolToSmiles(m,isomericSmiles=False)
#         value.append([id,yy])
#     return value




def arrange(nodeList):
    if nodeList == None or len(nodeList) == 0:
        return None
    strValue = ''
    for node in nodeList:
        strValue = strValue + node.childNodes[0].data + ';'
    if len(strValue) > 0:
        strValue = strValue[:-1]
    return strValue