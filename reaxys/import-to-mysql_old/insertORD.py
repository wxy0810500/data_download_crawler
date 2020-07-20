from commonUtils import insertTM

BASE_PATH = '/home/wangjm/software/python/temp'
TARGET_NAME = 'substance'
ID_KEY = 'IDE.XRN'
INSERT_INTO_TABLE_ORD = '''
INSERT INTO ORD(IDE_XRN,ORD_L,ORD_CITATION,ORD_SOURCE,ORD_SOL,ORD_ED,ORD_LCN,ORD_TAG,ORD_COM) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s)
'''
KEY_NAME = ['ORD.L', 'ORD.citation', 'ORD.SOURCE', 'ORD.SOL', 'ORD.ED', 'ORD.LCN', 'ORD.TAG', 'ORD.COM']

def dealXML(xmlStr):
    insertTM.setValue(TARGET_NAME, ID_KEY, INSERT_INTO_TABLE_ORD, KEY_NAME)
    insertTM.dealXML(xmlStr)

