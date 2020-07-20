from commonUtils import insertTM

BASE_PATH = '/home/wangjm/software/python/temp'
TARGET_NAME = 'substance'
ID_KEY = 'IDE.XRN'
INSERT_INTO_TABLE_EDIS = '''
INSERT INTO EDIS(IDE_XRN,EDIS_L,EDIS_CITATION,EDIS_SOURCE,EDIS_EDIS,EDIS_TYP,EDIS_ED,EDIS_LCN,EDIS_TAG,EDIS_COM) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
'''
KEY_NAME = ['EDIS.L', 'EDIS.citation', 'EDIS.SOURCE', 'EDIS.EDIS','EDIS.TYP', 'EDIS.ED', 'EDIS.LCN', 'EDIS.TAG', 'EDIS.COM']

def dealXML(xmlStr):
    insertTM.setValue(TARGET_NAME, ID_KEY, INSERT_INTO_TABLE_EDIS, KEY_NAME)
    insertTM.dealXML(xmlStr)
