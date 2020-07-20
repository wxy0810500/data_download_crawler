from commonUtils import insertTM

TARGET_NAME = 'substance'
ID_KEY = 'IDE.XRN'
INSERT_INTO_TABLE_POW = '''
INSERT INTO POW(IDE_XRN,POW_L,POW_CITATION,POW_SOURCE,POW_POW,POW_LOG,POW_T,POW_PH,POW_ED,POW_LCN,POW_TAG,POW_COM) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
'''
KEY_NAME = ['POW.L', 'POW.citation', 'POW.SOURCE', 'POW.POW','POW.LOG','POW.T', 'POW.PH', 'POW.ED', 'POW.LCN', 'POW.TAG', 'POW.COM']

def dealXML(xmlStr):
    insertTM.setValue(TARGET_NAME, ID_KEY, INSERT_INTO_TABLE_POW, KEY_NAME)
    insertTM.dealXML(xmlStr)