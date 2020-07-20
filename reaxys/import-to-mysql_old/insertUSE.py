from commonUtils import insertTM

TARGET_NAME = 'substance'
ID_KEY = 'IDE.XRN'

INSERT_INTO_TABLE_USE = '''
INSERT INTO G_USE(IDE_XRN,USE_L,USE_CITATION,USE_SOURCE,USE_LH,USE_PT,USE_ED,USE_LCN,USE_TAG,USE_COM) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
'''
KEY_NAME = ['USE.L', 'USE.citation', 'USE.SOURCE','USE.LH','USE.PT', 'USE.ED', 'USE.LCN', 'USE.TAG', 'USE.COM']

def dealXML(xmlStr):
    insertTM.setValue(TARGET_NAME, ID_KEY, INSERT_INTO_TABLE_USE, KEY_NAME)
    insertTM.dealXML(xmlStr)