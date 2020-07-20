from commonUtils import insertTM

TARGET_NAME = 'substance'
ID_KEY = 'IDE.XRN'
INSERT_INTO_TABLE_CSG = '''
INSERT INTO CSG(IDE_XRN,CSG_L,CSG_CITATION,CSG_SOURCE,CSG_CSG,CSG_ED,CSG_LCN,CSG_TAG,CSG_COM) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s)
'''
KEY_NAME = ['CSG.L', 'CSG.citation', 'CSG.SOURCE', 'CSG.CSG', 'CSG.ED', 'CSG.LCN', 'CSG.TAG', 'CSG.COM']

def dealXML(xmlStr):
    insertTM.setValue(TARGET_NAME, ID_KEY, INSERT_INTO_TABLE_CSG, KEY_NAME)
    insertTM.dealXML(xmlStr)