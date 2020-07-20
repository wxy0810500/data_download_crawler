from commonUtils import insertTM

TARGET_NAME = 'substance'
ID_KEY = 'IDE.XRN'
INSERT_INTO_TABLE_CDIC = '''
INSERT INTO CDIC(IDE_XRN,CDIC_L,CDIC_CITATION,CDIC_SOURCE,CDIC_SOL,CDIC_ED,CDIC_LCN,CDIC_TAG,CDIC_COM) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s)
'''
KEY_NAME = ['CDIC.L', 'CDIC.citation', 'CDIC.SOURCE', 'CDIC.SOL', 'CDIC.ED', 'CDIC.LCN', 'CDIC.TAG', 'CDIC.COM']

def dealXML(xmlStr):
    insertTM.setValue(TARGET_NAME, ID_KEY, INSERT_INTO_TABLE_CDIC, KEY_NAME)
    insertTM.dealXML(xmlStr)
