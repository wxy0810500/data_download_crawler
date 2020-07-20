from commonUtils import insertTM

TARGET_NAME = 'substance'
ID_KEY = 'IDE.XRN'
INSERT_INTO_TABLE_SDIC = '''
INSERT INTO SDIC(IDE_XRN,SDIC_L,SDIC_CITATION,SDIC_SOURCE,SDIC_SDIC,SDIC_T,SDIC_ED,SDIC_LCN,SDIC_TAG,SDIC_COM) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
'''
KEY_NAME = ['SDIC.L', 'SDIC.citation', 'SDIC.SOURCE', 'SDIC.SDIC','SDIC.T', 'SDIC.ED', 'SDIC.LCN', 'SDIC.TAG', 'SDIC.COM']

def dealXML(xmlStr):
    insertTM.setValue(TARGET_NAME, ID_KEY, INSERT_INTO_TABLE_SDIC, KEY_NAME)
    insertTM.dealXML(xmlStr)
