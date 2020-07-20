from commonUtils import insertTM

TARGET_NAME = 'substance'
ID_KEY = 'IDE.XRN'
INSERT_INTO_TABLE_DIC = '''
INSERT INTO DIC(IDE_XRN,DIC_L,DIC_CITATION,DIC_SOURCE,DIC_DIC,DIC_F,DIC_T,DIC_ED,DIC_LCN,DIC_TAG,DIC_COM) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
'''
KEY_NAME = ['DIC.L', 'DIC.citation', 'DIC.SOURCE', 'DIC.DIC','DIC.F','DIC.T', 'DIC.ED', 'DIC.LCN', 'DIC.TAG', 'DIC.COM']

def dealXML(xmlStr):
    insertTM.setValue(TARGET_NAME, ID_KEY, INSERT_INTO_TABLE_DIC, KEY_NAME)
    insertTM.dealXML(xmlStr)