from commonUtils import insertTM

TARGET_NAME = 'substance'
ID_KEY = 'IDE.XRN'
INSERT_INTO_TABLE_ELP = '''
INSERT INTO ELP(IDE_XRN,ELP_L,ELP_CITATION,ELP_SOURCE,ELP_KW,ELP_ED,ELP_LCN,ELP_TAG,ELP_COM) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s)
'''
KEY_NAME = ['ELP.L', 'ELP.citation', 'ELP.SOURCE', 'ELP.KW', 'ELP.ED', 'ELP.LCN', 'ELP.TAG', 'ELP.COM']

def dealXML(xmlStr):
    insertTM.setValue(TARGET_NAME, ID_KEY, INSERT_INTO_TABLE_ELP, KEY_NAME)
    insertTM.dealXML(xmlStr)
