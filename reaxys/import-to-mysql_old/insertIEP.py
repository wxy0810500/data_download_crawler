from commonUtils import insertTM

TARGET_NAME = 'substance'
ID_KEY = 'IDE.XRN'
INSERT_INTO_TABLE_IEP = '''
INSERT INTO IEP(IDE_XRN,IEP_L,IEP_CITATION,IEP_SOURCE,IEP_IEP,IEP_SOL,IEP_ED,IEP_LCN,IEP_TAG,IEP_COM) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
'''
KEY_NAME = ['IEP.L', 'IEP.citation', 'IEP.SOURCE', 'IEP.IEP','IEP.SOL', 'IEP.ED', 'IEP.LCN', 'IEP.TAG', 'IEP.COM']

def dealXML(xmlStr):
    insertTM.setValue(TARGET_NAME, ID_KEY, INSERT_INTO_TABLE_IEP, KEY_NAME)
    insertTM.dealXML(xmlStr)