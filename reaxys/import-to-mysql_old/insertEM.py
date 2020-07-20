from commonUtils import insertTM

TARGET_NAME = 'substance'
ID_KEY = 'IDE.XRN'
INSERT_INTO_TABLE_EM = '''
INSERT INTO EM(IDE_XRN,EM_L,EM_CITATION,EM_SOURCE,EM_KW,EM_EM,EM_T,EM_MET,EM_SOL,EM_ED,EM_LCN,EM_TAG,EM_COM) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
'''
KEY_NAME = ['EM.L', 'EM.citation', 'EM.SOURCE', 'EM.KW','EM.EM','EM.T','EM.MET','EM.SOL', 'EM.ED', 'EM.LCN', 'EM.TAG', 'EM.COM']

def dealXML(xmlStr):
    insertTM.setValue(TARGET_NAME, ID_KEY, INSERT_INTO_TABLE_EM, KEY_NAME)
    insertTM.dealXML(xmlStr)

