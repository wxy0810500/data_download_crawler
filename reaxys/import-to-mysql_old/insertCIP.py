from commonUtils import insertTM

TARGET_NAME = 'substance'
ID_KEY = 'IDE.XRN'
INSERT_INTO_TABLE_CIP = '''
INSERT INTO CIP(IDE_XRN,CIP_L,CIP_CITATION,CIP_SOURCE,CIP_KW,CIP_ED,CIP_LCN,CIP_TAG,CIP_COM) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s)
'''
KEY_NAME = ['CIP.L', 'CIP.citation', 'CIP.SOURCE', 'CIP.KW', 'CIP.ED', 'CIP.LCN', 'CIP.TAG', 'CIP.COM']

def dealXML(xmlStr):
    insertTM.setValue(TARGET_NAME, ID_KEY, INSERT_INTO_TABLE_CIP, KEY_NAME)
    insertTM.dealXML(xmlStr)
