from commonUtils import insertTM

TARGET_NAME = 'substance'
ID_KEY = 'IDE.XRN'
INSERT_INTO_TABLE_IP = '''
INSERT INTO IP(IDE_XRN,IP_L,IP_CITATION,IP_SOURCE,IP_IP,IP_MET,IP_ED,IP_LCN,IP_TAG,IP_COM) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
'''
KEY_NAME = ['IP.L', 'IP.citation', 'IP.SOURCE', 'IP.IP','IP.MET', 'IP.ED', 'IP.LCN', 'IP.TAG', 'IP.COM']

def dealXML(xmlStr):
    insertTM.setValue(TARGET_NAME, ID_KEY, INSERT_INTO_TABLE_IP, KEY_NAME)
    insertTM.dealXML(xmlStr)