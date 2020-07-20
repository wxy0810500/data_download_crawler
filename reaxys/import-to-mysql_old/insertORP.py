from commonUtils import insertTM

TARGET_NAME = 'substance'
ID_KEY = 'IDE.XRN'
INSERT_INTO_TABLE_ORP = '''
INSERT INTO ORP(IDE_XRN,ORP_L,ORP_CITATION,ORP_SOURCE,ORP_ORP,ORP_TYP,ORP_C,ORP_EE,ORP_LEN,ORP_SOL,ORP_W,ORP_T,ORP_ED,ORP_LCN,ORP_TAG,ORP_COM) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
'''
KEY_NAME = ['ORP.L', 'ORP.citation', 'ORP.SOURCE', 'ORP.ORP','ORP.TYP','ORP.C','ORP.EE','ORP.LEN','ORP.SOL','ORP.W','ORP.T', 'ORP.ED', 'ORP.LCN', 'ORP.TAG', 'ORP.COM']

def dealXML(xmlStr):
    insertTM.setValue(TARGET_NAME, ID_KEY, INSERT_INTO_TABLE_ORP, KEY_NAME)
    insertTM.dealXML(xmlStr)
