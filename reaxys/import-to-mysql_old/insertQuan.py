from commonUtils import insertTM

TARGET_NAME = 'substance'
ID_KEY = 'IDE.XRN'
INSERT_QUAN = '''
INSERT INTO QUAN (IDE_XRN,QUAN_L,QUAN_CITATION,QUAN_SOURCE,QUAN_PROP,QUAN_MET,QUAN_ED,QUAN_LCN,QUAN_TAG,QUAN_COM) VALUES(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
'''
KEY_NAME = ['QUAN.L','QUAN.CITATION','QUAN.SOURCE','QUAN.PROP','QUAN.MET','QUAN.ED','QUAN.LCN','QUAN.TAG','QUAN.COM']

def dealXML(xmlStr):
    insertTM.setValue(TARGET_NAME, ID_KEY, INSERT_QUAN, KEY_NAME)
    insertTM.dealXML(xmlStr)
