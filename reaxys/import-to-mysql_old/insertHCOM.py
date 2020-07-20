from commonUtils import insertTM

TARGET_NAME = 'substance'
ID_KEY = 'IDE.XRN'
INSERT_INTO_TABLE_HCOM = '''
INSERT INTO HCOM(IDE_XRN,HCOM_L,HCOM_CITATION,HCOM_SOURCE,HCOM_HCOM,HCOM_T,HCOM_P,HCOM_ED,HCOM_LCN,HCOM_TAG,HCOM_COM) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
'''
KEY_NAME = ['HCOM.L', 'HCOM.citation', 'HCOM.SOURCE', 'HCOM.HCOM','HCOM.T','HCOM.P', 'HCOM.ED', 'HCOM.LCN', 'HCOM.TAG', 'HCOM.COM']

def dealXML(xmlStr):
    insertTM.setValue(TARGET_NAME, ID_KEY, INSERT_INTO_TABLE_HCOM, KEY_NAME)
    insertTM.dealXML(xmlStr)
