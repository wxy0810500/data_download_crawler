from commonUtils import insertTM

TARGET_NAME = 'substance'
ID_KEY = 'IDE.XRN'
INSERT_INTO_TABLE_MP_BP = '''
INSERT INTO MP_BP(IDE_XRN, MP_L, MP_CITATION, MP_SOURCE, MP_MP, MP_SOL, MP_CSOL, MP_AMNT, MP_ED, MP_LCN, MP_TAG, MP_COM, BP_L, BP_CITATION, BP_BP, BP_P, BP_ED, BP_LCN, BP_TAG, BP_COM) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
'''
KEY_NAME = ['MP.L','MP.citation','MP.SOURCE','MP.MP','MP.SOL','MP.CSOL','MP.AMNT','MP.ED','MP.LCN','MP.TAG','MP.COM','BP.L','BP.citation','BP.BP','BP.P','BP.ED','BP.LCN','BP.TAG','BP.COM']

def dealXML(xmlStr):
    insertTM.setValue(TARGET_NAME, ID_KEY, INSERT_INTO_TABLE_MP_BP, KEY_NAME)
    insertTM.dealXML(xmlStr)