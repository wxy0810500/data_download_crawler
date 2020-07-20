import mysql.connector
import pprint
from rdkit import rdBase
from rdkit import Chem
from rdkit.Chem.rdmolfiles import SmilesWriter


gdb = mysql.connector.connect(
    host="localhost",
    user="root",
    passwd="123456",
    database="ghddi",
    auth_plugin='mysql_native_password'
    )
selectYYCursor = gdb.cursor()
selectYYCursor.execute('select IDE_XRN,YY_STR from YY where IDE_XRN in (select IDE_XRN from IDE_NEW) and YY_STR is not null')

ugdb = mysql.connector.connect(
    host="localhost",
    user="root",
    passwd="123456",
    database="ghddi",
    auth_plugin='mysql_native_password'
    )
updateCursor = ugdb.cursor()
updateSql = 'update GHDDI_SList set SMILES_ISO_YY = %s,SMILES_YY=%s where IDE_XRN = %s'

line = selectYYCursor.fetchone()
while line != None:
    stringValue = str(line[1]).split('HDR')
    try:
        yyValue = '$$$$\n' + stringValue[0].lstrip() + 'HDR\n' + stringValue[2][1:]
    except IndexError as identifier:
        print(line[0])
        line = selectYYCursor.fetchone()
        continue

    m = Chem.MolFromMolBlock(yyValue)
    if m == None:
        print(line[0])
        line = selectYYCursor.fetchone()
        continue
    isoSmiles = Chem.MolToSmiles(m)
    smiles = Chem.MolToSmiles(m, isomericSmiles=False)

    updateCursor.execute(updateSql,[str(isoSmiles),str(smiles),line[0]])
    ugdb.commit()
    line = selectYYCursor.fetchone()

print('success')
updateCursor.closeSession()
selectYYCursor.closeSession()
gdb.closeSession()
ugdb.closeSession()
# for line in yyStr:
#     stringValue = str(line[1]).split('HDR')
#     yyValue = '$$$$\n' + stringValue[0].lstrip() + 'HDR\n' + stringValue[2][1:]
#     m = Chem.MolFromMolBlock(yyValue)
#     smiles = Chem.MolToSmiles(m)
#     print(str(smiles),line[0])
#     cursor.execute(updateSql,[str(smiles),line[0]])
#     gdb.commit()
#     i += 1
#     print(i)
# with open('yyw.sdf','w') as wYy:
#     for line in yyStr:
#         print(line)
#         wYy.writelines(str(line[0]).lstrip() + '\n')
#         stringValue = str(line[1]).split('HDR')
#         if len(stringValue) == 3:
#             wYy.writelines(stringValue[0].lstrip())
#             wYy.writelines('HDR\n')
#             wYy.write(stringValue[2][1:])
#             wYy.write('$$$$\n')
#         else:
#             print(line[0])

