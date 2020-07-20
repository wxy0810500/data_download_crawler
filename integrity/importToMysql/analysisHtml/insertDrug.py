from bs4 import BeautifulSoup
import os
import time
import sys
from concurrent.futures import ThreadPoolExecutor

base_path = os.path.dirname(os.path.abspath(__file__)) + '/../../../'
sys.path.append(base_path)

from commonUtils.ghddiMysqlConnPool import GHDDI208MysqlConnPool

CREATE_MOA_TABLE = '''
'''

CREATE_INFO_TABLE = '''
'''

INSERT_INFO_TABLE = '''
INSERT INTO `integrity`.`druginf`
(`entry_number`,
`record_creation_date`,
`last_updated_date`,
`CAS_registry_number`,
`molecular_formula`,
`molecular_weight`,
`highest_phase`,
`under_active_development`,
`chemical_name/description`,
`code_name(CD)`,
`generic_name(GN)`,
`brand_name(BN)`,
`drug_name (CD, GN, BN)`,
`structure_entry_date`,
`smiles`,
`sequence`,
`sequence_type`,
`structure_img_url`,
`ghddi_entry_date`)
VALUES
(%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s);
'''

INSERT_MOA_TABLE = '''
INSERT INTO `integrity`.`drugmoa`
(`entry_number`,
`molecular_mechanism`,
`cellular_mechanism`,
`product_category`,
`therapeutic_group`,
`prescription/indication_type`,
`mechanism_of_action`,
`organization`,
`natural_source`,
`condition`,
`product_summary`,
`related_basic_patent`)
VALUES
(%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s);
'''

gConnPool = GHDDI208MysqlConnPool(10, "integrity")
gSuccessfulNum = 0
gFailedNum = 0


def dropAndCreateTable():
    db = gConnPool.getDhddiConn()
    cursor = db.cursor()
    cursor.execute('drop table if exists druginfo')
    cursor.execute('drop table if exists drugmoa')
    cursor.execute(CREATE_INFO_TABLE)
    cursor.execute(CREATE_MOA_TABLE)
    db.commit()
    cursor.close()
    db.close()


INSERT_MILESTORE = '''
INSERT INTO `integrity`.`drug_milestore`
(`entry_number`,
`milestore_date`,
`milestore`,
`condition`,
`notes`,
`organization`,
`country_region`)
VALUES
(%s, %s, %s, %s, %s, %s, %s);
'''


def dealMileStores(conn, cursor, trList, entry_number):
    insertList = []
    for msTr in trList[2:]:
        value = [entry_number]
        for child in msTr.children:
            if child != '\n':
                childText = child.text
                if childText is None:
                    childText = ''
                value.append(childText)
        fieldsNumber = 7
        arguNum = len(value)
        if arguNum == 2:
            print("ms 2 argu : ", value)
            continue
        if arguNum != fieldsNumber:
            for i in range(arguNum, fieldsNumber):
                value.append('')
            if len(value) != fieldsNumber:
                print(len(value))
        insertList.append(value)
    if len(insertList) > 0:
        # print(insertList)
        cursor.executemany(INSERT_MILESTORE, insertList)
        # conn.commit()


INSERT_DEVELOPSTATUS = '''
INSERT INTO `integrity`.`drug_development_status`
(`entrg_number`,
`status`,
`country_region`,
`phase`,
`organization`,
`brand_name`,
`condition`,
`indication`,
`admin_route`,
`formulation`)
VALUES
(%s, %s, %s, %s, %s, %s, %s, %s, %s, %s);
'''


def dealDevelopStatus(conn, cursor, tables, entry_number):
    insertList = []
    for dsTable in tables:
        dsTrList = dsTable.find_all('tr')
        dsTypeTr = dsTrList[0]
        dsType = list(dsTypeTr.children)[0].text
        for row in dsTrList[2:]:
            value = []
            value.append(entry_number)
            value.append(dsType)
            children = list(row.children)
            for child in children:
                if child != '\n':
                    childText = child.text
                    if childText is None:
                        childText = ''
                    value.append(child.text)
            arguNum = len(value)
            fieldsNumber = 10
            if arguNum != fieldsNumber:
                for i in range(arguNum, fieldsNumber):
                    value.append('')
                if len(value) != fieldsNumber:
                    print(len(value))
            insertList.append(value)
    if len(insertList) > 0:
        cursor.executemany(INSERT_DEVELOPSTATUS, insertList)
        # conn.commit()


def getInfoValue(childList, ghddi_entry_date):
    # drug info,数组下标:0:13 & -5: 共18项.加上ghddi_entry_date共19项
    # 前13
    value = []
    for child in childList[0:13]:
        if child == '\n':
            print(child)
        value.append(child.text)
    # -5:
    for child in childList[-5:-1]:
        value.append(child.text)
    # 读取structure image link
    structureChild = childList[-1]
    simg = structureChild.img
    if simg is not None:
        value.append(simg['src'])
    value.append(ghddi_entry_date)
    arguNum = len(value)
    # print(arguNum)
    fieldsNumber = 19
    if arguNum != fieldsNumber:
        for i in range(arguNum, fieldsNumber):
            value.append('')
        if len(value) != fieldsNumber:
            print(len(value))

    return value


def getMoaValue(entry_number, childList):
    # drugmoa,数组下标:13~22& 24~-6,共11项.还要加上entry_number,共12项
    value = [entry_number]
    # 去掉link to integrity
    for child in childList[13:22]:
        value.append(child.text)
    for child in childList[24:-6]:
        value.append(child.text)
    arguNum = len(value)
    # print(arguNum)
    fieldsNumber = 12
    if arguNum != fieldsNumber:
        for i in range(arguNum, fieldsNumber):
            value.append('')
        if len(value) != fieldsNumber:
            print(len(value))
    return value


def dealHtml(fileNames):
    conn = gConnPool.getDhddiConn()
    cursor = conn.cursor()
    try:
        sourceFile = fileNames[0]

        # print('{} ||| start---'.format(sourceFile))
        with open(sourceFile, 'r') as f:
            strings = ''.join(line for line in f).replace('<br>', ';')
            soup = BeautifulSoup(strings, features='lxml')
            trList = soup.find_all('table')[0].contents
            drugInfoInsertList = []
            drugMoaInsertList = []
            ghddi_entry_date = time.strftime('%Y-%m-%d', time.localtime(time.time()))
            for tr in trList[1:]:

                childList = list(tr.children)
                if childList[0] == '\n':
                    print(childList)
                entry_number = childList[0].text

                # druginfo
                # drugInfoValue = getInfoValue(childList, ghddi_entry_date)
                # drugInfoInsertList.append(drugInfoValue)
                # if len(drugInfoInsertList) == itemNumPerInsertAction:
                #     cursor.executemany(INSERT_INFO_TABLE, drugInfoInsertList)
                # drugInfoInsertList.clear()

                # Development Status 第23项
                developStatusTables = childList[22].find_all('table')
                if developStatusTables is not None:
                    dealDevelopStatus(conn, cursor, developStatusTables, entry_number)
                # milestores 第24项
                msTrList = childList[23].find_all('tr')
                if msTrList is not None and len(msTrList) != 0:
                    dealMileStores(conn, cursor, msTrList, entry_number)

                # drugmoa
                # drugMoaValue = getMoaValue(entry_number, childList)
                # drugMoaInsertList.append(drugMoaValue)
                # if len(drugMoaInsertList) == itemNumPerInsertAction:
                #     cursor.executemany(INSERT_MOA_TABLE, drugMoaInsertList)
                #     drugMoaInsertList.clear()

        # if len(drugInfoInsertList) > 0:
        #     cursor.executemany(INSERT_INFO_TABLE, drugInfoInsertList)
        #     # conn.commit()
        # if len(drugMoaInsertList) > 0:
        #     cursor.executemany(INSERT_MOA_TABLE, drugMoaInsertList)
        #     # conn.commit()
        conn.commit()
    except Exception as e:
        global gFailedNum
        gFailedNum += 1
        print(sourceFile, 'failed : ', e.__str__())
        raise e
    finally:
        cursor.close()
        conn.close()

    handledDir = fileNames[1]
    if not os.path.exists(handledDir):
        os.makedirs(handledDir)
    os.rename(sourceFile, os.path.join(handledDir, fileNames[2]))
    print(sourceFile, 'success')
    global gSuccessfulNum
    gSuccessfulNum += 1


if __name__ == '__main__':
    # 每次insert包含record的数量
    itemNumPerInsertAction = 200
    source_file_parent_dir = '/GHDDI/download/integrity/drugs'
    handled_file_parent_dir = '/GHDDI/download/integrity/handled/drugs'
    if not os.path.exists(handled_file_parent_dir):
        os.makedirs(handled_file_parent_dir)
    dirList = os.listdir(source_file_parent_dir)
    fileNameList = []
    for dirName in dirList:
        fileList = os.listdir(os.path.join(source_file_parent_dir, dirName))
        for fileName in fileList:
            fileNameList.append((os.path.join(source_file_parent_dir, dirName, fileName),
                                 os.path.join(handled_file_parent_dir, dirName), fileName))
    print("total file number : ", len(fileNameList))
    # with ThreadPoolExecutor(5) as exector:
    #     exector.map(dealHtml, fileNameList)
    for files in fileNameList:
        dealHtml(files)

    print("end. sucessful number : {} , failed number : {} ".format(gSuccessfulNum, gFailedNum))
