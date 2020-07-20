from bs4 import BeautifulSoup
import os
import sys
import time
import threading
import re
from concurrent.futures import ThreadPoolExecutor

base_path = os.path.dirname(os.path.abspath(__file__)) + '/../../../'  
sys.path.append(base_path)

from commonUtils.ghddiMysqlConnPool import GHDDI208MysqlConnPool

CREATE_TABLE_GENOMIC = '''
create table if not exists genomic(id INT,gene_symbol VARCHAR(256),gene_name VARCHAR(256),synonyms TEXT,sequence LONGTEXT,genomic_condition VARCHAR(2048),gene_description TEXT,
organism VARCHAR(128),links VARCHAR(256),protein_name VARCHAR(1024),protein_synonym TEXT,protein_links VARCHAR(128),protein_sequence TEXT,metacore VARCHAR(128),PRIMARY KEY(id))
'''
CREATE_TABLE_GENOMIC_GENE_VARIATION = '''
create table if not exists genomic_gene_variation(id INT NOT NULL AUTO_INCREMENT,gene_id INT, gen_variation_type VARCHAR(128),
gen_variation_name VARCHAR(128),gen_variation_condition VARCHAR(1024),PRIMARY KEY(id))'''

CREATE_TABLE_GENOMIC_RELATED_STUDIES = '''
create table if not exists genomic_related_studies(id INT NOT NULL AUTO_INCREMENT,gene_id INT,
genomic_base_studies_condition VARCHAR(128),
genomic_base_studies_role VARCHAR(128),genomic_base_studies_model VARCHAR(128), PRIMARY KEY(id))
'''

INSERT_TABLE_GENOMIC = '''
INSERT INTO genomic(id,gene_symbol,gene_name,synonyms,sequence,genomic_condition,gene_description,organism,links,protein_name,protein_synonym,protein_links,protein_sequence,metacore) 
VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s,%s, %s, %s, %s, %s)
'''

INSERT_TABLE_GENOMIC_GENE_VARIATION = '''
INSERT INTO genomic_gene_variation(gene_id, gen_variation_type,gen_variation_name,gen_variation_condition) 
VALUES (%s, %s, %s, %s)
'''

INSERT_TABLE_GENOMIC_GENE_RELATED_STUDIES = '''
INSERT INTO genomic_related_studies(gene_id, genomic_base_studies_condition,genomic_base_studies_role,genomic_base_studies_model) 
VALUES (%s, %s, %s, %s)
'''


class PrimaryIdSeed:
    __idSeed = 1
    __lock = threading.Lock()
    
    @classmethod
    def generateSeed(cls):
        cls.__lock.acquire()
        seed = cls.__idSeed
        cls.__idSeed += 1
        cls.__lock.release()
        return seed


gConnPool = GHDDI208MysqlConnPool(10, "integrity")


def createTables():
    db = gConnPool.getDhddiConn()
    cursor = db.cursor()
    cursor.execute('drop table if exists genomic')
    cursor.execute('drop table if exists genomic_gene_variation')
    cursor.execute('drop table if exists genomic_related_studies')
    cursor.execute(CREATE_TABLE_GENOMIC)
    cursor.execute(CREATE_TABLE_GENOMIC_GENE_VARIATION)
    cursor.execute(CREATE_TABLE_GENOMIC_RELATED_STUDIES)
    db.commit()
    cursor.close()
    db.close()


def fillListWithNullChar(unFilledList, expectedLen):
    oriLen = len(unFilledList)
    if expectedLen != oriLen:
        for i in range(oriLen, expectedLen):
            unFilledList.append('')
    return unFilledList


def getInsertData(childList):
    # 获取pid的seed
    pIdSeed = PrimaryIdSeed.generateSeed()
    # common columns 0:11, metacore:14
    commonDataList = [pIdSeed]
    commonDataList.extend([child.text for child in childList[0:12]])
    # common sequence
    sequence = commonDataList[4]
    commonDataList[4] = sequence.replace('\n',';')
    # common: metacore : get term=?&id=?
    metaCoreUrl = childList[14].a['href']
    ret = re.search(r'term=([0-9]*)&id=([0-9]*)', metaCoreUrl)
    if ret is not None:
        metaCore = 'term={}&id={}'.format(ret.groups()[0], ret.groups()[1])
    else:
        metaCore = ''
    commonDataList.append(metaCore)

    # gene variation 12
    varTrList = childList[12].find_all('tr')
    varDataList = None
    if varTrList is not None and len(varTrList) > 1:
        varDataList = []
        for varRow in varTrList[1:]:
            oneVar = [pIdSeed]
            oneVar.extend([varChild.text for varChild in varRow.children if varChild != '\n'])
            oneVar = fillListWithNullChar(oneVar, 4)
            varDataList.append(oneVar)

    # gene related study 13
    studyTrList = childList[13].find_all('tr')
    studyDataList = None    
    if studyTrList is not None and len(studyTrList) > 1:
        studyDataList = []
        for studyRow in studyTrList[1:]:
            studyOne = [pIdSeed]
            studyOne.extend([studyChild.text for studyChild in studyRow.children if studyChild != '\n'])
            studyOne = fillListWithNullChar(studyOne, 4)
            studyDataList.append(studyOne)

    return commonDataList, varDataList, studyDataList


gSuccessfulNum = 0
gFailedNum = 0


def dealHtml(fileNames):
       
    conn = gConnPool.getDhddiConn()
    cursor = conn.cursor()
    try:
        sourceFile = fileNames[0]
        # print('{} ||| start---'.format(sourceFile))
        with open(sourceFile, 'r', encoding="utf-8") as f:
            strings = ''.join(line for line in f).replace('<br />',';').replace('&#177;','±').replace('&#183;','·')
        soup = BeautifulSoup(strings, features='lxml')
        trList = soup.find_all('table')[0].contents
        entry_date = time.strftime('%Y-%m-%d', time.localtime(time.time()))
        commonInsertList = []
        varInsertList = []
        studyInsertList = []
        for row in trList[1:]:
            childList = [child for child in row.children if child.text != '\n']
            if len(childList) != 15:
                print(childList)
            commonDataValue, varDataList, studyDataList = getInsertData(childList)
            commonInsertList.append(commonDataValue)
            if varDataList is not None and len(varDataList) > 0:
                varInsertList.extend(varDataList)
            if studyDataList is not None and len(studyDataList) > 0:
                studyInsertList.extend(studyDataList)

            if len(commonInsertList) == 200:
                cursor.executemany(INSERT_TABLE_GENOMIC, commonInsertList)
                cursor.executemany(INSERT_TABLE_GENOMIC_GENE_VARIATION, varInsertList)
                cursor.executemany(INSERT_TABLE_GENOMIC_GENE_RELATED_STUDIES, studyInsertList)
                conn.commit()
                commonInsertList.clear()
                varInsertList.clear()
                studyInsertList.clear()

    except Exception as e:
        global gFailedNum
        gFailedNum += 1
        print(sourceFile, 'failed : ', e.__str__())
        return
    finally:
        cursor.close()
        conn.close()

    # handledDir = fileNames[1]
    # if not os.path.exists(handledDir):
    #     os.makedirs(handledDir)
    # os.rename(sourceFile, os.path.join(handledDir, fileNames[2]))
    print(sourceFile, 'success')
    global gSuccessfulNum
    gSuccessfulNum += 1


if __name__ == '__main__':
    # 文件中包含record的数量
    itemNumPerFile = 500
    source_file_parent_dir = '/GHDDI/download/integrity/genomics'
    handled_file_parent_dir = '/GHDDI/download/integrity/handled/genomics'
    if not os.path.exists(handled_file_parent_dir):
        os.makedirs(handled_file_parent_dir)
    dirList = os.listdir(source_file_parent_dir)
    fileNameList = []
    for dirName in dirList:
        fileList = os.listdir(os.path.join(source_file_parent_dir, dirName))
        for fileName in fileList:
            fileNameList.append((os.path.join(source_file_parent_dir, dirName, fileName),
                                 os.path.join(handled_file_parent_dir, dirName),  fileName))
    print("total file number : ", len(fileNameList))
    createTables()
    # for file in fileNameList:
    #     dealHtml(file)
    with ThreadPoolExecutor(10) as e:
        e.map(dealHtml, fileNameList)
    print("end. sucessful number : {} , failed number : {} ".format(gSuccessfulNum, gFailedNum) )