import os
from lxml import etree
import sys

base_path = os.path.dirname(os.path.abspath(__file__)) + '/../'  
sys.path.append(base_path)

from commonUtils.ghddiMysqlConnPool import GHDDI208MysqlConnPool

DROP_TABLE_DRUG_BANK = '''
DROP TABLE IF EXISTS drug_bank;
'''

CREATE_TABLE_DRUG_BANK = '''
CREATE TABLE if not exists `drugbank`.`drug_bank` (
  `id` VARCHAR(45) NOT NULL,
  `extra_ids` VARCHAR(128) NULL,
  `name` VARCHAR(512) NULL,
  `description` VARCHAR(2048) NULL,
  `cas_number` VARCHAR(128) NULL,
  `groups` VARCHAR(256) NULL,
  `pubmed_id` VARCHAR(512) NULL,
  `indication` VARCHAR(1024) NULL,
  `pharmacodynamics` VARCHAR(1024) NULL,
  `mechanism_of_action` VARCHAR(1024) NULL,
  PRIMARY KEY (`id`))
ENGINE = InnoDB
DEFAULT CHARACTER SET = utf8mb4;
'''

g_connPool = GHDDI208MysqlConnPool(5, "drugbank")


def dropAndCreateTable():
    db = g_connPool.getDhddiConn()
    cursor = db.cursor()
    cursor.execute(DROP_TABLE_DRUG_BANK)
    db.commit()
    cursor.execute(CREATE_TABLE_DRUG_BANK)
    db.commit()
    cursor.close()
    db.close()


TRUNCATE_TABLE_SQL = " TRUNCATE `drugbank`.`drug_bank`;"


def truncateTable():
    db = g_connPool.getDhddiConn()
    cursor = db.cursor()
    cursor.execute(TRUNCATE_TABLE_SQL)
    db.commit()
    cursor.close()
    db.close()


INSERT_DRUG_BANK = '''
INSERT INTO `drugbank`.`drug_bank`
(`id`,
`extra_ids`,
`name`,
`description`,
`cas_number`,
`groups`,
`pubmed_id`,
`indication`,
`pharmacodynamics`,
`mechanism_of_action`)
VALUES
(%s, %s, %s, %s, %s, %s, %s, %s, %s, %s);
'''


def getSingleSubElemText(elem, xpathStr):
    subElems = elem.xpath(xpathStr)
    if len(subElems) >= 1:
        return subElems[0].text
    else:
        return ''


def getBatchSubElemTexts(elem, xpathStr, separator):
    subElems = elem.xpath(xpathStr)
    if len(subElems) > 0:
        return separator.join([subElem.text for subElem in subElems if subElem.text is not None])
    else:
        return ''


xpPId = r'./drugbank-id[@primary="true"]'
xpExtraIds = r'./drugbank-id[not(@primary)]'
xpName = r'./name'
xpDesc = r'./description'
xpGroups = r'./groups/group'
xpPubmedIds = r'./general-references//pubmed-id'
xpCasNumber = r'./cas-number'
xpIndication = r'./indication'
xpPharmacodynamics = r'./pharmacodynamics'
xpMechanism = r'./mechanism-of-action'


def handleOneDrugElem(drugElement):
    sqlParam = []
    # pid
    pId = getSingleSubElemText(drugElement, xpPId)
    sqlParam.append(pId)
    # extraIds
    extraIds = getBatchSubElemTexts(drugElement, xpExtraIds, ';')
    sqlParam.append(extraIds)
    # name
    name = getSingleSubElemText(drugElement, xpName)
    sqlParam.append(name)
    # description
    desc = getSingleSubElemText(drugElement, xpDesc)
    sqlParam.append(desc)
    # cas-number
    casNumber = getSingleSubElemText(drugElement, xpCasNumber)
    sqlParam.append(casNumber)
    # groups
    groups = getBatchSubElemTexts(drugElement, xpGroups, ';')
    sqlParam.append(groups)
    # pubmed-id
    pubmedIds = getBatchSubElemTexts(drugElement, xpPubmedIds, ';')
    sqlParam.append(pubmedIds)
    # indication
    indication = getSingleSubElemText(drugElement, xpIndication)
    sqlParam.append(indication)
    # pharmacodynamics
    pharmacodynamics = getSingleSubElemText(drugElement, xpPharmacodynamics)
    sqlParam.append(pharmacodynamics)
    # mechanism-of-action
    mechanism = getSingleSubElemText(drugElement, xpMechanism)
    sqlParam.append(mechanism)

    return sqlParam


def insertToDB(insertParams):
    global g_connPool
    conn = g_connPool.getDhddiConn()
    try:
        cursor = conn.cursor()
        cursor.executemany(INSERT_DRUG_BANK, insertParams)
        conn.commit()
    except Exception as e:
        print(e)
        raise e
    finally:
        cursor.close()
        conn.close()


def process(fileName):
    context = etree.iterparse(fileName, events=('end',), tag='drug')
    seq = 0
    number = 500
    count = 0
    insertParams = []
    for event, drugElem in context:
        # 找到带有type的drugNode
        if drugElem.get('type') is not None:
            sqlParam = handleOneDrugElem(drugElem)
            insertParams.append(sqlParam)
            count += 1
            seq += 1
            print(seq)
            if count == number:
                print("insert!")
                insertToDB(insertParams)
                insertParams.clear()
                count = 0
    if len(insertParams) > 0:
        insertToDB(insertParams)
    print("end")


if __name__ == '__main__':

    truncateTable()

    fileName = '/GHDDI/document/DrugbankDatabase.xml'
    process(fileName)

