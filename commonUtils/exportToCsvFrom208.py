import argparse
import mysql.connector as connector
import os
import re

DEFAULT_OUTPUT_FILE_PATH_ON208 = "/mysql/data-files/output/{}.csv"
EXPORT_SQL_FORMAT = "{} INTO OUTFILE '{}' FIELDS TERMINATED BY '{}' LINES TERMINATED BY '{}'"
DEFAULT_OUTPUT_FILE_NAME_FORMAT = "{}_{}"  # database_table
INSERT_FIELDS_CMD_FORMAT = "sed -i '1 i {}' {}"

UPLOAD_FILE_TO_CLUSTER_CMD_FORMAT = '''scp -i /root/.ssh/wangxinyu {} xwang@111.204.125.107:{}'''
CLUSTER_TARGET_DIR_FORMAT = '''/data01/ghddi_public/AIDD/AIDD_DATA/DataPlatform/uploadedFiles/{}/'''
MAKE_CLUSTER_TARGET_DIR_FORMAT = '''ssh -i /root/.ssh/wangxinyu xwang@111.204.125.107 mkdir -m 775 {}'''

SELECT_ALL_TABLES_SQL_FORMAT = '''
select TABLE_NAME from INFORMATION_SCHEMA.TABLES where TABLE_SCHEMA = "{}";
'''
SELECT_ALL_RECORDS_IN_TABLE_SQL_FORMAT = '''
select * from {}.{}
'''
SELECT_ALL_FIELDS_SQL_FORMAT = '''select GROUP_CONCAT(COLUMN_NAME order BY ORDINAL_POSITION SEPARATOR ',')
from INFORMATION_SCHEMA.COLUMNS
WHERE TABLE_NAME = "{}"
AND TABLE_SCHEMA = "{}"'''


def exportOneTable(conn, queryStr, database, table,
                   outputFileName, fieldsTerminated, linesTerminated, targetDir):

    outFile = DEFAULT_OUTPUT_FILE_PATH_ON208.format(outputFileName)
    if os.path.exists(outFile):
        os.remove(outFile)
    if queryStr is None:
        # export all records in table
        queryStr = SELECT_ALL_RECORDS_IN_TABLE_SQL_FORMAT.format(database, table)
    exportSql = EXPORT_SQL_FORMAT.format(queryStr, outFile,
                                         fieldsTerminated, linesTerminated)
    print(exportSql)
    cursor = conn.cursor()
    cursor.execute(exportSql)

    # 设置表头
    fieldsGroup = re.search(r'select [\s\S]+ from', exportSql, re.IGNORECASE)
    fieldsStr = fieldsGroup.group()[6:-4].strip()
    if fieldsStr == '*':
        # 导出整表的fields
        querySql = SELECT_ALL_FIELDS_SQL_FORMAT.format(table, database)
        cursor.execute(querySql)
        ret = cursor.fetchall()
        fieldsStr = ret[0][0]
        print(fieldsStr)
    cursor.close()
    fieldsList = fieldsStr.split(',')
    heads = fieldsTerminated.join(field for field in fieldsList)
    cmd = INSERT_FIELDS_CMD_FORMAT.format(heads, outFile)
    print(cmd)
    os.system(cmd)
    # 上传文件
    targetDir = CLUSTER_TARGET_DIR_FORMAT.format(targetDir)
    cmd = MAKE_CLUSTER_TARGET_DIR_FORMAT.format(targetDir)
    os.system(cmd)
    cmd = UPLOAD_FILE_TO_CLUSTER_CMD_FORMAT.format(outFile, targetDir)
    os.system(cmd)
    print(cmd)


def doExport(inputArgs):
    conn = connector.connect(
        host="172.17.10.208",
        user="root",
        passwd="123456",
        database=inputArgs.database,
        auth_plugin='mysql_native_password',
        use_pure=True
    )

    if inputArgs.table is not None:
        # export one table
        if inputArgs.outputFileName is None:
            outputFileName = DEFAULT_OUTPUT_FILE_NAME_FORMAT.format(inputArgs.database, inputArgs.table)
        else:
            outputFileName = inputArgs.outputFileName
        exportOneTable(conn, inputArgs.queryStr, inputArgs.database, inputArgs.table, outputFileName,
                       inputArgs.fieldsTerminated, inputArgs.linesTerminated, inputArgs.targetDir)
    else:
        # export all tables in one schema
        cursor = conn.cursor()
        database = inputArgs.database
        getAllTablesSql = SELECT_ALL_TABLES_SQL_FORMAT.format(database)
        cursor.execute(getAllTablesSql)
        ret = cursor.fetchall()
        tables = [t[0] for t in ret]
        cursor.close()
        for table in tables:
            outputFileName = DEFAULT_OUTPUT_FILE_NAME_FORMAT.format(database, table)
            exportOneTable(conn, None, database, table, outputFileName,
                           inputArgs.fieldsTerminated, inputArgs.linesTerminated, inputArgs.targetDir)
    conn.close()


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument('-q', '--queryStr', default=None)
    parser.add_argument('-d', '--database', required=True)
    parser.add_argument('-t', '--table', default=None)
    parser.add_argument('-o', '--outputFileName', default=None)
    parser.add_argument('-ft', '--fieldsTerminated', default="\t")
    parser.add_argument('-lt', '--linesTerminated', default='\n')
    parser.add_argument('-targetDir', '--targetDir', default='')

    args = parser.parse_args()
    doExport(args)
