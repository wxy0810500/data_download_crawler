from commonUtils.ghddiMysqlConnPool import getCHGGI208MysqlSingleConnection
import os
import argparse
import pandas as pd


def formatCsvByPandas(file, ignoreLines):
    # skiprows = ignoreLines - 1 保留header行
    dataDF = pd.read_csv(file, skiprows=ignoreLines - 1, skipinitialspace=True, skip_blank_lines=True)
    # 去掉header行，只要源数据
    dataDF.to_csv(file, index=False, header=False)


def importOneFile(conn, file, tableName, ignoreLines, fieldTerminator=',', lineTerminator='\n'):
    print(f'{file} start')
    try:
        formatCsvByPandas(file, ignoreLines)
        cursor = conn.cursor()
        cursor.execute(f'''LOAD DATA INFILE '{file}'
                        INTO TABLE {tableName}
                        FIELDS TERMINATED BY '{fieldTerminator}'
                        ENCLOSED BY '"'
                        LINES TERMINATED BY '{lineTerminator}';
                  ''')
        conn.commit()

        print(f'{file} finished')
        os.rename(file, f'{file}_done')
    except Exception as e:
        print(f'{file} failed')
        print(e)
    # finally:
    #     cursor.close()


def process(fileDirOn208, tableName, withHeaders: bool, fieldTerminator=',', lineTerminator='\n'):
    if withHeaders:
        ignoreLines = 5
    else:
        ignoreLines = 1
    # with Pool(4) as p:
    #     p.starmap(importOneFile,
    #               [(os.path.join(fileDirOn208, fileName), tableName, ignoreLines, fieldTerminator, lineTerminator)
    #                for fileName in os.listdir(fileDirOn208)])
    conn = getCHGGI208MysqlSingleConnection('pharmaPendium')

    for fileName in os.listdir(fileDirOn208):
        # formatCsvByPandas(os.path.join(fileDirOn208, fileName), ignoreLines)
        importOneFile(conn, os.path.join(fileDirOn208, fileName), tableName, ignoreLines, fieldTerminator,
                      lineTerminator)
    conn.close()


def analyseInvalidRecordLog(logFile):
    pass


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('-t', '--tableName', required=True, type=str)
    parser.add_argument('-d', '--fileDir', required=True, type=str)
    parser.add_argument('--withHeaders', required=True, type=bool, help="if the files have spare headers")
    parser.add_argument('-ft', '--fieldTerminator', default=',', type=str)
    parser.add_argument('-lt', '--lineTerminator', default='\n', type=str)
    args = parser.parse_args()
    process(args.fileDir, args.tableName, args.withHeaders, args.fieldTerminator, args.lineTerminator)
