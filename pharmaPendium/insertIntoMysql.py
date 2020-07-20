import os
import pandas as pd
from commonUtils.ghddiMysqlConnPool import GHDDI208MysqlConnPool

g_mysqlConnPool = GHDDI208MysqlConnPool(4, 'pharmaPendium')


def formatCsvByPandas(file):
    dataDF = pd.read_csv(file, skiprows=4, skipinitialspace=True, skip_blank_lines=True)
    dataDF.to_csv(f'{file}_new', index=False)


if __name__ == '__main__':
    loadDataFromFile('D:\code\data_download_crawl\pharmaPendium\data\efficacy\Efficacy-human-clinical\Efficacy_Data_45.csv')
