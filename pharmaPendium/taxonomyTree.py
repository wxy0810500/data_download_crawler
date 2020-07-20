"""
获取所有分类条件的tree
url = https://www.pharmapendium.com/services/{datasource}/getChildren?id={id}&noCache={timestamp-ms}&taxonomy={taxonomy}
level 1: id = 0
respJson:
"""
import pandas as pd
from .sourceCfg import Source
from time import time
from .utils import secSleep, getCookie, getUserAgent
import requests
import os


def getTimestamp():
    return int(round(time() * 1000))


class TaxonomyTree:

    _urlModel = 'https://www.pharmapendium.com/services/{}/' \
                'getChildren?id={}&noCache={}&taxonomy={}'

    def __init__(self, dataSource: Source):
        self._dataSource = dataSource

    def _getChildren(self, pId: str, pDeep: int, taxonomy: str, cookies) -> pd.DataFrame:
        url = self._urlModel.format(self._dataSource.value, pId, getTimestamp(), taxonomy)
        resp = requests.get(url, cookies=cookies)
        if resp.status_code != 200:
            print(f'{resp.status_code} : {url}')
            return None

        jsonDF = pd.read_json(resp.content.decode())

        nodesDF = pd.DataFrame(jsonDF['payload'].array)
        nodesDF['pId'] = pId
        nodesDF['deep'] = pDeep + 1
        return nodesDF

    def _getTree(self, taxonomy: str):
        print(f"get taxonomy tree : {taxonomy}")
        cookies = getCookie()
        # level1
        deep = 0
        nodesDF = self._getChildren("0", deep, taxonomy, cookies)
        pNodeStack = [node for node in nodesDF.itertuples(index=False) if getattr(node, 'isLeaf') is False]
        while len(pNodeStack) > 0:
            pNode = pNodeStack.pop()
            pId = getattr(pNode, 'id')
            pDeep = getattr(pNode, 'deep')
            subNodesDF = self._getChildren(pId, pDeep, taxonomy, cookies)
            pNodeStack += [node for node in subNodesDF.itertuples(index=False) if getattr(node, 'isLeaf') is False]
            secSleep(1)
            nodesDF = nodesDF.append(subNodesDF, ignore_index=True)
        return nodesDF

    def getTreeToCsv(self, taxonomy: str, csvFilePath: str, fieldSeparator: str = '\t', lineTerminator: str = '\n'):
        nodesDF = self._getTree(taxonomy)

        outCsvFile = f'{csvFilePath}/{self._dataSource.value.title()}-{taxonomy}.csv'
        if os.path.exists(csvFilePath) is False:
            os.makedirs(csvFilePath)
        nodesDF.to_csv(outCsvFile, sep=fieldSeparator, line_terminator=lineTerminator, index=False)

        return outCsvFile
    
    def getTreeToCsvLarge(self, taxonomy: str, csvFilePath: str, fieldSeparator: str = '\t', lineTerminator: str = '\n'):
        print(f"get taxonomy tree : {taxonomy}")
        cookies = getCookie()
        # level1
        deep = 0
        nodesDF = self._getChildren("0", deep, taxonomy, cookies)
        if nodesDF is None:
            return
        pNodeStack = [node for node in nodesDF.itertuples(index=False) if getattr(node, 'isLeaf') is False]

        if os.path.exists(csvFilePath) is False:
            os.makedirs(csvFilePath)
        outCsvFile = f'{csvFilePath}/{self._dataSource.value.title()}-{taxonomy}.csv'

        with open(outCsvFile, 'a') as f:
            while len(pNodeStack) > 0:
                pNode = pNodeStack.pop()
                pId = getattr(pNode, 'id')
                pDeep = getattr(pNode, 'deep')
                subNodesDF = self._getChildren(pId, pDeep, taxonomy, cookies)
                pNodeStack += [node for node in subNodesDF.itertuples(index=False) if getattr(node, 'isLeaf') is False]
                secSleep(1)
                nodesDF = nodesDF.append(subNodesDF, ignore_index=True)
                if nodesDF.size >= 1000:
                    nodesDF.to_csv(f, sep='\t', line_terminator='\n', index=False)
                    nodesDF = nodesDF.iloc[0:0]
            nodesDF.to_csv(f, sep=fieldSeparator, line_terminator=lineTerminator, index=False)
        return outCsvFile
