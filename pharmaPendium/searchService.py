import pandas as pd
from typing import List, Dict, Tuple
from pharmaPendium.sourceCfg import Source
import requests
import json
from multiprocessing import Pool, cpu_count
from .utils import secSleep
import random
import string


class SearchService:
    _countLimit = 50000
    _cookie = {"JSESSIONID": "53246B38778CBA4201042971647AD70A",
               "AWSELB": "A5119D3F0ECC2D7E1F13E8D01FEACD8F72C399016774DC6AD4842B2336EE933321FD8813F4707150CE72113"
                         "FE2C1FD5A003A1841CB8E0F432E505CB29E6678114EB3D3258B270A42D166A25D4D374DA39178015951",
               "previousSessionFlag": "previousSessionFlag",
               "_ga":"GA1.2.331013834.1587106806"}

    def __init__(self, source: Source):
        self._dataSource = source
        self._source: str = source.value
        self._getHeadersUrl = f'https://www.pharmapendium.com/services/{self._source}/header'
        self._exportUrl = f'https://www.pharmapendium.com/services/{self._source}/export'
        self._searchUrl = f'https://www.pharmapendium.com/services/{self._source}/search'
        # self._cookie = getCookie()

    def _getHeaders(self):
        jsonDF = pd.read_json(self._getHeadersUrl)
        return pd.DataFrame(jsonDF['payload'].array)

    def getColumns(self):
        headersDF = self._getHeaders()
        columns = headersDF['name'].to_list()
        print(f'{self._dataSource} columns : {columns}')
        return columns

    def _getSimpleFilterItems(self, queryConditions: Dict, urlPath: str) -> Tuple:
        url = f'https://www.pharmapendium.com/services/{self._source}/{urlPath}'
        r = requests.post(url, json=queryConditions)
        rBody: Dict = json.loads(r.content.decode())

        infos: List = rBody['payload']['children']
        validOnes = []
        exceedOnes = []
        for info in infos:
            data = info['data']
            if data['count'] > self._countLimit:
                exceedOnes.append(data)
            else:
                validOnes.append(data)

        return validOnes, exceedOnes

    def getFilterSeries(self, queryConditions: Dict) -> Tuple:
        return self._getSimpleFilterItems(queryConditions, 'getFilterSerious')

    def getFilterPlacebo(self, queryConditions: Dict) -> Tuple:
        return self._getSimpleFilterItems(queryConditions, 'getFilterPlacebo')

    def getFilterYears(self, queryCondition: Dict) -> Tuple:
        url = f'https://www.pharmapendium.com/services/{self._source}/getFilterYears'
        r = requests.post(url, json=queryCondition)
        rBody: Dict = json.loads(r.content.decode())

        years: List = rBody['payload']['children']
        years.reverse()
        totalCount = 0
        yearNames = []
        validGroups = []
        exceedYears = []
        while len(years) > 0:
            year = years.pop()
            yData = year['data']
            if yData.get('isLeaf', True) is True:
                # 叶节点进行totalcount 分组
                count = yData['count']
                if count > self._countLimit:
                    exceedYears.append(yData)
                else:
                    if (totalCount + count) > self._countLimit:
                        validGroups.append({'count': totalCount, 'data': yearNames})
                        yearNames = []
                        totalCount = 0
                    totalCount += count
                    yearNames.append(yData['name'])
            else:
                subYears: List = year.get('children', None)
                if subYears is not None:
                    subYears.reverse()
                    years += subYears

        if totalCount != 0:
            validGroups.append({'count': totalCount, 'data': yearNames})
        return validGroups, exceedYears

    def getFilterDataProvider(self, queryCond: Dict) -> Tuple:
        url = f'https://www.pharmapendium.com/services/{self._source}/getFilterDataProviders'
        r = requests.post(url, json=queryCond)
        rBody: Dict = json.loads(r.content.decode())

        infos: List = rBody['payload']['children']
        validOnes = []
        exceedOnes = []
        totalCount = 0
        data = []
        for info in infos:
            pdata = info['data']
            count = pdata['count']
            if pdata['count'] > self._countLimit:
                exceedOnes.append(pdata)
            else:
                if (totalCount + count) > self._countLimit:
                    validOnes.append({'count': totalCount, 'data': data})
                    data = []
                    totalCount = 0
                totalCount += count
                data.append(pdata['name'])
        if totalCount != 0:
            validOnes.append({'count': totalCount, 'data': data})
        return validOnes, exceedOnes

    def _lookupFiltersOfOneLevel(self, taxInfos: List, parentNodesPath: List):
        validGroups = []
        exceedOnes = []
        currentTotalCount = 0
        tDataGroup = []

        for taxInfo in taxInfos:
            tData = taxInfo['data']
            count = tData['count']
            if count <= self._countLimit:
                if currentTotalCount + count > self._countLimit:
                    validGroups.append({
                        'count': currentTotalCount,
                        'parentNodesPath': parentNodesPath,
                        'dataGroup': tDataGroup,
                    })
                    currentTotalCount = 0
                    tDataGroup = []
                currentTotalCount += count
                tDataGroup.append(tData['name'])
            else:
                if tData['isLeaf']:
                    exceedOnes.append({
                        'data': tData,
                        'parentNodesPath': parentNodesPath,
                    })
                else:
                    subNodes = taxInfo['children']
                    if subNodes is not None and len(subNodes) > 0:
                        nextParentNodesPath = parentNodesPath.copy()
                        nextParentNodesPath.append(tData['name'])
                        subValidGroups, subExceedOnes = self._lookupFiltersOfOneLevel(subNodes, nextParentNodesPath)
                        if len(subExceedOnes) > 0:
                            exceedOnes += subExceedOnes
                        if len(subValidGroups) > 0:
                            validGroups += subValidGroups
        if currentTotalCount > 0 and len(tDataGroup) > 0:
            validGroups.append({
                'count': currentTotalCount,
                'parentNodesPath': parentNodesPath,
                'dataGroup': tDataGroup,
            })
        return validGroups, exceedOnes

    def lookupFilters(self, queryCondition: Dict, taxonomy: str) -> Tuple:
        """

        :param queryCondition:
        :param taxonomy:
        :return:validOnes :[
                    {
                        "count":10000,
                        "dataGroup":[
                            {
                                "id":
                                "count":
                                "isLeaf":
                                "name"
                                "parentDataList"：[{the same as parent dict structure}]

                            }
                        ]
                    }
                ]
        """
        url = f'https://www.pharmapendium.com/services/{self._source}/lookupFiltered?taxonomy={taxonomy}'
        r = requests.post(url, json=queryCondition)
        body = json.loads(r.content.decode())

        taxInfos: List = body['payload']['children']
        return self._lookupFiltersOfOneLevel(taxInfos, [])

    def export(self, requestJsonBody: Dict, downloadFile: str, fileModel: str = 'wb') -> str:
        cookie = {"JSESSIONID": "ACC9E9E46137C9282B7FFDF9BA3C75A4",
                  "AWSELB": "A5119D3F0ECC2D7E1F13E8D01FEACD8F72C399016774DC6AD4842B2336EE933321FD8813F4707150CE72113"
                            "FE2C1FD5A003A1841CB8E0F432E505CB29E6678114EB3D3258B270A42D166A25D4D374DA39178015951",
                  "previousSessionFlag": "previousSessionFlag"}

        r = requests.post(self._exportUrl, json=requestJsonBody, cookies=cookie)
        rBody = json.loads(r.content.decode())
        status: str = rBody['status']
        if 'OK' == status:
            # status ok, download tsv
            dUrl = f'https://www.pharmapendium.com/services/download/tsv?hash={rBody["payload"]}'
            dr = requests.get(dUrl, cookies=cookie)
            with open(downloadFile, fileModel) as f:
                for chunk in dr.iter_content(chunk_size=1024):
                    if chunk:
                        f.write(chunk)
        return status

    def searchToCsv(self, requestJsonBody: Dict, outFilePath: str, fileNameSufix: str, firstRow: int = 0, totalCount=0):
        print(fileNameSufix)

        # get totalCount
        if totalCount == 0:
            totalCount = self._searchTotalCount(requestJsonBody)
        seq = firstRow // 2000 + 1
        self._searchToCsvTask(requestJsonBody, firstRow, totalCount, seq, outFilePath, fileNameSufix)

    def searchToCsv_multiprocess(self, requestJsonBody: Dict, outFilePath: str, fileNameSufix: str, firstRow: int = 0):
        print(fileNameSufix)

        # get totalCount
        totalCount = self._searchTotalCount(requestJsonBody)
        seq = firstRow // 2000 + 1
        count = 2000
        if (totalCount - firstRow) // count <= 100:
            self._searchToCsvTask(requestJsonBody, firstRow, totalCount, seq, outFilePath, fileNameSufix)
        else:
            # 设置多进程为每个进程分配firstRow和其需要完成的totalCount
            poolSize = max(4, cpu_count())
            step = (totalCount - firstRow) // count // poolSize * count
            args = [(requestJsonBody, i, i + step, seq, outFilePath, fileNameSufix)
                    for i in range(firstRow, totalCount, step)]
            with Pool(poolSize) as p:
                p.starmap(self._searchToCsvTask, args)

    def _searchTotalCount(self, requestJsonBody: Dict):
        # get totalCount
        requestJsonBody["limitation"] = {
            "firstRow": 0,
            "count": 1
        }

        r = requests.post(self._searchUrl, json=requestJsonBody, cookies=self._cookie)
        df = pd.read_json(r.content.decode())
        return df['payload']['countTotal']

    def _searchToCsvTask(self, requestJsonBody: Dict, firstRow, totalCount, seq, outFilePath: str, fileNameSufix: str):
        count = 2000
        rStr = ran_str = ''.join(random.sample(string.ascii_letters + string.digits, 6))
        print(f"start one search to csv task, random str : {rStr} , firstRow:{firstRow}, totalCount:{totalCount}")
        while firstRow < totalCount:
            if seq % 10 == 0:
                secSleep(2)
            if seq % 100 == 0:
                secSleep(5)
            requestJsonBody["limitation"] = {
                "firstRow": firstRow,
                "count": count
            }
            r = requests.post(self._searchUrl, json=requestJsonBody, cookies=self._cookie)
            df = pd.read_json(r.content.decode())

            itemsDF: pd.DataFrame = pd.DataFrame(df['payload']['items'])

            itemsDF.to_csv(f'{outFilePath}/{self._source}-{fileNameSufix}-{firstRow}.csv', index=False, sep='\t')
            print(f"random str : {rStr}, totalCount:{totalCount}, firstRow:{firstRow} finished")
            seq += 1
            firstRow += count
