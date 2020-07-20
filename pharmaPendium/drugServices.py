import pandas as pd
from .sourceCfg import Source
import requests
from typing import List
import json


class DrugService:

    def __init__(self, source: Source):
        self._dataSource = source
        self._source: str = source.value
        self._getByTargetUrl = f'https://www.pharmapendium.com/services/{self._source}/getDrugsByTargets'
        self._getByIndicationUrl = f'https://www.pharmapendium.com/services/{self._source}/getDrugsByIndications'

    def _getLeafDrugsByTarget(self, targetIds: List):
        r = requests.post(self._getByTargetUrl, json={'targetIDs': {'initial': targetIds}})
        bodyStr = r.content.decode()
        body = json.loads(bodyStr)
        if body['status'] != 'OK':
            raise Exception(f"get drugs by targets failed : {body['status']},{body.get('message', 'error')}")
        drugs: List = body['payload']['children']
        leafDrugList = []
        while len(drugs) > 0:
            drug = drugs.pop()
            dData = drug['data']
            if dData['isLeaf']:
                # 返回叶节点drug对应的所有targetId。后续处理时，要针对drugId-targetId来去重
                dId = dData['id']
                leafDrugList += [(dId, tId) for tId in dData['targetIDs']]
            else:
                subNodes = drug['children']
                if subNodes is not None and len(subNodes) > 0:
                    drugs += subNodes
        return leafDrugList

    def getLeafDrugsByTargetToCsv(self, targetIds: List[str], outputFilePath: str):

        if len(targetIds) > 100:
            targetIdGroups = [targetIds[i: i + 100] for i in range(0, len(targetIds), 100)]
        else:
            targetIdGroups = [targetIds]
        allDF = pd.DataFrame(columns=['drugId', 'targetId'])
        for idGroup in targetIdGroups:
            drugList = self._getLeafDrugsByTarget(idGroup)
            if len(drugList) > 0:
                df = pd.DataFrame(drugList, columns=['drugId', 'targetId'])
                allDF = allDF.append(df, ignore_index=True)

        # 根据drugId-targetId去重
        allDF.drop_duplicates(inplace=True)
        outFile = f'{outputFilePath}/{self._source.title()}-drug&target.csv'
        allDF.to_csv(outFile, sep='\t', header=True, line_terminator='\n', index=False, columns=['drugId', 'targetId'])

    def _getLeafDrugsByIndication(self, indicationIds: List):
        r = requests.post(self._getByIndicationUrl, json={"indicationIDs": {"initial": indicationIds},
                                                          "sourcesOr": {
                                                              "initial": ["FDA Label", "EMA ANNEX", "FDA Classic",
                                                                          "Efficacy (FDA)", "Efficacy (EMA)",
                                                                          "DESI",
                                                                          "MOSBY'S", "Meyler"]}})
        bodyStr = r.content.decode()
        body = json.loads(bodyStr)
        if body['status'] != 'OK':
            raise Exception(f"get drugs by indications failed : {body['status']},{body.get('message', 'error')}")
        drugs: List = body['payload']['children']
        leafDrugList = []
        while len(drugs) > 0:
            drug = drugs.pop()
            dData = drug['data']
            if dData['isLeaf']:
                # 返回叶节点drug对应的所有indicationId。后续处理时，要针对drugId-indicationId来去重
                dId = dData['id']
                leafDrugList += [(dId, tId) for tId in dData['indicationIDs']]
            else:
                subNodes = drug['children']
                if subNodes is not None and len(subNodes) > 0:
                    drugs += subNodes
        return leafDrugList

    def getLeafDrugsByIndicationToCsv(self, indicationIds: List[str], outputFilePath: str):

        if len(indicationIds) > 100:
            indicationIdGroups = [indicationIds[i: i + 100] for i in range(0, len(indicationIds), 100)]
        else:
            indicationIdGroups = [indicationIds]
        allDF = pd.DataFrame(columns=['drugId', 'indicationId'])
        for idGroup in indicationIdGroups:
            drugList = self._getLeafDrugsByIndication(idGroup)
            if len(drugList) > 0:
                df = pd.DataFrame(drugList, columns=['drugId', 'indicationId'])
                allDF = allDF.append(df, ignore_index=True)

        # 根据drugId-indicationId去重
        allDF.drop_duplicates(inplace=True)
        outFile = f'{outputFilePath}/{self._source.title()}-drug&indication.csv'
        allDF.to_csv(outFile, sep='\t', header=True, line_terminator='\n', index=False,
                     columns=['drugId', 'indicationId'])
