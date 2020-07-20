import os
import xml.dom.minidom
import csv


class InsertFromXML:
    def __init__(self, targetName, idKey, basePath, saveToCsv: bool = False):
        self.targetName = targetName
        self.idKey = idKey
        self.basePath = basePath
        self.saveToCsv = saveToCsv

    def printV(self):
        print(self.targetName, self.idKey, self.basePath)

    def getTree(self, fileName):
        with open(os.path.join(f'{self.basePath}/{fileName}'), 'r') as file:
            fileLine = ''.join(line for line in file)
            if fileLine.count('<hi>') != fileLine.count('</hi>'):
                return
            fileLine = fileLine.replace('<hi>', '').replace('</hi>', '')

            domTree = xml.dom.minidom.parseString(fileLine)
            return domTree

    @classmethod
    def _arrange(cls, nodeList):
        if nodeList is None or len(nodeList) == 0:
            return None
        strValue = ''
        for node in nodeList:
            if node.childNodes[0].nodeValue is not None:
                strValue += node.childNodes[0].nodeValue + ';'
        if len(strValue) > 0:
            strValue = strValue[:-1]
        return strValue

    def joinNodeData(self, tId, node, keyList):
        nodeData = [tId]
        for key in keyList:
            nodeData.append(self._arrange(node.getElementsByTagName(key)))
        return nodeData

    def getInsertDataListFromXMLTree(self, domTree, keyList):
        collection = domTree.documentElement
        nodeList = collection.getElementsByTagName(self.targetName)
        insertDataList = []
        for node in nodeList:
            if len(node.getElementsByTagName(self.idKey)) == 0:
                continue
            tId = node.getElementsByTagName(self.idKey)[0].childNodes[0].nodeValue
            if tId is None:
                continue
            nodeData = self.joinNodeData(tId, node, keyList)
            insertJude = False
            for resValue in nodeData[1:]:
                if resValue is not None:
                    insertJude = True
                    break
            if insertJude:
                insertDataList.append(nodeData)
        return insertDataList

    def insertFromXMLTree(self, fileName: str, insertSqlFormat: str, keyList, conn):
        domTree = self.getTree(fileName)
        if domTree is None:
            raise Exception(f"get dom tree failed, fileName:{fileName}")
        insertDataList = self.getInsertDataListFromXMLTree(domTree, keyList)
        if len(insertDataList) > 0:
            if self.saveToCsv:
                with open(f'{self.basePath}/{fileName.rstrip(".xml")}.csv', 'w') as f:
                    f_csv = csv.writer(f)
                    f_csv.writerows(insertDataList)
            else:
                cursor = conn.cursor()
                cursor.executemany(insertSqlFormat, insertDataList)
                conn.commit()
                cursor.close()
