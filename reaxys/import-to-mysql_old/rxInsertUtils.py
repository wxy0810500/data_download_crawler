import os
import xml.dom.minidom


class InsertFromXML:
    def __init__(self, targetName, idKey, basePath):
        self.targetName = targetName
        self.idKey = idKey
        self.basePath = basePath

    def printV(self):
        print(self.targetName, self.idKey, self.basePath)

    def getTree(self, fileName):
        with open(os.path.join('%s/%s' % (self.basePath, fileName)), 'r') as file:
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

    def deal(self, tId, node, keyList):
        dList = [tId]
        for key in keyList:
            dList.append(self._arrange(node.getElementsByTagName(key)))
        return dList

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
            res = self.deal(tId, node, keyList)
            insertJude = False
            for resValue in res[1:]:
                if resValue is not None:
                    insertJude = True
                    break
            if insertJude:
                insertDataList.append(res)
        return insertDataList

    def dealXMLByTree(self, domTree, insertSql, keyList, conn):
        tcursor = conn.cursor()
        collection = domTree.documentElement
        nodeList = collection.getElementsByTagName(self.targetName)
        insertDataList = []
        for node in nodeList:
            if len(node.getElementsByTagName(self.idKey)) == 0:
                continue
            tId = node.getElementsByTagName(self.idKey)[0].childNodes[0].nodeValue
            if tId is None:
                continue
            res = self.deal(tId, node, keyList)
            insertJude = False
            for resValue in res[1:]:
                if resValue is not None:
                    insertJude = True
                    break
            if insertJude:
                insertDataList.append(res)
        tcursor.executemany(insertSql, insertDataList)
        conn.commit()
        tcursor.close()
