from selenium import webdriver  
import os
import time
import re
from selenium.webdriver.common.action_chains import ActionChains
import random
import sys

#设置递归深度
sys.setrecursionlimit(10000000)
# 树节点图标:
# 非叶子节点,图标为<img valign="center" src="/integrity/img/treeminus.gif" border="0">
# 叶子节点,图标为<img src="/integrity/img/treeend.gif" border="0">

# 每一组可点击的分类由4个element组成
# 1. 空格.<img src="/integrity/img/en/vspacer_en.gif" width="40" height="5" border="0">
# 2. 可点击展开的树节点点图标,带有超链接.
#         <a href="pk_hierar_tree.hierarchyTreeEATarget?p_parent_id=1&p_action=click&p_element=p_val_EXP1#sn">
#             <img valign="center" src="/integrity/img/treeminus.gif" border="0">
#         </a>
# 3. 该节点的str value
#         <a href="javascript:sel_llista_valors(&quot;Adaptor Proteins&quot;,'p_val_EXP1');" class="opened">Adaptor Proteins</a>
# 4. 换行L <br>

# ** 每次被展开的位置都会有<a name="sn"></a>,可通过这个标签找到本次解析开始的位置.

xpath_sub_a_format = '//a[@name="sn"]/following-sibling::a[%d]'
xpath_sub_node_img_format = '//a[@name="sn"]/following-sibling::a[%d]/img'
xpath_firstLevel_node_img_format = '//a[%d]/img'
xpath_firstLevel_node_a_format = '//a[%d]'

class TreeNode:
    def __init__(self, link, name, pid, deep):
        self.link = link
        self.name = name
        self.id = int(re.search(r'(\d+)', str(re.search(r'p_parent_id=(\d+)&', link).group())).group())
        self.pid = pid
        self.deep = deep

    def print(self):
        print('id : %s, name : %s, pid : %s, deep : %s' % (self.id, self.name, self.pid, self.deep))
    
    def outputCsv(self):
        return '{};{};{};{}\n'.format(self.id, self.name,self.pid,  self.deep)


#随机等待时间
def sleep (sec):
  time.sleep(sec * (1 + random.random()))

#引入chromedriver.exe
# chromedriver = 'C:/Program Files (x86)/Google/Chrome/Application/chromedriver.exe'
chromedriver = '/usr/bin/chromedriver'
os.environ['webdriver.chrome.driver'] = chromedriver

# 初始化根节点
rootNode = TreeNode('https://integrity.clarivate.com/integrity/xmlxsl/pk_hierar_tree.hierarchyTreeEACondition?p_parent_id=0&p_action=click&p_element=p_val_EXP1#sn', 'THERAPEUTIC ACTIVITIES', 0, 0)

# 初始化父节点栈
parentNodeStack = []
parentNodeStack.append(rootNode)

# 初始化节点dict
nodeDict = {0:rootNode}

def getFollowingLevelInfo(browser, parentNode):
    browser.get(parentNode.link)
    sleep(2)
    i = 1
    deep = parentNode.deep + 1
    if parentNode.id > 0:
        nodeImgXpath = xpath_sub_node_img_format
        nodeAXpath = xpath_sub_a_format
    else:
        nodeImgXpath = xpath_firstLevel_node_img_format
        nodeAXpath = xpath_firstLevel_node_a_format
    while 1:
        try:
            treeNodeimg = browser.find_element_by_xpath(nodeImgXpath % (1 + 2* i))
        except:
            #找到html最后一个节点之后,再找不到类似节点,则退出循环
            i -= 1
            break
        aTreeNode = browser.find_element_by_xpath(nodeAXpath % (1 + 2* i))
        aStrValue = browser.find_element_by_xpath(nodeAXpath % (2 + 2 * i))
        node = TreeNode(aTreeNode.get_attribute('href'), aStrValue.text, parentNode.id, deep)
        node.print()
        # 若该节点不是end节点,则将其添加到stack中
        if treeNodeimg.get_attribute('src') == 'https://integrity.clarivate.com/integrity/img/treeplus.gif':
            parentNodeStack.append(node)

        # 若该节点不在dict中,则加进去
        if nodeDict.get(node.id, None) == None :
            nodeDict[node.id] = node
        else:
            # 若该节点已经在dict中,则后续节点不用继续遍历
            break
        i += 1
    

if __name__ == "__main__":
    browser = webdriver.Chrome(chromedriver)
    urlEntry = 'https://www.google.com'
    ck1 = {'name': 'igr_user', 'value': '1649022361M460140%|ITGR|', 'domain': '.clarivate.com',
           'path': '/integrity/xmlxsl'}
    browser.get(urlEntry)
    browser.add_cookie(ck1)

    while len(parentNodeStack) != 0:
        pNode = parentNodeStack.pop()
        getFollowingLevelInfo(browser, pNode)
    
    if len(nodeDict) != 0:
        filePath = '/home/ubuntu/GHDDI/download/integrity/experimentalActivitesConditionTree.csv'
        if os.path.exists(filePath):
            os.remove(filePath)
        with open(filePath, 'x') as outputFile:
            for node in nodeDict.values():
                outputFile.write(node.outputCsv())
    browser.quit()
