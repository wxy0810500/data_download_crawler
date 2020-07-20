#根据target页左栏的疾病类型下载对应的图片及其targetlist

from selenium import webdriver
import os
import time
import re
from selenium.webdriver.common.action_chains import ActionChains
from selenium.webdriver.support.wait import WebDriverWait
from selenium.common.exceptions import NoSuchElementException
from selenium.webdriver.support import expected_conditions as EC
import random
import requests
from io import BytesIO
from PIL import Image
# 随机等待时间
def sleep(sec):
    time.sleep(sec * (1 + random.random()))

# 引入chromedriver.exe
# chromedriver = 'C:/Program Files (x86)/Google/Chrome/Application/chromedriver.exe'
chromedriver = '/usr/bin/chromedriver'
os.environ['webdriver.chrome.driver'] = chromedriver

downloadFilePath = '/home/ubuntu/GHDDI/download/integrity/targetscapes'

# 左栏一级节点xpath,n从1开始
firstLevelNodePath = '//form[@id="search_form"]//table/following::td[@width=194][{}]//a' #n
# 一级节点展开后,其下面二级节点的xpath,n同上,m从1开始
subLevelNodePath = '//form[@id="search_form"]//table/following::td[@width=194][{}]//a/following::a[@class = "tipicalink"][{}]'#n,m

def getSubLevelIdsOfNextFirstLevelNode(optionBrowser, n):

    try:
        firstNode = optionBrowser.find_element_by_xpath(firstLevelNodePath.format(n))
    except:
        return None
    sleep(2)
    firstName = firstNode.text
    firstNode.click()
    landscapeNodes = {}
    m = 1
    while 1:
        try:
            subNode = optionBrowser.find_element_by_xpath(subLevelNodePath.format(n, m))
            linkStr = subNode.get_attribute('onclick')
            subId = int(re.search(r'\d+', linkStr).group())
            subName = '{}-{}'.format(firstName, subNode.text.replace('/', " or "))
            landscapeNodes[subId] = subName
            m += 1 
        except:
            break

    return landscapeNodes

dataUrlModel = 'https://integrity.clarivate.com/integrity/xmlxsl/TARGET_LANDSCAPES_PKG.getMainLandscape_short?p_landscapeId={}'
def getDatas(dataBrowser, landscapeNodes):
    for subId, subName in landscapeNodes.items():
        dataBrowser.get(dataUrlModel.format(subId))
        dataBrowser.implicitly_wait(30)
        sleep(2)
        # 解析图片url
        print(subId, subName)
        bgElement = dataBrowser.find_element_by_xpath('//div[@id="container"]')
        bgNames = re.search(r'/\w*.jpg', bgElement.get_attribute('style'))
        if bgNames is not None:
            bgName = bgNames.group()
            bgImgUrl = 'https://integrity.clarivate.com/integrity/edcontent/backgrounders{}'.format(bgName)
            #下载图片
            imgFile = '{}/{}.jpg'.format(downloadFilePath, subName)

            r = requests.get(bgImgUrl)
            if os.path.exists(imgFile):
                os.remove(imgFile)
            img = Image.open(BytesIO(r.content))
            img.save(imgFile)

        else:
            continue
        # 解析targetlist
        try :
            # 点击targetlist
            dataBrowser.find_element_by_xpath('//span[@class = "ui-selectmenu-text"]').click()
            # 从后续元素中获取target list value
            targetlistElements = dataBrowser.find_elements_by_xpath("//div[@class = 'ui-menu-item-wrapper']")
            tlistFile = '{}/{}-tlist'.format(downloadFilePath, subName)
            if os.path.exists(tlistFile):
                os.remove(tlistFile)
            with open(tlistFile, 'x') as f:
                for e in targetlistElements:
                    f.write(e.text + '\n')
        except:
            pass
        
# 开两个浏览器,optionBrowser用于读取查询条件中的ID,dataBrowser用于加载数据的页面.这样不用频繁切换和关闭页面
def doFetchData(optionBrowser, dataBrowser):
    # 设置浏览器需要打开的url
    url = 'https://integrity.clarivate.com/integrity/xmlxsl/target_form_pkg.targetForm'
    optionBrowser.get(url)
    sleep(2)

    n = 6
    while 1:
        landscapeNodes = getSubLevelIdsOfNextFirstLevelNode(optionBrowser, n)
        if landscapeNodes is None:
            break
        elif len(landscapeNodes) == 0:
            n += 1
            continue
        else:
            getDatas(dataBrowser, landscapeNodes)
            n += 1

def fetchData():
    if not os.path.exists(downloadFilePath):
        os.makedirs(downloadFilePath)
    chromeOptions = webdriver.ChromeOptions()
    prefs = {'download.default_directory': downloadFilePath}
    chromeOptions.add_experimental_option('prefs', prefs)
    optionBrowser = webdriver.Chrome(chromedriver, options=chromeOptions)
    dataBrowser = webdriver.Chrome(chromedriver, options=chromeOptions)
    urlEntry = 'https://www.google.com'
    ck1 = {'name': 'igr_user', 'value': '1485352363M460140%|ITGR|', 'domain': '.clarivate.com',
           'path': '/integrity/xmlxsl'}
    optionBrowser.get(urlEntry)
    optionBrowser.add_cookie(ck1)
    dataBrowser.get(urlEntry)
    dataBrowser.add_cookie(ck1)

    try:
        doFetchData(optionBrowser, dataBrowser)
    except Exception as ep:
        raise ep
    else:
        # 关闭浏览器
        sleep(5)
        optionBrowser.quit()
        dataBrowser.quit()

if __name__ == "__main__":
    fetchData()
