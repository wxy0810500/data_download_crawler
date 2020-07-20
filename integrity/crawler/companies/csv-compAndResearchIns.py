from selenium import webdriver
import os
import time
import re
from selenium.webdriver.common.action_chains import ActionChains
from selenium.webdriver.support.wait import WebDriverWait
from selenium.common.exceptions import NoSuchElementException
from selenium.webdriver.support import expected_conditions as EC
import random
import json


# 随机等待时间
def sleep(sec):
    time.sleep(sec * (1 + random.random()))

# 引入chromedriver.exe
# chromedriver = 'C:/Program Files (x86)/Google/Chrome/Application/chromedriver.exe'
chromedriver = '/usr/bin/chromedriver'
os.environ['webdriver.chrome.driver'] = chromedriver

class Company():
    def __init__(self, id, organization, official_website, main_activity, headquarters, fiscal_year_ends, therapeutic_areas):
        self.id = id
        self.organization = organization
        self.official_website = official_website
        self.main_activity = main_activity
        self.headquarters = headquarters
        self.fiscal_year_ends = fiscal_year_ends
        self.therapeutic_areas = therapeutic_areas
        
    @property
    def market(self):
        return self._market
    
    @market.setter
    def market(self, mkList):
        self._market = mkList

    @property
    def clinicalTrialsAndPreclinicalResearch(self):
        return self._clinicalTrialsAndPreclinicalResearch

    @clinicalTrialsAndPreclinicalResearch.setter
    def clinicalTrialsAndPreclinicalResearch(self, ctprList):
        self._clinicalTrialsAndPreclinicalResearch = ctprList

    @property
    def recentPatents(self):
        return self._recentPatents
    
    @recentPatents.setter
    def recentPatents(self, rpList):
        self._recentPatents = rpList

    def printAsJson(self):
        print(json.dumps(self, default= lambda obj : obj.__dict__, indent=4))
    
    def outputAsCsv(self):
        info = '{}|{}|{}|{}|{}|{}|{}'.format(self.id, self.organization, self.official_website, self.main_activity,
            self.headquarters, self.fiscal_year_ends, self.therapeutic_areas)
        info += '|'  + ';'.join("&".join(sublist) for  sublist in self._market)
        info += '|' + ';'.join("&".join(sublist) for  sublist in self._clinicalTrialsAndPreclinicalResearch)
        info += '|' + ';'.join("&".join(sublist) for  sublist in self._recentPatents)
        info += '\n'
        return info

basicInfoXpathDict = {
    "organization":"//table[3]//table[1]//table//table//table//table//tr[2]/td[1]/span",
    "official_website":"//table[3]//table[1]//table//table//table//table//tr[2]/td[2]/a",
    "main_activity":"//table[3]//table[1]//table//table//table//table//tr[4]/td[1]",
    "headquarters":"//table[3]//table[1]//table//table//table//table//tr[4]/td[2]",
    "fiscal_year_ends":"//table[3]//table[1]//table//table//table//table//tr[4]/td[3]/span",
    "therapeutic_areas":"//table[3]//table[1]//table//table//table//table//tr[5]//tr[3]/td/span"
}
# market信息,n从2开始,直到找不到类似元素则结束
# n从2开始
marketLinkXpathModel = '//table[3]//table[1]//table[1]//table[1]//table[2]//tr[2]//table//tr[{}]//td[1]/a'#n
# n从2开始,m为2,3,4
marketSpanXpathModel = '//table[3]//table[1]//table[1]//table[1]//table[2]//tr[2]//table//tr[{}]//td[{}]/span'#n,m

# Clinical Trials and Preclinical Research,n从2开始,直到找不到类似元素则结束
ctprLinkXpathModel = '//table[3]//table[1]//table[1]//table[1]//table[3]//tr[2]//tr[{}]/td[1]/a'#n
# n从2开始,m为2,3
ctprSpanXpathModel = '//table[3]//table[1]//table[1]//table[1]//table[3]//tr[2]//tr[{}]/td[{}]/span'#n,m

# recent patents n从2开始
recentLinkXpathModel = '//table[3]//table[1]//table[1]/tbody/tr[1]/td[2]/table[1]/tbody/tr[3]//table//tr[2]/td//tr[{}]/td[1]/a'#n
recentSpanXpathModel = '//table[3]//table[1]//table[1]/tbody/tr[1]/td[2]/table[1]/tbody/tr[3]//table//tr[2]/td//tr[{}]/td[2]/span'#n

def fetchOneCompData(browser):

    compId = re.search(r'\d+', re.search(r'p_company_id=\d+&', browser.current_url).group()).group()
    #basicInfo
    compInfo = Company(
        compId,
        browser.find_element_by_xpath(basicInfoXpathDict["organization"]).text,
        browser.find_element_by_xpath(basicInfoXpathDict["official_website"]).get_attribute('href'),
        browser.find_element_by_xpath(basicInfoXpathDict["main_activity"]).text.replace('\n','\\'),
        browser.find_element_by_xpath(basicInfoXpathDict["headquarters"]).text,
        browser.find_element_by_xpath(basicInfoXpathDict["fiscal_year_ends"]).text,
        browser.find_element_by_xpath(basicInfoXpathDict["therapeutic_areas"]).text
    )
    #market
    mkList = []
    n = 2
    while 1:
        try:
            name = browser.find_element_by_xpath(marketLinkXpathModel.format(n)).text
        except:
            break
        mkinfo = [name]
        for m in range(2,5):
            mkinfo.append(browser.find_element_by_xpath(marketSpanXpathModel.format(n, m)).text.replace('\n', '\\'))
        mkList.append(mkinfo)
        n += 1
    compInfo.market = mkList
    
    #ctpr
    ctprList = []
    n = 2
    while 1:
        try:
            name = browser.find_element_by_xpath(ctprLinkXpathModel.format(n)).text
        except:
            break
        ctprInfo = [name]
        for m in range(2,4):
            ctprInfo.append(browser.find_element_by_xpath(ctprSpanXpathModel.format(n, m)).text.replace('\n', '\\'))
        ctprList.append(ctprInfo)
        n += 1
    compInfo.clinicalTrialsAndPreclinicalResearch = ctprList
    #recent patents
    rpList = []
    n = 2
    while 1:
        try:
            name = browser.find_element_by_xpath(recentLinkXpathModel.format(n)).text
        except:
            break
        rpInfo = [name]
        rpInfo.append(browser.find_element_by_xpath(recentSpanXpathModel.format(n)).text.replace('\n', '\\'))
        rpList.append(rpInfo)
        n += 1
    compInfo.recentPatents = rpList
    compInfo.printAsJson()
    return compInfo

def fetchOnePageCompData(browser, list_window):
    # 逐个点击链接进入单独的页面,分页每一页最多20个
    #//table[3]//table[1]//tr[3]//table//table//tr[n],n=2-21
    n = 2
    comInfoList = []
    while 1:
        try:
            browser.switch_to.window(list_window)
            a = browser.find_element_by_xpath('//table[3]//table[1]//tr[3]//table//table//tr[{}]//a'.format(n))
        except:
            break
        link = a.get_attribute('href')
        #打开新的标签页
        newWindowsp = 'window.open("{}")'.format(link)
        browser.execute_script(newWindowsp)
        browser.switch_to.window(browser.window_handles[1])
        compInfo = fetchOneCompData(browser)
        comInfoList.append(compInfo)
        sleep(2)
        browser.close()
        n += 1
    return comInfoList

def doFetchData(browser, query):
    # 设置浏览器需要打开的url
    url = 'https://integrity.clarivate.com/integrity/xmlxsl/pk_com_form.xml_com_form_pr#here'
    urlEntry = 'https://www.google.com'
    ck1 = {'name': 'igr_user', 'value': '1485352363M460140%|ITGR|', 'domain': '.clarivate.com',
           'path': '/integrity/xmlxsl'}
    browser.get(urlEntry)
    browser.add_cookie(ck1)
    browser.get(url)
    sleep(2)

    # 设置query
    #select value按钮
    browser.find_element_by_id('div_par_COM1').click()
    sleep(1)
    #鼠标选择Company Economic Data
    action = ActionChains(browser)
    economicDataMenu = browser.find_element_by_xpath('//td[@id="ORG_CED"]/b')
    action.move_to_element(economicDataMenu).perform()
    sleep(1)
    #选择total sales
    browser.find_element_by_xpath('//td[@id="ORG_CED_TOTAL_SALES"]/a').click()
    #填入From 100
    q1 = browser.find_element_by_id('p_val_COM1')
    q1.clear()
    q1.send_keys(query)
    sleep(1 )

    # 单击搜索按钮
    browser.find_element_by_xpath('//*[@id="0"]/table/tbody/tr[2]/td/table/tbody/tr[1]/td[8]/a/img').click()
    sleep(10)
    
    # 保存窗口句柄
    list_window = browser.current_window_handle
    outputFile = '/home/ubuntu/GHDDI/download/integrity/comAndResearchIns-{}.csv'.format(query)
    if os.path.exists(outputFile):
        os.remove(outputFile)
    with open(outputFile, 'x') as f:
        # 循环点击分页按钮
        i = 0
        while 1:
            compList = fetchOnePageCompData(browser,list_window)
            for compInfo in compList:
                f.write(compInfo.outputAsCsv())
            i += 1
            #点击下一页的按钮
            try:
                browser.find_element_by_xpath('//table[3]//table//tr[1]/td[3]/a[{}]'.format(i))
            except :
                #找不到下一页的按钮,结束处理
                break

def fetchData(query):
    downloadFilePath = '/home/ubuntu/GHDDI/download/integrity/company'
    if not os.path.exists(downloadFilePath):
        os.makedirs(downloadFilePath)
    chromeOptions = webdriver.ChromeOptions()
    prefs = {'download.default_directory': downloadFilePath}
    chromeOptions.add_experimental_option('prefs', prefs)
    browser = webdriver.Chrome(chromedriver, options=chromeOptions)

    try:
        doFetchData(browser, query)
    except NoSuchElementException as ep:
        raise ep
    else:
        # 关闭浏览器
        sleep(5)
    finally:
        browser.quit()

if __name__ == "__main__":
    fetchData('From 1')

    