from selenium import webdriver
import os
import time
import re
from selenium.webdriver.common.action_chains import ActionChains
from selenium.webdriver.common.by import By
from selenium.webdriver.support.wait import WebDriverWait
from selenium.common.exceptions import NoSuchElementException
from selenium.webdriver.support import expected_conditions as EC
import random

# 下载drugs页面左栏的pipeline list

# 随机等待时间
def sleep(sec):
    time.sleep(sec * (1 + random.random()))

# 引入chromedriver.exe
# chromedriver = 'C:/Program Files (x86)/Google/Chrome/Application/chromedriver.exe'
chromedriver = '/usr/bin/chromedriver'
os.environ['webdriver.chrome.driver'] = chromedriver


downloadBasicFilePath = '/home/ubuntu/GHDDI/download/integrity/drugs-pipeline'
if not os.path.exists(downloadBasicFilePath):
    os.makedirs(downloadBasicFilePath)

exportExcelUrlModel = 'https://integrity.clarivate.com/igrreport/PrintBox/export/pk_pipeline.pipelineListExport?p_sessionID=363505&p_page=1&p_orden=1&p_origen=POM&p_type=Milestones&p_format=Excel&p_fileName={}.xls&p_totalRecords=30&p_knowledgeArea=POM&p_userId=460140&type_export=xls&entry=on&entry2=on&entry3=on&entry4=on&entry5=on&entry6=on&entry7=on&pages={}-{}&p_exportName={}'

def downloadExcel(browser, fileNumber):
    # 跳转到download页,下载生成的excel文件
    # 先从result页点击Downloads
    browser.find_element_by_id('reports').click()
    download_window = browser.window_handles[1]
    browser.switch_to.window(download_window)
    browser.find_element_by_xpath('/html/body/table[1]/tbody/tr[2]/td/table/tbody/tr[2]/td[4]/a').click()
    sleep(2)
    # 从列表的第一个开始下载,下载一个删除一个
    while fileNumber > 0:
        # 下载
        try:
            browser.find_element_by_xpath('/html/body/table[3]/tbody/tr[2]/td[6]/a').click()
        except:
            sleep(2)
            continue        
        sleep(2)
        # 删除
        browser.find_element_by_xpath('/html/body/table[3]/tbody/tr[2]/td[7]/a').click()
        fileNumber -= 1
        sleep(2)
    sleep(10)
    browser.close()


def exportExcel(exportBrowser, totalNumber, fileName):
    sleep(2)
    exportUrl = exportExcelUrlModel.format(fileName,  1, totalNumber, fileName)
    exportBrowser.get(exportUrl)

def doFetchData(browser, exportBrowser):
    url = 'https://integrity.clarivate.com/integrity/xmlxsl/pk_pipeline.xmlPipeline?p_SessionID=363743&p_page=1&p_origen=HOME&p_lunes=18/12/02&p_domingo=24/12/02'
    browser.get(url)
    sleep(2)

    year = 2002
    currentYear = year
    fileNum = 0
    totalNumber = 0
    while currentYear >= 2000:
        browser.implicitly_wait(10)
        sleep(2)
        # 提取总数
        numberText = browser.find_element_by_xpath('//table[2]//td[3]/b').text
        recordNums = re.search(r'(\d+)', numberText)
        recordNum = int(recordNums.group())
        totalNumber += recordNum

        # 提取milestone
        mileStone = browser.find_element_by_xpath('//table[3]//table//td[2]').text
        currentYear = int(mileStone[-5:-1])
        #  从export-center中生成excel
        fileName = mileStone[1:-1] + '--' + str(recordNum)
        print(fileName)
        exportExcel(exportBrowser, recordNum, fileName)
        fileNum += 1
        if currentYear != year :
            # 等待excel文件生成
            sleep(10)
            result_window = browser.current_window_handle
            downloadExcel(browser, fileNum)
            browser.switch_to.window(result_window)
            year = currentYear
            fileNum = 0
        # 点击切换到下一次
        browser.find_element_by_xpath("//a[@class='subtitol']").click()
    return totalNumber

def fetchData():
    chromeOptions = webdriver.ChromeOptions()
    prefs = {'download.default_directory': downloadBasicFilePath}
    chromeOptions.add_experimental_option('prefs', prefs)
    urlEntry = 'https://www.google.com'
    ck1 = {'name': 'igr_user', 'value': '1649022361M460140%|ITGR|', 'domain': '.clarivate.com',
           'path': '/integrity/xmlxsl'}
    browser = webdriver.Chrome(chromedriver, options=chromeOptions)
    browser.get(urlEntry)
    browser.add_cookie(ck1)
    exportBrowser = webdriver.Chrome(chromedriver)
    exportBrowser.get(urlEntry)
    exportBrowser.add_cookie(ck1)

    try:
        totalNum = doFetchData(browser, exportBrowser)
        print(totalNum)
    except Exception as ep:
        raise ep
    else:
        # 关闭浏览器
        sleep(5)
    finally:
        browser.quit()
        exportBrowser.quit()

if __name__ == "__main__":
    fetchData()
