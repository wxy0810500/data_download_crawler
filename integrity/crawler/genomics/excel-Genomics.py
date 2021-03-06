from selenium import webdriver
import os
import time
import re
from selenium.webdriver.common.action_chains import ActionChains
from selenium.webdriver.support.wait import WebDriverWait
from selenium.common.exceptions import NoSuchElementException
from selenium.webdriver.support import expected_conditions as EC
import random


# 随机等待时间
def sleep(sec):
    time.sleep(sec * (1 + random.random()))

# 引入chromedriver.exe
# chromedriver = 'C:/Program Files (x86)/Google/Chrome/Application/chromedriver.exe'
chromedriver = '/usr/bin/chromedriver'
os.environ['webdriver.chrome.driver'] = chromedriver


def downloadExcel(result_window, browser, fileNumber):
    # 跳转到download页,下载生成的excel文件
    # 先从result页点击Downloads
    browser.switch_to.window(result_window)
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
            sleep(5)
            continue        
        sleep(2)
        # 删除
        browser.find_element_by_xpath('/html/body/table[3]/tbody/tr[2]/td[7]/a').click()
        fileNumber -= 1
        sleep(5)
    sleep(200)
    browser.close()


def exportExcel(result_window, browser, totalNumber, basicFileName):
    num = 1000
    pageNumber = 0
    if totalNumber <= num:
        generateExcel(result_window, browser, 0, totalNumber, basicFileName)
        pageNumber += 1
    else:
        while pageNumber < totalNumber / num:
            generateExcel(result_window, browser, pageNumber, totalNumber, basicFileName)
            pageNumber += 1
    return pageNumber


# 从exportCenter中生成excel
def generateExcel(result_window, browser, pageNumber, totalNumber, basicFileName):
    browser.switch_to.window(result_window)
    sleep(2)
    # 在查询结果页选择options下的Export Center
    action = ActionChains(browser)
    optionMenu = browser.find_element_by_xpath('/html/body/table/tbody/tr/td/table[2]/tbody/tr[2]/td[4]/a/img')
    action.move_to_element(optionMenu).perform()
    sleep(1)
    browser.find_element_by_link_text('Export Center').click()
    sleep(2)
    # 获取export-center窗口
    export_center_window = browser.window_handles[1]
    # 跳转到export-center窗口
    browser.switch_to.window(export_center_window)
    sleep(3)
    # 填写分页参数
    pageString = '{} - {}'.format(pageNumber * 1000 + 1, (
        (pageNumber + 1) * 1000 if (pageNumber + 1) * 1000 < totalNumber else totalNumber))
    while(1):
        try:
            rRange = browser.find_element_by_id('pages')
            break
        except:
            sleep(2)
            continue
    rRange.clear()
    sleep(1)
    rRange.send_keys(pageString)
    sleep(1)

    # 填写导出的名字
    export_file_name = basicFileName + '-' + pageString
    rFileName = browser.find_element_by_id('p_exportName')
    rFileName.clear()
    rFileName.send_keys(export_file_name)
    sleep(1)
    # 点击generate按钮,而后关闭
    browser.find_element_by_xpath('//form/center/table/tbody/tr[7]/td/center/p[1]/img').click()
    sleep(2)
    browser.close()

def doFetchData(year, yRange, browser):
    # 设置浏览器需要打开的url
    url = 'https://integrity.clarivate.com/integrity/xmlxsl/genomic_form_pkg.genomicForm'
    urlEntry = 'https://www.google.com'
    ck1 = {'name': 'igr_user', 'value': '1692174364M460140%|ITGR|', 'domain': '.clarivate.com',
           'path': '/integrity/xmlxsl'}
    browser.get(urlEntry)
    browser.add_cookie(ck1)
    browser.get(url)
    sleep(2)
    browser.maximize_window()
    sleep(2)

    # 设置Key1:时间
    browser.find_element_by_id('div_par_GEN1').click()
    sleep(1)
    browser.find_element_by_id('GEN_AVAILABLE_SINCE').click()
    sleep(1)

    # 设置query1
    q2 = browser.find_element_by_id('p_val_GEN1')
    q2.clear()
    q2.send_keys(yRange)
    sleep(1)

    # 单击搜索按钮
    browser.find_element_by_xpath('//*[@id="0"]/table/tbody/tr[2]/td/table/tbody/tr[1]/td[8]/a/img').click()
    sleep(10)
    
    # 提取总数
    numberText = browser.find_element_by_xpath('/html/body/table/tbody/tr/td/table[2]/tbody/tr[2]/td[3]/b').text
    totalNumberS = re.search(r'(\d+)', str(re.search(r'of (\d+) retrieved', numberText).group()))
    totalNumber = int(totalNumberS.group())
    print('total number: ', totalNumber)

    # 保存窗口句柄
    result_window = browser.current_window_handle
    sleep(1)
    #  从export-center中生成excel
    fileNum = exportExcel(result_window, browser, totalNumber, yRange)
    # 等待excel文件生成
    sleep(20)
    downloadExcel(result_window, browser, fileNum)

def fetchData(year, yRange):
    print("start :" + yRange)
    downloadFilePath = '/home/ubuntu/GHDDI/download/integrity/genomics/{}'.format(year)
    if not os.path.exists(downloadFilePath):
        os.makedirs(downloadFilePath)
    chromeOptions = webdriver.ChromeOptions()
    prefs = {'download.default_directory': downloadFilePath}
    chromeOptions.add_experimental_option('prefs', prefs)
    browser = webdriver.Chrome(chromedriver, options=chromeOptions)

    try:
        doFetchData(year, yRange, browser)
    except NoSuchElementException as ep:
        browser.quit()
        raise ep
    else:
        # 关闭浏览器
        sleep(5)
        browser.quit()

if __name__ == "__main__":
    # 控制流程
    years = [ 2011]
    year_ranges = [
        'From {}0101 to {}1231'
    ]
    half_year_ranges = [
        'From {}0101 to {}0630',
        'From {}0701 to {}1231',
    ]
    season_ranges = [
        'From {}0101 to {}0331',
        'From {}0401 to {}0630',
        'From {}0701 to {}0930',
        'From {}1001 to {}1231',
    ]
    #下载的时间区间是2004-2019
    # for _year in range(2005, 2020):
    for _year in years:
        for _range in year_ranges:
        # for _range in half_year_ranges:
        # for _range in season_ranges:
            yRange = _range.format(_year, _year)
            fetchData(str(_year), yRange)
            sleep(20)
