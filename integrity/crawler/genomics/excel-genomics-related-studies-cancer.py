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
            sleep(2)
            continue        
        sleep(2)
        # 删除
        browser.find_element_by_xpath('/html/body/table[3]/tbody/tr[2]/td[7]/a').click()
        fileNumber -= 1
        sleep(2)
    sleep(20)
    browser.close()


def exportExcel(result_window, browser, totalNumber, basicFileName):
    num = 1000
    pageNumber = 0
    if totalNumber <= num:
        browser.switch_to.window(result_window)
        generateExcel(browser, 0, totalNumber, basicFileName)
        pageNumber += 1
    else:
        while pageNumber < totalNumber / num:
            browser.switch_to.window(result_window)
            generateExcel(browser, pageNumber, totalNumber, basicFileName)
            pageNumber += 1
    return pageNumber


# 从exportCenter中生成excel
def generateExcel(browser, pageNumber, totalNumber, basicFileName):
    
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

def doFetchData(browser, n):
    # 设置浏览器需要打开的url
    url = 'https://integrity.clarivate.com/integrity/xmlxsl/genomic_form_pkg.genomicForm#'
    urlEntry = 'https://www.google.com'
    ck1 = {'name': 'igr_user', 'value': '1485352363M460140%|ITGR|', 'domain': '.clarivate.com',
           'path': '/integrity/xmlxsl'}
    browser.get(urlEntry)
    browser.add_cookie(ck1)
    browser.get(url)
    sleep(2)

    # 2. 切换到cancer窗口
    cancerWindow = 'window.open("https://integrity.clarivate.com/integrity/xmlxsl/genomic_list_pkg.xml_gateways_nivel2?p_disease_id=576")'
    browser.execute_script(cancerWindow)
    browser.switch_to.window(browser.window_handles[1])
    browser.implicitly_wait(3)
    sleep(1)
    # 3. 选择cancer的一个下级选项
    checkbox = browser.find_element_by_xpath('//input[@id="id1856"]/following::img[@width=40][{}]/following::input[1]'.format(n))
    id = int(checkbox.get_attribute('value'))
    checkbox.click()
    name = browser.find_element_by_xpath('//input[@id="id1856"]/following::img[@width=40][{}]/following::a[1]'.format(n)).text
    sleep(1)
    # 4.click ok按钮 //center//tr[5]//input
    browser.find_element_by_xpath("//center//tr[5]//input").click()
    sleep(5)
    # 5. cancer选择窗口自动关闭,切回到原有窗口
    browser.switch_to.window(browser.window_handles[0])
    # 提取总数
    numberText = browser.find_element_by_xpath('//table[2]//tr[2]//td[3]/b').text
    totalNumberS = re.search(r'(\d+)', str(re.search(r'of (\d+) retrieved', numberText).group()))
    totalNumber = int(totalNumberS.group())
    print('total number: ', totalNumber)

    # # 保存窗口句柄
    result_window = browser.current_window_handle
    sleep(1)
    #  从export-center中生成excel
    fileBasicName = 'cancer-' + name + '_' + str(id)
    fileNum = exportExcel(result_window, browser, totalNumber, fileBasicName)
    # 等待excel文件生成
    sleep(20)
    downloadExcel(result_window, browser, fileNum)

def fetchData(n):
    downloadFilePath = '/home/ubuntu/GHDDI/download/integrity/genomics-related-studies'
    if not os.path.exists(downloadFilePath):
        os.makedirs(downloadFilePath)
    chromeOptions = webdriver.ChromeOptions()
    prefs = {'download.default_directory': downloadFilePath}
    chromeOptions.add_experimental_option('prefs', prefs)
    browser = webdriver.Chrome(chromedriver, options=chromeOptions)

    try:
        doFetchData(browser, n)
    except Exception as ep:
        raise ep
    else:
        # 关闭浏览器
        sleep(5)
    finally:
        browser.quit()

if __name__ == "__main__":
    #cancer.Malignant neoplasms中共17种小类
    for i in range(1,18):
        fetchData(i)
