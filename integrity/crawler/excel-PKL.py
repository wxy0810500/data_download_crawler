from selenium import webdriver  
import os
import time
import re
from selenium.webdriver.common.action_chains import ActionChains
import random

#随机等待时间
def sleep (sec):
  time.sleep(sec * (1 + random.random()))

#引入chromedriver.exe
# chromedriver = 'C:/Program Files (x86)/Google/Chrome/Application/chromedriver.exe'
chromedriver = '/usr/bin/chromedriver'
os.environ['webdriver.chrome.driver'] = chromedriver

def fetchData(year, yRange):
    #设置下载路径如果不存在，新建
    downloadFilePath = '/home/ubuntu/GHDDI/download/integrity/pk/' + year
    if not os.path.exists(downloadFilePath):
        os.makedirs(downloadFilePath)
    chromeOptions = webdriver.ChromeOptions()
    prefs = {'download.default_directory': downloadFilePath}
    chromeOptions.add_experimental_option('prefs', prefs)
    browser = webdriver.Chrome(chromedriver, options=chromeOptions)

    #设置浏览器需要打开的url
    url = 'https://integrity.clarivate.com/integrity/xmlxsl/pk_pkl_form.xml_pkl_form_pr'
    urlEntry = 'https://www.google.com'
    ck1 = {'name':'igr_user', 'value':'1689563423M460140%|ITGR|', 'domain':'.clarivate.com', 'path':'/integrity/xmlxsl'}
    browser.get(urlEntry)
    browser.add_cookie(ck1)
    browser.get(url)
    sleep(2)
    browser.maximize_window()
    sleep(2)
    #设置Key1
    browser.find_element_by_id('div_par_PKL1').click()
    sleep(1)
    browser.find_element_by_id('PKL_AVAILABLE_SINCE').click()
    sleep(1)

    #设置query1
    q1 = browser.find_element_by_id('p_val_PKL1')
    q1.clear()
    q1.send_keys(yRange)
    sleep(1)

    #单击红色start按钮
    browser.find_element_by_xpath('//*[@id="0"]/table/tbody/tr[2]/td/table/tbody/tr[1]/td[8]/a/img').click()
    sleep(5)

    #当单次查询的数量超过20,000的时候提取总数
    # numberText = browser.find_element_by_xpath('//div[@id="mainLayer"]/table/tbody/tr[2]/td[2]/table/tbody/tr/td/table/tbody/tr[4]/td[2]/span').text
    # totalNumberS = re.search(r'(\d+[,\d+]+)', numberText)
    # totalNumber = int(totalNumberS.group().replace(',',''))
    # print('{}, {} total number: {}'.format(year, yRange, totalNumber))

    #提取总数
    numberText = browser.find_element_by_xpath('/html/body/table[2]/tbody/tr/td/table/tbody/tr[2]/td[3]').text
    totalNumberS = re.search(r'(\d+)', numberText)
    totalNumber = int(totalNumberS.group())
    print('total number: ', totalNumber)

    #保存窗口句柄
    result_window = browser.current_window_handle
    sleep(1)

    pageNumber = 0
    while pageNumber < totalNumber/1000:
        browser.switch_to.window(result_window)
         #在查询结果页选择options下的Export Center
        action = ActionChains(browser)
        optionMenu = browser.find_element_by_xpath('/html/body/table[2]/tbody/tr/td/table/tbody/tr[2]/td[4]/a/img')
        action.move_to_element(optionMenu).perform()
        sleep(2)
        browser.find_element_by_link_text('Export Center').click()
        sleep(2)
        #获取export-center窗口
        export_center_window = browser.window_handles[1]
        #跳转到export-center窗口
        browser.switch_to.window(export_center_window)
        
        #填写分页参数
        pageString = '{} - {}'.format(pageNumber*1000+1,((pageNumber+1)*1000 if (pageNumber+1)*1000 < totalNumber else totalNumber))
        rRange = browser.find_element_by_id('pages')
        rRange.clear()
        sleep(1)
        rRange.send_keys(pageString)
        sleep(1)

        #填写导出的名字
        export_file_name = yRange + '-' + pageString
        rFileName = browser.find_element_by_id('p_exportName')
        rFileName.clear()
        rFileName.send_keys(export_file_name)
        sleep(1)
        #点击generate按钮,而后关闭
        browser.find_element_by_xpath('//form/center/table/tbody/tr[7]/td/center/p[1]/img').click()
        sleep(2)
        browser.close()
        pageNumber += 1

    sleep(10)
    #跳转到download页,下载生成的excel文件
    #先从result页点击Downloads
    browser.switch_to.window(result_window)
    browser.find_element_by_id('reports').click()
    download_window = browser.window_handles[1]
    browser.switch_to.window(download_window)
    browser.find_element_by_xpath('/html/body/table[1]/tbody/tr[2]/td/table/tbody/tr[2]/td[4]/a').click()

    #从列表的第一个开始下载,下载一个删除一个
    while pageNumber > 0:
        #下载
        browser.find_element_by_xpath('/html/body/table[3]/tbody/tr[2]/td[6]/a').click()
        sleep(2)
        #删除
        browser.find_element_by_xpath('/html/body/table[3]/tbody/tr[2]/td[7]/a').click()
        pageNumber -=1
        sleep(2)
    #关闭浏览器
    sleep(20)
    browser.quit()

# 控制流程
years = ['2019']
ranges = [
    # 'From {}0301 to {}0331',
    'From {}0401 to {}0430',
     'From {}0501 to {}0531',
    'From {}0601 to {}0630',
    'From {}0701 to {}0731',
    'From {}0801 to {}0831',
    'From {}0901 to {}0930',
    'From {}1001 to {}1031',
    'From {}1101 to {}1130',
    'From {}1201 to {}1231',
]

for _year in years:
    for _range in ranges:
        yRange = _range.format(_year,_year)    
        fetchData(_year, yRange)
        sleep(30)