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
chromedriver = 'C:/Program Files (x86)/Google/Chrome/Application/chromedriver.exe'
os.environ['webdriver.chrome.driver'] = chromedriver

def fetchData(yRange):
    #设置下载路径如果不存在，新建
    if not os.path.exists('D:/GHDDI/file/target/'+ yRange):
        os.makedirs('D:/GHDDI/file/target/'+ yRange)
    chromeOptions = webdriver.ChromeOptions()
    prefs = {'download.default_directory': 'D:/GHDDI/file/target/'+ yRange}
    chromeOptions.add_experimental_option('prefs', prefs)
    browser = webdriver.Chrome(chromedriver, options=chromeOptions)

    #设置浏览器需要打开的url
    url = 'https://integrity.thomson-pharma.com/integrity/xmlxsl/target_form_pkg.targetForm'
    urlEntry = 'https://www.google.com'
    ck1 = {'name':'igr_user', 'value':'564924736M460140%|ITGR|', 'domain':'.thomson-pharma.com', 'path':'/integrity/xmlxsl'}
    browser.get(urlEntry)
    browser.add_cookie(ck1)
    browser.get(url)
    sleep(2)
    browser.maximize_window()
    sleep(2)
    #设置Key1
    browser.find_element_by_id('div_par_TAR1').click()
    sleep(1)
    browser.find_element_by_id('TAR_AVAILABLE_SINCE').click()
    sleep(1)

    #设置query1
    q1 = browser.find_element_by_id('p_val_TAR1')
    q1.clear()
    q1.send_keys(yRange)
    sleep(1)

    # #设置Key2
    # browser.find_element_by_id('div_par_EXP2').click()
    # sleep(1)
    # browser.find_element_by_id('EXP_AVAILABLE_SINCE').click()
    # sleep(1)

    # #设置query2
    # q2 = browser.find_element_by_id('p_val_EXP2')
    # q2.clear()
    # q2.send_keys(yRange)
    # sleep(1)


    #单击搜索按钮
    browser.find_element_by_xpath('//*[@id="0"]/table/tbody/tr[2]/td/table/tbody/tr[1]/td[8]/a/img').click()
    sleep(5)

    #提取总数
    numberText = browser.find_element_by_xpath('/html/body/table/tbody/tr/td/table[2]/tbody/tr[2]/td[3]').text
    totalNumberS = re.findall(r'(\d+)', numberText)
    totalNumber = int(totalNumberS[-1])
    print('{} total number: {}'.format(yRange, totalNumber))

    #保存窗口句柄
    result_window = browser.current_window_handle
    sleep(1)
    #点击导出按钮
    action = ActionChains(browser)

    optionMenu = browser.find_element_by_xpath('/html/body/table/tbody/tr/td/table[2]/tbody/tr[2]/td[4]/a/img')
    action.move_to_element(optionMenu).perform()
    sleep(2)
    browser.find_element_by_link_text('Export Center').click()
    sleep(3)

    #跳转窗口
    download_window = browser.window_handles[1]

    pageNumber = 0
    while pageNumber < totalNumber/1000:
        browser.switch_to.window(download_window)
        #点击选项
        # entry15 = browser.find_element_by_name('entry15')
        # if entry15.is_selected():
        #     entry15.click()
        # sleep(1)
        pageString = '{} - {}'.format(pageNumber*1000+1,((pageNumber+1)*1000 if (pageNumber+1)*1000 < totalNumber else totalNumber))
        rRange = browser.find_element_by_id('pages')
        rRange.clear()
        rRange.send_keys(pageString)
        sleep(1)
        browser.find_element_by_xpath('/html/body/form/center/table/tbody/tr[7]/td/center/center/p[1]/b/span/input[1]').click()
        sleep(120)
        pageNumber += 1

    #关闭浏览器
    sleep(60)
    # browser.quit()

# 控制流程
#最新截止到201904
ranges = [
    'From 19900101 to 20101231',
    # 'From 20110101 to 20190331',
]

for _range in ranges:  
    fetchData(_range)
    sleep(30)