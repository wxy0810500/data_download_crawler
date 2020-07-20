from selenium import webdriver  
import os
import time
import re
from selenium.webdriver.common.action_chains import ActionChains
import random
import sys

#设置递归深度
sys.setrecursionlimit(10000000)


#随机等待时间
def sleep (sec):
  time.sleep(sec * (1 + random.random()))

#引入chromedriver.exe
# chromedriver = 'C:/Program Files (x86)/Google/Chrome/Application/chromedriver.exe'
chromedriver = '/usr/bin/chromedriver'
os.environ['webdriver.chrome.driver'] = chromedriver

if __name__ == "__main__":
    browser = webdriver.Chrome(chromedriver)
    urlEntry = 'https://www.google.com'
    ck1 = {'name': 'igr_user', 'value': '1649022361M460140%|ITGR|', 'domain': '.clarivate.com',
           'path': '/integrity/xmlxsl'}
    browser.get(urlEntry)
    browser.add_cookie(ck1)
    url  = 'https://integrity.clarivate.com/integrity/xmlxsl/pk_browse_index.xml_lov?p_type=A&p_subsystem=EXP&p_seleccio=EXP_PHARMA_ACT&p_loc=p_val_EXP1&p_lletra='
    browser.get(url)
    sleep(2)
    browser.switch_to.frame('mainFrame')
    optionlist = browser.find_elements_by_xpath("//option")
    file =  '/home/ubuntu/GHDDI/download/integrity/expPharmacologicalActivitesOptions.csv'
    if os.path.exists(file):
        os.remove(file)

    with open(file, 'x') as f:
        for option in optionlist:
            f.write(option.text + '\n')

    browser.quit()
