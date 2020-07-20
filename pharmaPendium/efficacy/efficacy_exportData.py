from selenium import webdriver
import os
import time
import json
import re
from selenium.webdriver.common.action_chains import ActionChains
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
import random
import sys


# 随机等待时间
def sleep(sec):
    time.sleep(sec * (1 + random.random()))


# 引入chromedriver.exe
chromedriver = 'C:/Program Files (x86)/Google/Chrome/Application/chromedriver.exe'
# chromedriver = '/usr/bin/chromedriver'
os.environ['webdriver.chrome.driver'] = chromedriver


def getChromeDriver():
    # 设置下载路径如果不存在，新建
    downloadFilePath = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'data\\efficacy')
    if not os.path.exists(downloadFilePath):
        os.makedirs(downloadFilePath)
    print(downloadFilePath)
    chromeOptions = webdriver.ChromeOptions()
    prefs = {'download.default_directory': downloadFilePath}
    chromeOptions.add_experimental_option('prefs', prefs)

    driver = webdriver.Chrome(chromedriver, options=chromeOptions)
    driver.implicitly_wait(60)
    return driver


def searchHuman(browser):
    url = 'https://www.pharmapendium.com/efficacy/search'
    browser.get(url)
    browser.maximize_window()
    browser.find_element_by_xpath('//els-btn[@text="Add species"]/button').click()

    sleep(2)
    # select human
    checkboxHuman = browser.find_elements_by_xpath('//label[@class="itemSelf__checkbox"]')

    checkboxHuman[0].click()
    # Done
    browser.find_element_by_xpath('//els-btn[@text="Done"]/button').click()
    # search
    searchButton = WebDriverWait(browser, 5).until(EC.element_to_be_clickable(
        (By.XPATH, '//els-btn[@text="Search"]/button')))
    # searchButton = browser.find_element_by_xpath('//els-btn[@text="Search"]/button')
    searchButton.click()
    return browser


def selectConditions(browser: webdriver, placebo, years, dataProviders):
    # open placebo items
    browser.find_element_by_xpath("//h4[normalize-space(text())='Placebo']/"
                                  "following-sibling::els-btn/button").click()
    sleep(2)

    # select placebo
    if placebo == 'Placebo':
        WebDriverWait(browser, 60). \
            until(EC.element_to_be_clickable((By.XPATH, '//span[.="Placebo"]/parent::span/preceding-sibling::label'))).\
            click()
    else:
        WebDriverWait(browser, 60). \
            until(EC.element_to_be_clickable((By.XPATH, '//span[.="Not placebo"]/parent::span/preceding-sibling::label'))).\
            click()

    # select years
    browser.find_element_by_xpath("//h4[normalize-space(text())='Years']/"
                                  "following-sibling::els-btn/button").click()
    sleep(2)
    yearsSeleted = {}
    print(years)
    for year in years:
        year = int(year)
        if year % 10 == 0:
            start = (year // 10 - 1) * 10 + 1
        else:
            start = (year // 10) * 10 + 1
        end = start + 9
        yearsInPeriod = yearsSeleted.get(f'{start} - {end}', None)
        print(f'{year} | {yearsInPeriod}')
        if yearsInPeriod is None:
            yearsSeleted[f'{start} - {end}'] = [year]
        else:
            yearsInPeriod.append(year)

    for period, pYears in yearsSeleted.items():
        # open year period
        yearPeriodButtonXpath = f"//span[text()='{period}']/ancestor::span/parent::div"
        browser.find_element_by_xpath(yearPeriodButtonXpath).click()
        for pYear in pYears:
            selectYearLabelXpath = f"//span[text()='{pYear}']/ancestor::span/preceding-sibling::label"
            browser.find_element_by_xpath(selectYearLabelXpath).click()
    sleep(1)
    # select data provider if necessary
    if dataProviders is not None:
        browser.find_element_by_xpath("//h4[normalize-space(text())='Data provider']/"
                                      "following-sibling::els-btn/button").click()
        sleep(2)
        for dp in dataProviders:
            browser.find_element_by_xpath(f"//span[text()='{dp}']/ancestor::span/preceding-sibling::label").click()

    # apply
    browser.find_element_by_xpath('//els-btn[@text="Apply"]/button').click()

    return browser


def export(browser: webdriver):
    browser.find_element_by_xpath('//els-btn[@text="Export"]/button').click()
    sleep(3)
    # select all columns
    browser.find_element_by_xpath("//a[@ng-click='setSelectedAllColumns(true)']").click()
    while 1:
        try:
            exportEle = WebDriverWait(browser, 60). \
                until(EC.element_to_be_clickable((By.XPATH, '//a[.="Export as comma delimited (.csv)"]')))
            # export csv
            exportEle.click()
            break
        except Exception as e:
            continue
    # browser.find_element_by_xpath('//a[.="Export as comma delimited (.csv)"]').click()


def clearQueryConditions(browser: webdriver):
    browser.find_element_by_xpath('//els-btn[@text="Clear all"]/button').click()


def process():
    browser = getChromeDriver()

    try:
        browser = searchHuman(browser)
        with open('efficacy-qcond-clinical', 'r') as f:
            lines = f.readlines()
            i = 1
            for line in lines:
                if i % 3 == 0:
                    sleep(10)
                qCond = json.loads(line)
                placeBo = qCond["queryCond"]["placebo"]["narrowing"][0]
                years = qCond["queryCond"]["years"]["narrowing"]
                dataProviders = qCond["queryCond"].get('dataProviders')
                if dataProviders is not None:
                    dataProviders = dataProviders['narrowing']
                browser = selectConditions(browser, placeBo, years, dataProviders)
                sleep(1)
                export(browser)
                sleep(3)
                clearQueryConditions(browser)
                sleep(3)
                i = i + 1
    finally:
        browser.close()


if __name__ == '__main__':
    process()
