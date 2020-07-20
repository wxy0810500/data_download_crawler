import random
from time import sleep
import platform
from selenium import webdriver


def secSleep(sec: int):
    sleep(sec * (1 + random.random()))


def getUserAgent():
    sysStr = platform.system()
    if 'Window' == sysStr:
        userAgent = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko)" \
                    " Chrome/83.0.4103.116 Safari/537.36"
    elif 'Linux' == sysStr:
        userAgent = "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) " \
                    "Chrome/83.0.4044.138 Safari/537.36"
    elif 'MacOs' == sysStr:
        userAgent = ''
    else:
        userAgent = None
    return userAgent


def getCookie(chromeDriverPath: str = "C:/Program Files (x86)/Google/Chrome/Application/chromedriver.exe"):
    url = 'https://www.pharmapendium.com/home'

    option = webdriver.ChromeOptions()
    # if userAgent is not None:
    #     option.add_argument(f'User-Agent={userAgent}')
    option.add_argument('headless')
    driver = webdriver.Chrome(executable_path=chromeDriverPath,
                              options=option)
    driver.get(url)
    driver.implicitly_wait(5)
    tempCookie = [
        {

            "domain": ".pharmapendium.com",
            "expirationDate": 1656739197,
            "hostOnly": False,
            "httpOnly": False,
            "name": "_ga",
            "path": "/",
            "sameSite": "unspecified",
            "secure": False,
            "session": False,
            "storeId": "0",
            "value": "GA1.2.331013834.1587106806",
            "id": 1
        },
        {
            "domain": ".pharmapendium.com",
            "expirationDate": 1593753597,
            "hostOnly": False,
            "httpOnly": False,
            "name": "_gid",
            "path": "/",
            "sameSite": "unspecified",
            "secure": False,
            "session": False,
            "storeId": "0",
            "value": "GA1.2.508585961.1593508314",
            "id": 2
        },
        {
            "domain": ".pharmapendium.com",
            "expirationDate": 1656738186,
            "hostOnly": False,
            "httpOnly": False,
            "name": "AMCV_4D6368F454EC41940A4C98A6%40AdobeOrg",
            "path": "/",
            "sameSite": "unspecified",
            "secure": False,
            "session": False,
            "storeId": "0",
            "value": "870038026%7CMCIDTS%7C18446%7CMCMID%7C70204397711297046712905969574590041730%7CMCAAMLH-1594270986%7C11%7CMCAAMB-1594270986%7CRKhpRz8krg2tLO6pguXWp5olkAcUniQYPHaMWWgdJ3xzPWQmdj0y%7CMCOPTOUT-1593673386s%7CNONE%7CMCAID%7CNONE%7CvVersion%7C5.0.0%7CMCCIDH%7C-1108708810",
            "id": 3
        },
        {
            "domain": ".pharmapendium.com",
            "hostOnly": False,
            "httpOnly": False,
            "name": "AMCVS_4D6368F454EC41940A4C98A6%40AdobeOrg",
            "path": "/",
            "sameSite": "unspecified",
            "secure": False,
            "session": True,
            "storeId": "0",
            "value": "1",
            "id": 4
        },
        {
            "domain": ".pharmapendium.com",
            "expirationDate": 1688275223,
            "hostOnly": False,
            "httpOnly": False,
            "name": "s_pers",
            "path": "/",
            "sameSite": "unspecified",
            "secure": False,
            "session": False,
            "storeId": "0",
            "value": "%20v8%3D1593667223723%7C1688275223723%3B%20v8_s%3DLess%2520than%25201%2520day%7C1593669023723%3B%20c19%3Dpp%253Asearch%253Afaers%2520st%253A1.9.2%2520faers%2520selection%2520mode%2520form%7C1593669023729%3B%20v68%3D1593667198064%7C1593669023736%3B",
            "id": 5
        },
        {
            "domain": ".pharmapendium.com",
            "hostOnly": False,
            "httpOnly": False,
            "name": "s_sess",
            "path": "/",
            "sameSite": "unspecified",
            "secure": False,
            "session": True,
            "storeId": "0",
            "value": "%20s_cpc%3D0%3B%20s_cc%3DTrue%3B%20e41%3D1%3B%20s_sq%3D%3B%20s_ppvl%3Dpp%25253Asearch%25253Aquick%252520search%25253Aother%25253A1.1%252520quick%252520search%252C100%252C100%252C938%252C1920%252C938%252C1920%252C1080%252C2%252CP%3B%20s_ppv%3Dpp%25253Asearch%25253Afaers%252520st%25253A1.9.2%252520faers%252520selection%252520mode%252520form%252C100%252C100%252C938%252C1920%252C938%252C1920%252C1080%252C2%252CP%3B",
            "id": 6
        },
        {
            "domain": "www.pharmapendium.com",
            "hostOnly": True,
            "httpOnly": True,
            "name": "AWSELB",
            "path": "/",
            "sameSite": "unspecified",
            "secure": False,
            "session": True,
            "storeId": "0",
            "value": "8DCD43B71E182A8B6F5C60D1CFF30DCBA5B4C54693310DFCD064EC9DEA52EFF9B3E0DBF52958C4E653CB938CB5095043E36B04F3857A68A7FFFA6356657E3E0C0DA497A8243C0718BFF39F776F3787502CDDC72C2C",
            "id": 7
        },
        {
            "domain": "www.pharmapendium.com",
            "expirationDate": 1593834485,
            "hostOnly": True,
            "httpOnly": False,
            "name": "headerBanner",
            "path": "/",
            "sameSite": "unspecified",
            "secure": False,
            "session": False,
            "storeId": "0",
            "value": "-1101040272",
            "id": 8
        },
        {
            "domain": "www.pharmapendium.com",
            "hostOnly": True,
            "httpOnly": True,
            "name": "JSESSIONID",
            "path": "/",
            "sameSite": "unspecified",
            "secure": False,
            "session": True,
            "storeId": "0",
            "value": "038458A7839C7E74D0B4D5A66EE25035",
            "id": 9
        },
        {
            "domain": "www.pharmapendium.com",
            "expirationDate": 1593695996.69766,
            "hostOnly": True,
            "httpOnly": False,
            "name": "previousSessionFlag",
            "path": "/",
            "sameSite": "unspecified",
            "secure": False,
            "session": False,
            "storeId": "0",
            "value": "previousSessionFlag",
            "id": 10
        }
    ]
    cookies = {}
    for cookie in tempCookie:
        cookies[cookie['name']] = cookie['value']

    return cookies
