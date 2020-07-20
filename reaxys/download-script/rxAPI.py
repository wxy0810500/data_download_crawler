from string import Template
import requests
import xml.etree.ElementTree as ET
import time
import argparse
from time import sleep
import os
from datetime import date

"""
Please follow these best practises
We would like to recommend that the number of parallel sessions is limited to 5 to 10.
-	Create not more than 1000 hitsets per session
-	Do not create a session per query, where a query generates only small hitsets
-	Loop over a reaction hitset in chunks of 200-500 reactions
-	Retrieve reactions details also by looping over an existing hitset
"""

connect_temp = Template("""<?xml version="1.0"?>
          <xf>
            <request caller="$CALLER">
              <statement command="connect" username="$USER" password="$PASSWORD"/>
            </request>
          </xf>
""")
disconnect_temp = Template("""<?xml version="1.0"?>
          <xf>
            <request caller="$CALLER">
              <statement command="disconnect" sessionid="$SESSIONID"/>
            </request>
          </xf>
""")
select_temp = Template("""<?xml version="1.0" encoding="UTF-8"?>
          <xf>
            <request caller="$CALLER" sessionid="">
              <statement command="select"/>
              <select_list>
                <select_item/>
              </select_list>
              <from_clause dbname="$DB" context="$CONTEXT">
              </from_clause>
              <where_clause>$WHERE</where_clause>
              <order_by_clause>$ORDER</order_by_clause>
              <options>$OPTIONS</options>
            </request>
          </xf>
""")
retrieve_temp = Template("""<?xml version="1.0" encoding="UTF-8"?>
          <xf>
            <request caller="$CALLER" sessionid="">
              <statement command="select"/>
              <select_list>
                $SELECT_ITEM
              </select_list>
              <from_clause dbname="$DB" resultname="$RESULTNAME" first_item="$FIRST" last_item="$LAST">
              </from_clause>
              <options>$OPTIONS</options>
            </request>
          </xf>
""")


class SearchArguments:

    _DEFAULT_OPTIONS = 'WORKER,NO_CORESULT'

    def __init__(self, context, where, order, options=_DEFAULT_OPTIONS):
        self.CONTEXT = context
        self.WHERE = where
        self.ORDER = order
        self.OPTIONS = options
        self.DB = 'RX'


# CALLER=args.caller, SELECT_ITEM=select_item, DB=database,
# RESULTNAME=resultName, FIRST=first, LAST=last, OPTIONS=options

class SelectItemsArguments:

    _DEFAULT_OPTIONS = 'OMIT_CIT'

    def __init__(self, selectItem: str, options: str = _DEFAULT_OPTIONS) -> object:
        self.DB = 'RX'
        self.SELECT_ITEM = selectItem
        self.OPTIONS = options


class RequiredArguments:

    _DEFAULT_RET_FILE_FORMAT = 'data_%d_%d.xml'

    def __init__(self, searchArgs, selectDataArgs, retFileFormat=_DEFAULT_RET_FILE_FORMAT):
        self.searchArgs = searchArgs
        self.selectDataArgs = selectDataArgs
        self.retFileFormat = retFileFormat


g_argsDict = {
    "target": RequiredArguments(
        SearchArguments('TGI', "TARGET.ED = '*'", "TARGET.ID asc"),
        SelectItemsArguments('''<select_item>TARGET</select_item>
                            <select_item>SUBUNIT</select_item>
                            <select_item>TOVERW</select_item>''')),
    "reaction": RequiredArguments(
        SearchArguments('R', "RX.ED = '*'", "RX.ID asc"),
        SelectItemsArguments('''<select_item>RX</select_item>
                            <select_item>RXNLINK</select_item>
                            <select_item>RXD</select_item>''')),
    "dat": RequiredArguments(
        SearchArguments('DPI', "DAT.ED = *", 'DAT.ID asc'),
        SelectItemsArguments('''<select_item>DAT</select_item><select_item>DATIDS</select_item>''')
    ),
    'yy_SLB': RequiredArguments(
        SearchArguments('S', "SLB.ED = *", 'IDE.XRN asc'),
        SelectItemsArguments('''<select_item>YY</select_item>''')
    ),
}

g_request_headers = {'Content-type': 'text/xml; charset="UTF-8"'}


def createSession():
    connect = connect_temp.substitute(CALLER=args.caller, USER=args.user,
                                      PASSWORD=args.password)
    response = requests.post(url=url, data=connect, headers=g_request_headers)
    cookies = response.cookies
    doc = ET.fromstring(response.text.encode('utf-8'))
    sessionId = doc.find('.//sessionid').text
    print(sessionId)
    return sessionId, cookies


def doSearch(cookies):
    searchArgs = g_argsDict[args.database].searchArgs
    if args.where is not None:
        searchArgs.WHERE = args.where
    searchReqData = select_temp.substitute(searchArgs.__dict__, CALLER=args.caller)

    searchResp = requests.post(url=url, data=searchReqData, cookies=cookies, headers=g_request_headers)

    print(searchResp.text)

    doc = ET.fromstring(searchResp.text)
    resultName = doc.find('.//resultname').text
    print("Hit Set Name: %s" % resultName)
    resultSize = int(doc.find('.//resultsize').text)
    print("Number of Hits: %s" % resultSize)

    return resultName, resultSize


def closeSession(sessionId, cookies):
    disconnect = disconnect_temp.substitute(CALLER=args.caller, SESSIONID=sessionId)
    requests.post(url=url, data=disconnect, headers=g_request_headers, cookies=cookies)


def process():
    ret = createSession()
    cookies = ret[1]
    resultName, resultTotalSize = doSearch(cookies)
    sessionChunk = 5000
    # 每次下载500条
    chunk = 500
    print("start database : " + args.database)
    startChunkNum = int(args.startChunkNum)
    chunk_number = startChunkNum
    sessionStartIndex = startChunkNum // (sessionChunk // chunk)
    for i in range(sessionStartIndex, resultTotalSize // sessionChunk + 1, 1):
        print("session start : " + str(i))
        ret = createSession()
        sessionId = ret[0]
        cookies = ret[1]

        resultName, resultTotalSize = doSearch(cookies)
        print("start time : ", time.asctime(time.localtime(time.time())))

        selectArgs = g_argsDict[args.database].selectDataArgs.__dict__
        retFileFormat = basicFilePath + g_argsDict[args.database].retFileFormat
        loopCount = sessionChunk // chunk
        if chunk_number != startChunkNum:
            chunk_number = i * loopCount
        startIndex = chunk_number * chunk + 1
        endIndex = startIndex + (loopCount - chunk_number % loopCount) * chunk
        endIndex = endIndex if endIndex <= resultTotalSize else resultTotalSize
        print(startIndex, endIndex)
        for datIndex in range(startIndex, endIndex, chunk):
            first = datIndex
            last = datIndex + chunk - 1

            retrieve = retrieve_temp.substitute(selectArgs,
                                                CALLER=args.caller, RESULTNAME=resultName, FIRST=first, LAST=last)
            # retrieve
            retrieve_response = requests.post(url=url, data=retrieve, cookies=cookies, headers=g_request_headers)
            retrieve_response.encoding = 'utf-8'
            ET.fromstring(retrieve_response.text)
            # save to file
            with open(retFileFormat % (chunk_number, datIndex), "wb") as f:
                f.write(retrieve_response.content)
            print('chunkNum : {}, datIndex :{}'.format(chunk_number, datIndex))
            chunk_number += 1

        print("end time : ", time.asctime(time.localtime(time.time())))

        closeSession(sessionId, cookies)

        sleep(10)


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument('-c', '--caller', default='GHDDI_oct2018')
    parser.add_argument('-s', '--server', default='www.reaxys.com')
    parser.add_argument('-u', '--user', default='')
    parser.add_argument('-p', '--password', default='')
    parser.add_argument('-d', '--database', required=True, type=str)
    parser.add_argument('-w', '--where', default=None)
    parser.add_argument('-sc', '--startChunkNum', default=0)
    parser.add_argument('-t', '--time', default='')
    args = parser.parse_args()

    url = "https://" + args.server + '/reaxys/api/v2'

    basicFilePath = f"/GHDDI/download/reaxys/{args.database.lower()}/{date.today().strftime('%y-%m-%d')}/"
    # basicFilePath = f"D:/code/python/reaxys/data/{args.database.lower()}/{date.today().strftime('%y-%m-%d')}/"
    if not os.path.exists(basicFilePath):
        os.makedirs(basicFilePath)
    process()
