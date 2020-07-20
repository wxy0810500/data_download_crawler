from string import Template
import requests
import xml.etree.ElementTree as ET
import argparse
from time import sleep
import math
import datetime

# gdb = mysql.connector.connect(
#     host="localhost",
#     user="root",
#     passwd="123456",
#     database="ghddi",
#     auth_plugin='mysql_native_password'
#     )
# cursor = gdb.cursor()
# cursor.execute('select max(TARGET_ID) from TARGET')
# maxId = cursor.fetchone()
# print('max target_id:', maxId)
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

headers = {'Content-type': 'text/xml; charset="UTF-8"'}


def getSession():
    connect = connect_temp.substitute(CALLER=args.caller, USER=args.user, PASSWORD=args.password)
    response = requests.post(url=url, data=connect, headers=headers)
    cookies = response.cookies
    doc = ET.fromstring(response.text.encode('utf-8'))
    sessionid = doc.find('.//sessionid').text
    return sessionid, cookies


def connectDatabase(where, database, context, options, order, cookies):
    select = select_temp.substitute(CALLER=args.caller, DB=database, CONTEXT=context, WHERE=where, ORDER=order,
                                    OPTIONS=options)
    select_response = requests.post(url=url, data=select, cookies=cookies, headers=headers)
    doc = ET.fromstring(select_response.text)
    resultname = doc.find('.//resultname').text
    # print "Hit Set Name: %s" % resultname
    resultsize = int(doc.find('.//resultsize').text)
    # print "Number of Hits: %s" % resultsize
    return resultname, resultsize


def loginout(sessionid, cookies):
    disconnect = disconnect_temp.substitute(CALLER=args.caller, SESSIONID=sessionid)
    requests.post(url=url, data=disconnect, headers=headers, cookies=cookies)


def process(dataBaseName, time):
    if time == None:
        today = datetime.datetime.now()
        offset = datetime.timedelta(days=-14)
        time = (today + offset).strftime('%Y/%m/%d')
    downloadOptions = 'OMIT_CIT'
    options = 'WORKER,NO_CORESULT'
    order = 'IDE.XRN asc'
    database = 'RX'
    context = 'S'
    if dataBaseName == 'IDE':
        print('pending')
        return
    elif dataBaseName == 'DAT':
        import insertDAT as insertXML
        select_item = """<select_item>DAT</select_item><select_item>DATIDS</select_item>"""
        where = "DAT.ED > " + time + " OR DAT.UPD > " + time
        context = 'DPI'
        order = 'DAT.ID asc'
    elif dataBaseName == 'TARGET':
        import insertTarget as insertXML
        select_item = '''<select_item>TARGET</select_item><select_item>SUBUNIT</select_item><select_item>TOVERW</select_item>'''
        where = "TARGET.ED > " + time + " OR TARGET.UPD > " + time
        context = 'TGI'
        order = 'TARGET.ID asc'
    elif dataBaseName == 'SLB':
        from untreated import insertSLB as insertXML
        select_item = '''<select_item>IDE.XRN</select_item><select_item>SLB</select_item><select_item>SLBP</select_item><select_item>SOLM</select_item>'''
        where = "SLB.ED > " + time
    elif dataBaseName == 'AZE':
        from untreated import insertAZE as insertXML
        select_item = '''<select_item>IDE.XRN</select_item><select_item>AZE</select_item>'''
        where = "AZE.ED > " + time
    elif dataBaseName == 'CDIC':
        from untreated import insertCDIC as insertXML
        select_item = '''<select_item>IDE.XRN</select_item><select_item>CDIC</select_item>'''
        where = "CDIC.ED > " + time
    elif dataBaseName == 'CIP':
        from untreated import insertCIP as insertXML
        select_item = '''<select_item>IDE.XRN</select_item><select_item>CIP</select_item>'''
        where = "CIP.ED > " + time
    elif dataBaseName == 'CSG':
        from untreated import insertCSG as insertXML
        select_item = '''<select_item>IDE.XRN</select_item><select_item>CSG</select_item>'''
        where = "CSG.ED > " + time
    elif dataBaseName == 'DIC':
        from untreated import insertDIC as insertXML
        select_item = '''<select_item>IDE.XRN</select_item><select_item>DIC</select_item>'''
        where = "DIC.ED > " + time
    elif dataBaseName == 'EDIS':
        from untreated import insertEDIS as insertXML
        select_item = '''<select_item>IDE.XRN</select_item><select_item>EDIS</select_item>'''
        where = "EDIS.ED > " + time
    elif dataBaseName == 'ELP':
        from untreated import insertELP as insertXML
        select_item = '''<select_item>IDE.XRN</select_item><select_item>ELP</select_item>'''
        where = "ELP.ED > " + time
    elif dataBaseName == 'EM':
        from untreated import insertEM as insertXML
        select_item = '''<select_item>IDE.XRN</select_item><select_item>EM</select_item>'''
        where = "EM.ED > " + time
    elif dataBaseName == 'ENEM':
        from untreated import insertENEM as insertXML
        select_item = '''<select_item>IDE.XRN</select_item><select_item>ENEM</select_item>'''
        where = "ENEM.ED > " + time
    elif dataBaseName == 'FLAP':
        from untreated import insertFLAP as insertXML
        select_item = '''<select_item>IDE.XRN</select_item><select_item>FLAP</select_item>'''
        where = "FLAP.ED> " + time
    elif dataBaseName == 'USE':
        from untreated import insertUSE as insertXML
        select_item = '''<select_item>IDE.XRN</select_item><select_item>USE</select_item>'''
        where = "USE.ED > " + time
    elif dataBaseName == 'HCOM':
        from untreated import insertHCOM as insertXML
        select_item = '''<select_item>IDE.XRN</select_item><select_item>HCOM</select_item>'''
        where = "HCOM.ED > " + time
    elif dataBaseName == 'IEP':
        from untreated import insertIEP as insertXML
        select_item = '''<select_item>IDE.XRN</select_item><select_item>IEP</select_item>'''
        where = "IEP.ED > " + time
    elif dataBaseName == 'IP':
        from untreated import insertIP as insertXML
        select_item = '''<select_item>IDE.XRN</select_item><select_item>IP</select_item>'''
        where = "IP.ED > " + time
    elif dataBaseName == 'MP_BP':
        from untreated import insertMP_BP as insertXML
        select_item = '''<select_item>IDE.XRN</select_item><select_item>MP</select_item><select_item>BP</select_item>'''
        where = "MP.ED ='*' or BP.ED = ‘*’"
    elif dataBaseName == 'ORD':
        from untreated import insertORD as insertXML
        select_item = '''<select_item>IDE.XRN</select_item><select_item>ORD</select_item>'''
        where = "ORD.ED > " + time
    elif dataBaseName == 'ORP':
        from untreated import insertORP as insertXML
        select_item = '''<select_item>IDE.XRN</select_item><select_item>ORP</select_item>'''
        where = "ORP.ED > " + time
    elif dataBaseName == 'POW':
        from untreated import insertPOW as insertXML
        select_item = '''<select_item>IDE.XRN</select_item><select_item>POW</select_item>'''
        where = "POW.ED > " + time
    elif dataBaseName == 'PSD':
        from untreated import insertPSD as insertXML
        select_item = '''<select_item>IDE.XRN</select_item><select_item>PSD</select_item><select_item>EXTID</select_item>'''
        where = "PSD.ED > " + time
    elif dataBaseName == 'SDIC':
        from untreated import insertSDIC as insertXML
        select_item = '''<select_item>IDE.XRN</select_item><select_item>SDIC</select_item>'''
        where = "SDIC.ED > " + time
    elif dataBaseName == 'SPE':
        from untreated import insertSPE as insertXML
        select_item = '''<select_item>IDE</select_item><select_item>NMR</select_item><select_item>IR</select_item><select_item>MS</select_item><select_item>UV</select_item><select_item>ESR</select_item><select_item>ROT</select_item><select_item>RAMAN</select_item><select_item>LUM</select_item><select_item>FLU</select_item><select_item>PHO</select_item><select_item>OSM</select_item>'''
        where = "NMR.ED = '*' or IR.ED = '*' or MS.ED = '*' or UV.ED = '*' or ESR.ED = '*' or NQR.ED = '*' or ROT.ED = '*' or RAMAN.ED = '*' or LUM.ED = '*' or FLU.ED = '*' or PHO.ED = '*' or OSM.ED = '*' "
    elif dataBaseName == 'QUAN':
        from untreated import insertQuan as insertXML
        select_item = '''<select_item>IDE.XRN</select_item><select_item>QUAN</select_item>'''
        where = "QUAN.ED > " + time
    elif dataBaseName == 'RX':
        from untreated import insertReaction as insertXML
        select_item = '''<select_item>RX</select_item><select_item>RXNLINK</select_item><select_item>RXD</select_item>'''
        where = "RX.ED > " + time + " OR RX.UPD > " + time
        context = 'R'
    elif dataBaseName == 'CIT':
        downloadOptions = ''
        print('pending')
        return
    else:
        print('waiting achieve')
    cs = getSession()
    result = connectDatabase(where.replace('*'), database, context, options, order, cs[1])
    size = result[1]
    chunk = 500
    for i in range(0, math.ceil(size / 100000), 1):
        chunk_number = 0
        # print "start time : ", time.asctime(time.localtime(time.time()))
        for datindex in range(1 + i * 100000, 100001 + i * 100000, chunk):
            chunk_number += 1
            first = datindex
            last = datindex + chunk - 1
            retrieve = retrieve_temp.substitute(CALLER=args.caller, SELECT_ITEM=select_item, DB=database,
                                                RESULTNAME=result[0], FIRST=first, LAST=last, OPTIONS=downloadOptions)
            retrieve_response = requests.post(url=url, data=retrieve, cookies=cs[0], headers=headers)
            f = open('%s/%s/data_%d_%d.txt' % (args.location, args.database, chunk_number, datindex), "w")
            content = retrieve_response.text.encode('utf-8')
            insertXML.doTask(content)
            f.write(content)
            f.close()
        sleep(30)
    loginout(cs[0], cs[1])


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument('-c', '--caller', required=True)
    parser.add_argument('-s', '--server', default='www.reaxys.com')
    parser.add_argument('-u', '--user', default='')
    parser.add_argument('-p', '--password', default='')
    parser.add_argument('-d', '--database', default='')
    parser.add_argument('-l', '--location', default='./')
    parser.add_argument('-t', '--time', default='')
    args = parser.parse_args()

    url = "https://" + args.server + '/reaxys/api'

    process(args.database)
