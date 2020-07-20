from commonUtils.ghddiMysqlConnPool import getCHGGI208MysqlSingleConnection


conn = getCHGGI208MysqlSingleConnection()
cursor = conn.cursor()

cursor.execute()


conn.close()