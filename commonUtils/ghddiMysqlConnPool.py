import time
import mysql.connector.pooling as connectorPooling
import mysql.connector as connector

_g_208MysqlConf = {
    "host": "172.17.10.208",
    "user": "root",
    "passwd": "123456",
    "auth_plugin": "mysql_native_password",
    "ssl_disabled": True,
}


def getCHGGI208MysqlSingleConnection(database):
    return connector.connect(**{**_g_208MysqlConf, 'database' : database})


class GHDDI208MysqlConnPool:

    def _generatePool(self):
        conf = {
            "database": self._database,
            "pool_size": self._poolSize,
            "pool_name": self._poolName,
        }

        return connectorPooling.MySQLConnectionPool(**{**conf, **_g_208MysqlConf})

    def __init__(self, poolSize, database, poolName='ghddiPool'):
        self._poolSize = poolSize
        self._database = database
        self._poolName = poolName
        self._connPool = self._generatePool()

    def getDhddiConn(self):
        while True:
            try:
                return self._connPool.get_connection()
            except Exception as e:
                print("waiting for conn : " + e.__str__())
                time.sleep(1)
