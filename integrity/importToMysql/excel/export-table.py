import mysql.connector
import xlwt

db = mysql.connector.connect(
    host="localhost",
    user="root",
    passwd="123456",
    database="ghddi",
    auth_plugin='mysql_native_password'
) 
cursor = db.cursor()
cursor.execute('show tables')
res = cursor.fetchall()

workbook = xlwt.Workbook()
worksheet = workbook.add_sheet('table_name')
row = 0
for line in res:
    tableName = str(line[0],'utf8')
    worksheet.write(row,0,tableName)
    row += 1
workbook.save('tableName.xlsx')