import cx_Oracle
dsn_tns = cx_Oracle.makedsn('celclnx33.us.oracle.com', '45005', service_name='TARML')
conn = cx_Oracle.connect(user='SYSTEM', password='7geHHHms', dsn=dsn_tns)
c = conn.cursor()
c.execute('select instance_name from v$instance')
for row in c:
   print(row[0])
