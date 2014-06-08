from DBUtils.PooledDB import PooledDB
import MySQLdb
#unix_socket 
conn=MySQLdb.connect(host="localhost",user="root",passwd="root",port=3306,unix_socket='/data/mysql3306/run/mysql.sock',db="test",charset="utf8")
pool = PooledDB(mysql.connector, pool_size, database='test', user='root',  host='127.0.0.1')
pool = PooledDB(MySQLdb, pool_size, database='test', user='root',  host='127.0.0.1')

PooledDB(creator=MySQLdb, maxusage=120,host='localhost',user='root', passwd='root',db='test',unix_socket='/data/mysql3306/run/mysql.sock')
pool = PooledDB(creator=MySQLdb,
    maxusage=120,
    mincached=1,
    maxcached=20,
    pool_size=4,
    host='localhost',user='root', passwd='root',db='test',unix_socket='/data/mysql3306/run/mysql.sock')
