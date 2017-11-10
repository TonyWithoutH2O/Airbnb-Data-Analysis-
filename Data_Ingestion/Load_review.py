# write csv file to mysql db
# what you need to do in your local:
# 1> change mysql info
# 2> change the name for your table within SQL query

import pymysql

# mysql info
mysql_host = ''
mysql_user = ''
mysql_password = ''
mysql_port = 1234
mysql_database = ''

conn = pymysql.connect(host=mysql_host, user=mysql_user, password=mysql_password, port=mysql_port, database=mysql_database, local_infile=True)
cur = conn.cursor()

try:
    with conn.cursor() as cursor:
        # Create a new record
        sql = """CREATE TABLE your_table_name(date VARCHAR(20) NOT NULL, listing_id VARCHAR(20) NOT NULL, PRIMARY KEY(date,listing_id))"""
        cursor.execute(sql)
    # connection is not autocommit by default. So you must commit to save
    # your changes.
    conn.commit()

    with conn.cursor() as cursor:
        # Read a single record
        # path: /AirbnbProject/dataset
        sql = """LOAD DATA LOCAL INFILE ‘your_csv_file_name’ INTO TABLE review_small FIELDS TERMINATED BY ',' ENCLOSED BY '"' LINES TERMINATED BY '\n';"""
        cursor.execute(sql)
    conn.commit()
finally:
    conn.close()