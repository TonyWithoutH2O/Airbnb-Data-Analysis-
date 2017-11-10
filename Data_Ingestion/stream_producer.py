# connect to mysql, read table row by row and send it to
# you can edit the query_number to control how much rows you want to feed into kafka
import pymysql
import time
from kafka import KafkaProducer
import logging
import json
import atexit
import argparse
import getpass

# set default variable
mysql_host = None
mysql_user = None
mysql_password = None
mysql_port = None
mysql_database = None
mysql_table = None
topic_name = None
kafka_broker = None
query_number = 50

# set up logger to receive program running info
logger_format = '%(asctime)-15s %(message)s'
logging.basicConfig(format=logger_format)
logger = logging.getLogger('Stream Producer')
logger.setLevel(logging.INFO)

parser = argparse.ArgumentParser()
parser.add_argument('mysql_host', help = 'The address of MySQL server')
parser.add_argument('mysql_user', help = 'User identity for MySQL')
parser.add_argument('mysql_port', type = int, help = 'MySQL port')
parser.add_argument('mysql_database', help = 'The name for MySQL database')
parser.add_argument('mysql_table', help = 'The name for MySQL table')
parser.add_argument('kafka_broker', help = 'kafka_broker ip')
parser.add_argument('topic_name', help = 'topic name should be : calendar or review')

args = parser.parse_args()
mysql_host = args.mysql_host
mysql_user = args.mysql_user
mysql_port = args.mysql_port
mysql_database = args.mysql_database
mysql_table = args.mysql_table
kafka_broker = args.kafka_broker
topic_name = args.topic_name
mysql_password = getpass.getpass('MySQL user Password:')

# set up a connection to MySQL database
conn = pymysql.connect(host=mysql_host, user=mysql_user, password=mysql_password, port=mysql_port, database=mysql_database,cursorclass = pymysql.cursors.SSCursor)
cur = conn.cursor() 
cur.execute("SELECT * FROM %s LIMIT %d" % (mysql_table,query_number)) 
row = cur.fetchone()
# set up kafka producer
producer = KafkaProducer(bootstrap_servers=kafka_broker)

def produce_data(row):
	if topic_name == 'calendar':
		data = {
			'date': row[0],
			'listing_id': row[1],
			'available': row[2],
			'price': row[3]
		}
		data = json.dumps(data)
	else:
		data = {
			'date': row[0],
			'listing_id': row[1],
		}
		data = json.dumps(data)
	return data


try:
	while row is not None:
		data = produce_data(row)
		logger.info('Writing topic:%s to kafka producer %s' % (topic_name, kafka_broker))
		producer.send(topic = topic_name, value = data)
   		row = cur.fetchone()
   		time.sleep(0.5)

except KeyboardInterrupt:
   	row = cur.fetchall()
	logger.info('KeyboardInterrupt')
finally:
	logger.info('Closing KafkaProducer')
	producer.flush(10)
	producer.close(10)
	logger.info('KafkaProducer closed')
	logger.info('Trying to close connection to MySQL')
	cur.close()
	conn.close()
	logger.info('Connection to MySQL closed')
