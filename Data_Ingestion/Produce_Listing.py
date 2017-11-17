# This code is used for produce the detailed_listing into kafka 
# using as a changelog table.
import csv
import json
from kafka import KafkaProducer
import logging

logger_format = '%(asctime)-15s %(message)s'
logging.basicConfig(format=logger_format)
logger = logging.getLogger('For kafka Ktable topic')
logger.setLevel(logging.INFO)

csvfile = open('your_csv_file_path', 'r')

fieldnames = ('id', 'listing_url', 'name','transit','host_name', 
'neighbourhood', 'neighbourhood_cleansed', 'neighbourhood_group_cleansed',  
'city', 'state', 'zipcode', 'country_code', 'country',  'latitude', 'longitude', 
'property_type', 'room_type', 'security_deposit', 'cleaning_fee', 'minimum_nights','maximum_nights',    
'calendar_last_scraped','number_of_reviews', 'review_scores_rating', 
'review_scores_accuracy', 'review_scores_cleanliness', 'review_scores_checkin','review_scores_communication', 
'review_scores_location','review_scores_value','cancellation_policy','reviews_per_month')

reader = csv.DictReader(csvfile, fieldnames)
producer = KafkaProducer(bootstrap_servers='localhost:9092')

for row in reader:
    id = row['id']
    del row['id']
    data = json.dumps(row)
    producer.send(topic = 'detailed_listing', key = id,value = data)
    print('Keep writing data to kafka')


logger.info('Closing KafkaProducer')
producer.flush(10)
producer.close(10)
logger.info('KafkaProducer closed')