# Clean the detailed list csv file for 6 cities chosen
# You need to modify the read_csv path and to_csv path for your local
# If you add cities more than: Boston, LA, NYC, SF, SEA, Chicago, you need to check the fileds to
# make sure the field of the city you choose include in the scheme_dict 

import pandas as pd

try:
	c1 = pd.read_csv('your_detailed_listing_csv_file_path',low_memory=False)
	c2 = pd.read_csv('your_detailed_listing_csv_file_path',low_memory=False)
	c3 = pd.read_csv('your_detailed_listing_csv_file_path',low_memory=False)
	c4 = pd.read_csv('your_detailed_listing_csv_file_path',low_memory=False)
	c5 = pd.read_csv('your_detailed_listing_csv_file_path',low_memory=False)
	c6 = pd.read_csv('your_detailed_listing_csv_file_path',low_memory=False)

except IOError:
        print("Wrong file or file path")


scheme_dict = set([ 'id', 'listing_url','last_scraped',	'name',	'transit',	'host_id',	'host_url',
		    'host_name', 'host_response_rate', 'host_acceptance_rate', 'neighbourhood', 
		    'neighbourhood_cleansed', 'neighbourhood_group_cleansed',	'city',	'state', 'zipcode', 
		    'country_code', 'country',	'latitude', 'longitude', 'is_location_exact', 
		    'property_type', 'room_type', 'accommodates', 'bathrooms', 'bedrooms',
		    'beds', 'bed_type', 'square_feet', 'price', 'weekly_price', 'monthly_price', 
		    'security_deposit', 'cleaning_fee', 'minimum_nights','maximum_nights',	
		    'has_availability',	'calendar_last_scraped','number_of_reviews', 'first_review',	
		    'last_review', 'review_scores_rating', 'review_scores_accuracy', 
		    'review_scores_cleanliness', 'review_scores_checkin', 'review_scores_communication', 
		    'review_scores_location','review_scores_value','cancellation_policy','reviews_per_month'])

def clean_col(df):
	col_name = df.columns.tolist()
	for elem in col_name:
		if elem not in scheme_dict:
			df.drop(elem,axis=1, inplace=True)
	return df		

if __name__ == '__main__':
	boston = clean_col(c1)
	chicago = clean_col(c2)
	los_angeles = clean_col(c3)
	new_york = clean_col(c4)
	san_fran = clean_col(c5)
	seattle = clean_col(c6)
	combined = pd.concat([boston, chicago, los_angeles, new_york, san_fran, seattle])
	try:
		combined.to_csv('your_output_csv_file_path',index=False,header=False)
	except IOError:
		print("Wrong path for wirting file")
