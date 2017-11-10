# clean and concat calendar data

import pandas as pd

# Use this code localily, you need to 
# 1> change the path for review.csv 
# 2> change the path for output file
# I choose 6 cities: boston, chicago, LA, NY, SF, seattle
# The output file is 910MB, should take more than 5 minutes to wait.

try:
	c1 = pd.read_csv('your cvs file 1')
	c2 = pd.read_csv('your cvs file 2')
	c3 = pd.read_csv('your cvs file 3')
	c4 = pd.read_csv('your cvs file 4')
	c5 = pd.read_csv('your cvs file 5')
	c6 = pd.read_csv('your cvs file 6')

except IOError:
        print("Wrong file or file path")





def change_column(df):
	temp = df['date']
	df.drop(labels=['date'], axis=1,inplace = True)
	df.insert(0, 'date', temp)
	df['date'] =pd.to_datetime(df.date)
	return df.sort_values(by=["date"])

if __name__ == '__main__':
	boston = change_column(c1)
	chicago = change_column(c2)
	los_angeles = change_column(c3)
	new_york = change_column(c4)
	san_fran = change_column(c5)
	seattle = change_column(c6)

	combined = pd.concat([boston, chicago, los_angeles, new_york, san_fran, seattle])
	result = combined.sort_values(by=["date"])
	result.to_csv('output path/xxx.csv',index=False,header=False)
