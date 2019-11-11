import sqlite3
from sqlite3 import Error
import pandas as pd
from functools import reduce
import time
import re
import scipy.stats
import pickle

def main():
	conn = connect()
	cursor = conn.cursor()
	pd.set_option('mode.chained_assignment',None)
	
	user = 'LynSAaDEr7OGtTVL5kedlg'
	#torontonians = get_torontonians(cursor)
	#pickle.dump(torontonians,open('torontonians.p','wb'))
	torontonians = pickle.load(open('torontonians.p','rb'))
	#vegas_restaurants = get_vegas_restaurants(cursor)
	#pickle.dump(vegas_restaurants,open('vegas_restaurants.p','wb'))
	vegas_restaurants = pickle.load(open('vegas_restaurants.p','rb'))
	#general_score = general_torontonian_view(cursor,torontonians,vegas_restaurants)
	#general_score.to_pickle("general_score.p")
	general_score = pd.read_pickle('general_score.p')
	personal_score = personal_view(cursor,user,vegas_restaurants)
	print(aggregate_scorer(cursor,general_score,personal_score, user))
	
	conn.close()
	
def connect():
	try:
		conn = sqlite3.connect(r"D:\\Work\\yelp_dataset.db")
 
	except Error as e:
		print(e)

	return conn

def get_torontonians(cursor):
	cursor.execute("select user_id, city, state, count() from review, business where review.business_id=business.business_id group by user_id,city,state")
	df = pd.DataFrame(cursor.fetchall())
	df.columns = ['user', 'city', 'state', 'reviews']
	r = re.compile(r'.*[tT]oronto.*')
	df['toronto'] = df.apply(lambda row: bool(r.match(row.city)) and row.state=='ON', axis=1)
	user_cities = df.groupby(['user','toronto']).sum().reset_index()
	possible_torontonian = user_cities[['user']].drop_duplicates()
	possible_torontonian = pd.merge(user_cities[user_cities['toronto']==1],possible_torontonian,on='user')[['user','reviews']].rename(columns={'reviews':'toronto_reviews'})
	possible_torontonian = pd.merge(user_cities[user_cities['toronto']==0],possible_torontonian,on='user')[['user','toronto_reviews','reviews']].rename(columns={'reviews':'not_toronto_reviews'})
	possible_torontonian['proportion'] = possible_torontonian['toronto_reviews']/(possible_torontonian['toronto_reviews']+possible_torontonian['not_toronto_reviews'])
	torontonian = set(possible_torontonian[possible_torontonian['proportion']>0.5]['user'])
	
	return torontonian
	
def get_vegas_restaurants(cursor):
	cursor.execute("select business_id from business where city like '%Vegas%' and state='NV' and categories like '%Restaurants%'")
	businesses = list(map(lambda x: x[0],cursor.fetchall()))
	return businesses

def general_torontonian_view(cursor,torontonians,restaurants):
	formatted_torontonians = "'"+"','".join(torontonians)+"'"
	formatted_restaurants = "'"+"','".join(restaurants)+"'"
	cursor.execute("select business_id, avg(stars) from review where user_id in (%s) and business_id in (%s) group by business_id"%(formatted_torontonians,formatted_restaurants))
	df = pd.DataFrame(cursor.fetchall())
	df.columns = ['business_id', 'stars']
	
	return df

def personal_view(cursor, user_id,restaurants):
	formatted_restaurants = "'"+"','".join(restaurants)+"'"
	cursor.execute("select cast(review.stars as real) as value,categories from review,business where review.business_id=business.business_id and user_id='"+user_id+"' and categories like '%Restaurants%' and value>=3")
	df = pd.DataFrame(cursor.fetchall())
	df.columns = ['stars', 'categories']
	
	five_stars = df[df['stars']==5]
	four_stars = df[df['stars']==4]
	three_stars = df[df['stars']==3]
	five_stars['categories']=five_stars['categories'].apply(lambda x: x.split(', ') if len(x)>0 else 'Restaurants')
	four_stars['categories']=four_stars['categories'].apply(lambda x: x.split(', ') if len(x)>0 else 'Restaurants')
	three_stars['categories']=three_stars['categories'].apply(lambda x: x.split(', ') if len(x)>0 else 'Restaurants')
	
	if len(five_stars)>0:
		five_star_categories = set(reduce(lambda x,y: x+y, five_stars['categories']))
	else:
		five_star_categories = set()
	if len(four_stars)>0:
		four_star_categories = set(reduce(lambda x,y: x+y, four_stars['categories']))
	else:
		four_star_categories = set()
	if len(three_stars)>0:
		three_star_categories = set(reduce(lambda x,y: x+y, three_stars['categories']))
	else:
		three_star_categories = set()
	
	cursor.execute("select business_id,cast(stars as real) as value,categories from business where business_id in (%s) and value>=3"%formatted_restaurants)
	df = pd.DataFrame(cursor.fetchall())
	df.columns = ['business_id', 'stars', 'categories']
	df['categories']=df['categories'].apply(lambda x: x.split(', '))
	df['five_intersection'] = df['categories'].apply(lambda x: len(five_star_categories.intersection(x)))
	df['four_intersection'] = df['categories'].apply(lambda x: len(four_star_categories.intersection(x)))
	df['three_intersection'] = df['categories'].apply(lambda x: len(three_star_categories.intersection(x)))
	
	df['rating'] = df.apply(lambda x: max(5*x.five_intersection*x.stars,4*x.four_intersection*x.stars,3*x.three_intersection*x.stars), axis=1)
	
	final = df[['business_id','rating']]
	
	return final
	
def aggregate_scorer(cursor, general_score, personal_score, user_id):
	cursor.execute("select count() from review where user_id='"+user_id+"'")
	count = int(cursor.fetchall()[0][0])
	
	proportion = min(count*0.08, 0.8)
	scores = pd.merge(general_score,personal_score, on='business_id').fillna(0)
	scores['aggregate_score'] = scores.apply(lambda row: row['rating']*proportion+row['stars']*(1-proportion), axis=1) 
	
	final = scores.sort_values(by='aggregate_score', ascending=False).head(5)
	final_businesses = "'"+"','".join(list(final['business_id']))+"'"
	
	cursor.execute("select business_id, name from business where business_id in (%s)"%final_businesses)
	df = pd.DataFrame(cursor.fetchall())
	df.columns = ['business_id', 'name']
	final = pd.merge(final,df, on='business_id')
	
	return final[['name','aggregate_score']]

if __name__ == '__main__':
	main()
