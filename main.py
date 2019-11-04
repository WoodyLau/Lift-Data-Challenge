import sqlite3
from sqlite3 import Error
import pandas as pd
import datetime
import time
import json
from functools import reduce
import scipy.stats

def main():
	conn = connect()
	cursor = conn.cursor()
	
	Q2(cursor)
	Q3(cursor)
	Q4(cursor)
	Q5(cursor)
	Q6(cursor)
	
	conn.close()
	
def connect():
	try:
		conn = sqlite3.connect(r"D:\\Work\\yelp_dataset.db")
 
	except Error as e:
		print(e)

	return conn

def Q2(cursor):
	print("\nQuestion 2: Top 10 restaurants in Toronto with the highest popularity")
	cursor.execute("select name,CAST(stars as REAL), CAST(review_count as int) from business where city='Toronto' and categories like '%Restaurants%'")
	rows = cursor.fetchall()
	
	df = pd.DataFrame(rows)
	df.columns = ['name', 'stars', 'reviews']
	
	#A dataframe of the top 20 resturants sorted by number of reviews
	justReviews = df.sort_values(['reviews','stars'],ascending=False).head(10)
	
	#A dataframe of the top 20 resturants sorted by number of reviews, multiplied by star score
	starsReviews = df
	starsReviews['Total'] = starsReviews.reviews * starsReviews.stars
	starsReviews = starsReviews.sort_values('Total',ascending=False).head(10)
	print("Top 20 sorted by just number of reviews")
	print(justReviews)
	print("Top 20 sorted by number of reviews and star score")
	print(starsReviews)
	
def Q3(cursor):
	print("\nQuestion 3: Number of Canadian residents that reviewed Mon Ami Gabi in the past year")
	cursor.execute("select user_id, state, count() from review, business where review.business_id=business.business_id group by user_id,state")
	rows = cursor.fetchall()
	df = pd.DataFrame(rows)
	df.columns = ['user', 'state', 'reviews']
	df['country'] = df.apply(lambda row: 1 if row.state in ['ON', 'AB', 'QC', 'BC', 'MB', 'NB', 'NL', 'NS', 'NT', 'NU', 'PE', 'SK', 'YT'] else 0, axis=1)
	with_canadian_review = set(df[df['country']==1]['user'])
	df = df[df['user'].isin(with_canadian_review)]
	user_countries = df.groupby(['user','country']).sum().reset_index()
	possible_canadians=user_countries[['user']].drop_duplicates()
	possible_canadians = pd.merge(user_countries[user_countries['country']==1],possible_canadians,on='user')[['user','reviews']].rename(columns={'reviews':'canadian_reviews'})
	possible_canadians = pd.merge(user_countries[user_countries['country']==0],possible_canadians,on='user')[['user','canadian_reviews','reviews']].rename(columns={'reviews':'not_canadian_reviews'})
	possible_canadians['proportion'] = possible_canadians['canadian_reviews']/(possible_canadians['canadian_reviews']+possible_canadians['not_canadian_reviews'])
	canadians = set(possible_canadians[possible_canadians['proportion']>0]['user'])

	date = datetime.datetime.now() - datetime.timedelta(days=365)
	date.strftime('%Y-%m-%d')
	
	cursor.execute("select user_id,date from review where business_id in (select business_id from business where name='Mon Ami Gabi')")
	df = pd.DataFrame(cursor.fetchall())
	df.columns = ['user_id', 'date']
	users=set(df['user_id'])
	df['date'] = df['date'].apply(lambda x: datetime.datetime.strptime(x,'%Y-%m-%d %H:%M:%S'))
	df = df.groupby(['user_id']).max().reset_index()
	test=df
	df = df[(df['date'] > date) & (df['user_id'].isin(canadians))]
	
	numberUsers = len(df.index)
	
	print(numberUsers)
	
def Q4(cursor):
	print("\nQuestion 4: Most common words in the business Chipotle Mexican Grill")
	cursor.execute("select text from review where business_id in (select business_id from business where name='Chipotle Mexican Grill')")
	rows = cursor.fetchall()
	df = pd.DataFrame(rows)
	df.columns = ['text']
	
	allReviews = df['text'].tolist()
	# Turn into a list of all the words used, split by spaces
	allWords = reduce(lambda x,y: x+y, map(lambda x: x.split(), allReviews))
	allWords = map(lambda x: x.lower().replace(".","")
									  .replace(",","")
									  .replace("(","")
									  .replace(")","")
									  .replace("!","")
									  .replace("@","")
									  .replace("&","")
									  .replace("-","")
									  .replace(":",""), allWords)
									  
	filtered_words = ["","the","a","i","and","for","nor","but","or","yet","so",
					  "of","with","at","from","into","to","in","for","on","by",
					  "about","is","was","are","it","this","be","my","me","you",
					  "they","that","have","had","has","not","there","as","when","why"]
	allWords = filter(lambda x: x not in filtered_words, allWords)
 
	df2 = pd.DataFrame(list(allWords))[0].value_counts().head(10)
	print(df2)
	
def Q5(cursor):
	print("\nQuestion 5: Percentage of users who reviewed Mon Ami Gabi and also reviewed ten restaurants in ON")

	# Get the users that went to Mon Ami Gabi
	cursor.execute("select distinct(user_id) from review where business_id in (select business_id from business where name='Mon Ami Gabi')")
	all = list(map(lambda x: x[0],cursor.fetchall()))
	totalBusiness = len(all)
	users = "'"+ "', '".join(all) + "'"
	

	ONBusinesses = "select distinct(business_id) from business where state='ON' and categories like '%Restaurants%'"
	
	# Get the number of people who went to Mon Ami Gabi and 10 ON businesses
	cursor.execute("select user_id,business_id from review where (user_id in (%s)) and (business_id in (%s))"%(users,ONBusinesses))
	df = pd.DataFrame(cursor.fetchall()).drop_duplicates()
	df.columns = ['user_id','business_id']
	counts = df.user_id.value_counts()
	count = float(len(counts[counts >= 10].index))
	
	cursor.execute("select count(*) from user")
	total = cursor.fetchall()[0][0]
	print("Percentage of total number of users")
	print(str(count/total*100)+'%')
	print("Percentage of users that reviewed Mon Ami Gabi")
	print(str(count/totalBusiness*100)+'%')
	
def Q6(cursor):
	print("\nQuestion 6:")
	print("Whether providing takeout affects ratings")
	cursor.execute("select stars from business where attributes like '%RestaurantsTakeOut_: _True%' and categories like '%Restaurants%' and categories like '%Chinese%'")
	with_takeout = pd.DataFrame(cursor.fetchall())
	cursor.execute("select stars from business where (attributes like '%RestaurantsTakeOut_: _False%' or attributes not like '%RestaurantsTakeOut%') and categories like '%Restaurants%' and categories like '%Chinese%'")
	without_takeout = pd.DataFrame(cursor.fetchall())
	print("Mean with takeout")
	print(with_takeout[0].apply(float).mean())
	print("Mean without takeout")
	print(without_takeout[0].apply(float).mean())
	print(scipy.stats.ks_2samp(list(with_takeout),list(without_takeout)))
	
	print("Whether serving alcohol affects their ratings.")
	cursor.execute("select stars from business where attributes like '%Alcohol%' and attributes not like '%Alcohol_: _u_none%' and attributes not like '%Alcohol_: __none%' and attributes not like '%Alcohol_: _none%' and categories like '%Restaurants%' and categories like '%Nightlife%'")
	with_alcohol = pd.DataFrame(cursor.fetchall())
	cursor.execute("select stars from business where (attributes like '%Alcohol_: _u_none%' or attributes like '%Alcohol_: __none%' or attributes like '%Alcohol_: _none%') and categories like '%Restaurants%' and categories like '%Nightlife%'")
	without_alcohol = pd.DataFrame(cursor.fetchall())
	print("Mean with alcohol")
	print(with_alcohol[0].apply(float).mean())
	print("Mean without alcohol")
	print(without_alcohol[0].apply(float).mean())
	print(scipy.stats.ks_2samp(list(with_alcohol),list(without_alcohol)))
	
	
def output():
	output = """
Question 2: Top 10 restaurants in Toronto with the highest popularity
Top 20 sorted by just number of reviews
                                   name  stars  reviews
5228          Pai Northern Thai Kitchen    4.5     2121
6698                      Khao San Road    4.0     1410
7370             KINKA IZAKAYA ORIGINAL    4.0     1397
4730       Seven Lives Tacos Y Mariscos    4.5     1152
4138                       Banh Mi Boys    4.5     1045
7318  Uncle Tetsu's Japanese Cheesecake    3.5      939
958                 Momofuku Noodle Bar    3.0      897
7254              Salad King Restaurant    3.5      876
2200                          Gusto 101    4.0      836
3030       Insomnia Restaurant & Lounge    4.0      795
Top 20 sorted by number of reviews and star score
                                   name  stars  reviews   Total
5228          Pai Northern Thai Kitchen    4.5     2121  9544.5
6698                      Khao San Road    4.0     1410  5640.0
7370             KINKA IZAKAYA ORIGINAL    4.0     1397  5588.0
4730       Seven Lives Tacos Y Mariscos    4.5     1152  5184.0
4138                       Banh Mi Boys    4.5     1045  4702.5
2200                          Gusto 101    4.0      836  3344.0
7318  Uncle Tetsu's Japanese Cheesecake    3.5      939  3286.5
3030       Insomnia Restaurant & Lounge    4.0      795  3180.0
4439                     Sansotei Ramen    4.0      794  3176.0
1857                             Byblos    4.5      700  3150.0

Question 3: Number of Canadian residents that reviewed Mon Ami Gabi in the past year
2

Question 4: Most common words in the business Chipotle Mexican Grill
chipotle    8721
food        7184
location    4886
burrito     4026
get         3832
one         3748
time        3635
out         3602
like        3577
just        3489
Name: 0, dtype: int64

Question 5: Percentage of users who reviewed Mon Ami Gabi and also reviewed ten restaurants in ON
Percentage of total number of users
0.0036649323392407974%
Percentage of users that reviewed Mon Ami Gabi
0.7186489399928134%

Question 6:
Whether providing takeout affects ratings
Mean with takeout
3.3211538461538463
Mean without takeout
3.2051526717557253
Ks_2sampResult(statistic=0.0, pvalue=1.0)
Whether serving alcohol affects their ratings.
Mean with alcohol
3.4952297769416822
Mean without alcohol
3.6763005780346822
Ks_2sampResult(statistic=0.0, pvalue=1.0)
"""
	
if __name__ == '__main__':
	main()
