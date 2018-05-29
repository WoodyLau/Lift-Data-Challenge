import mysql.connector
from mysql.connector import Error
import pandas as pd
import datetime
import time

def main():
    conn = connect()
    cursor = conn.cursor()
    cursor.execute("use yelp_db")
    
    Q2(cursor)
    Q3(cursor)
    Q4(cursor)
    Q5(cursor)
    Q6()
    
    conn.close()
    
def connect():
    try:
        conn = mysql.connector.connect(host='localhost',
                                       database='yelp_db',
                                       user='root',
                                       password='password')
        if conn.is_connected():
            print('Connected to MySQL database')
 
    except Error as e:
        print(e)

    return conn

def Q2(cursor):
    print "\nQuestion 2: Top 20 cities with most reviews and best ratings"
    cursor.execute("select city,stars,review_count from business")
    rows = cursor.fetchall()
    
    df = pd.DataFrame(rows)
    df.columns = ['city', 'stars', 'reviews']
    
    #A dataframe of the top 20 resturants sorted by number of reviews
    justReviews = df.groupby(['city']).agg({'reviews':sum}).sort_values('reviews',ascending=False).head(20)
    
    #A dataframe of the top 20 resturants sorted by number of reviews, multiplied by star score
    starsReviews = df.groupby(['city']).agg({'reviews':sum,'stars':'mean'})
    starsReviews['Total'] = starsReviews.reviews * starsReviews.stars
    starsReviews = starsReviews.sort_values('Total',ascending=False).head(20)
    print "Top 20 sorted by just number of reviews"
    print justReviews
    print "Top 20 sorted by number of reviews and star score"
    print starsReviews
    
def Q3(cursor):
    print "\nQuestion 3: Number of users that reviewed Mon Ami Gabi in the past year"
    cursor.execute("select id,name from business")
    rows = cursor.fetchall()
    business_id = filter(lambda x: x[1] == "Mon Ami Gabi", rows)[0][0]
    
    date = datetime.datetime.now() - datetime.timedelta(days=365)
    date.strftime('%Y-%m-%d')
    
    cursor.execute("select user_id,date from review where business_id='%s'"%business_id)
    df = pd.DataFrame(cursor.fetchall())
    df.columns = ['user_id', 'date']
    df = df[(df['date'] > date)]
    
    numberUsers = len(df.index)
    print numberUsers
    
def Q4(cursor):
    print "\nQuestion 4: Most common words in the business Chipotle Mexican Grill"
    cursor.execute("select id,name from business")
    rows = cursor.fetchall()
    business_id = filter(lambda x: x[1] == "Chipotle Mexican Grill", rows)[0][0]
    
    cursor.execute("select text from review where business_id='%s'"%business_id)
    rows = cursor.fetchall()
    df = pd.DataFrame(rows)
    df.columns = ['text']
    df = df.head(10)
    
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
    allWords = filter(lambda x: x != "", allWords)
 
    df2 = pd.DataFrame(allWords)[0].value_counts().head(10)
    print df2
    
def Q5(cursor):
    print "\nQuestion 5: Percentage of users who reviewed Mon Ami Gabi and also reviewed ten restaurants in the US"
    cursor.execute("select id,name from business")
    rows = cursor.fetchall()
    business_id = filter(lambda x: x[1] == "Mon Ami Gabi", rows)[0][0]
    
    # Get the users that went to Mon Ami Gabi
    cursor.execute("select user_id from review where business_id='%s'"%business_id)
    all = map(lambda x: x[0],cursor.fetchall())
    users = "'"+ "', '".join(all) + "'"
    totalBusiness = len(all)
    
    cursor.execute("select state_code from us_locations.us_states")
    usStates = "'"+ "', '".join(map(lambda x: x[0],cursor.fetchall())) + "'"
    
    # Get the businesses those users went to
    cursor.execute("select business_id from review where (user_id in (%s))"%users)
    df = pd.DataFrame(cursor.fetchall()).drop_duplicates()
    df.columns = ['business_id']
    allBusinesses = "'"+ "', '".join(df.business_id.values) + "'"
    
    # Check if the businesses are in the US
    cursor.execute("select id from business where (id in (%s)) and (state in (%s))"%(allBusinesses,usStates))
    usBusinesses = "'"+ "', '".join(map(lambda x: x[0],cursor.fetchall())) + "'"
    
    # Get the number of people who went to Mon Ami Gabi and 10 US businesses
    cursor.execute("select business_id,user_id from review where (user_id in (%s)) and (business_id in (%s))"%(users,usBusinesses))
    df = pd.DataFrame(cursor.fetchall()).drop_duplicates()
    df.columns = ['business_id', 'user_id']
    counts = df.user_id.value_counts()
    count = float(len(counts[counts >= 10].index))
    
    cursor.execute("select count(*) from user")
    total = cursor.fetchall()[0][0]
    print "Percentage of total number of users"
    print str(count/total*100)+'%'
    print "Percentage of users that reviewed Mon Ami Gabi"
    print str(count/totalBusiness*100)+'%'
    
def Q6():
    print "\nQuestion 6:"
    print "There should be some normalizing of the star value, based off of the average star level of the user"
    print "There is also the option of modifying the star score slightly based off the useful, funny, or cool ratings"
    print "\nInstead of arranging by city, it might be useful to figure out popularity based off of latitude and longitude"
    
def output():
    output = """
Connected to MySQL database

Question 2: Top 20 cities with most reviews and best ratings
Top 20 sorted by just number of reviews
                 reviews
city
Las Vegas        1604173
Phoenix           576709
Toronto           430923
Scottsdale        308529
Charlotte         237115
Pittsburgh        179471
Henderson         166884
Tempe             162772
Mesa              130883
Montreal          122620
Chandler          122343
Gilbert            97204
Cleveland          92280
Madison            86614
Glendale           76293
Edinburgh          48838
Mississauga        43147
Peoria             42581
Markham            38840
North Las Vegas    37928
Top 20 sorted by number of reviews and star score
                 reviews     stars         Total
city
Las Vegas        1604173  3.709916  5.951347e+06
Phoenix           576709  3.673793  2.118710e+06
Toronto           430923  3.487272  1.502746e+06
Scottsdale        308529  3.948529  1.218236e+06
Charlotte         237115  3.571554  8.468690e+05
Pittsburgh        179471  3.629819  6.514473e+05
Henderson         166884  3.789362  6.323838e+05
Tempe             162772  3.729885  6.071209e+05
Mesa              130883  3.636024  4.758938e+05
Chandler          122343  3.753380  4.591998e+05
Montreal          122620  3.706604  4.545037e+05
Gilbert            97204  3.838875  3.731540e+05
Cleveland          92280  3.589103  3.312024e+05
Madison            86614  3.635543  3.148889e+05
Glendale           76293  3.622583  2.763777e+05
Edinburgh          48838  3.787099  1.849544e+05
Peoria             42581  3.700762  1.575821e+05
Mississauga        43147  3.306493  1.426653e+05
North Las Vegas    37928  3.482771  1.320945e+05
Markham            38840  3.299552  1.281546e+05

Question 3: Number of users that reviewed Mon Ami Gabi in the past year
551

Question 4: Most common words in the business Chipotle Mexican Grill
the     52
i       47
and     33
it      31
a       26
to      24
that    24
is      23
my      21
was     21
Name: 0, dtype: int64

Question 5: Percentage of users who reviewed Mon Ami Gabi and also reviewed ten restaurants in the US
Percentage of total number of users
0.178870236882%
Percentage of users that reviewed Mon Ami Gabi
32.2195055691%

Question 6:
There should be some normalizing of the star value, based off of the average star level of the user
There is also the option of modifying the star score slightly based off the useful, funny, or cool ratings

Instead of arranging by city, it might be useful to figure out popularity based off of latitude and longitude
"""
    
if __name__ == '__main__':
    main()