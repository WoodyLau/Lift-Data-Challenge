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
    
if __name__ == '__main__':
    main()