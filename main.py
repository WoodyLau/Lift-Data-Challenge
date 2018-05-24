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
    cursor.execute("select city,stars,review_count from business")
    rows = cursor.fetchall()
    
    df = pd.DataFrame(rows)
    df.columns = ['city', 'stars', 'reviews']
    
    justReviews = df.groupby(['city']).agg({'reviews':sum}).sort_values('reviews',ascending=False).head(20)
    starsReviews = df.groupby(['city']).agg({'reviews':sum,'stars':'mean'})
    starsReviews['Total'] = starsReviews.reviews * starsReviews.stars
    starsReviews = starsReviews.sort_values('Total',ascending=False).head(20)
    print justReviews
    print starsReviews
    
def Q3(cursor):
    cursor.execute("select id,name from business")
    rows = cursor.fetchall()
    business_id = filter(lambda x: x[1] == "Mon Ami Gabi", rows)[0][0]
    
    date = datetime.datetime.now() - datetime.timedelta(days=365)
    date.strftime('%Y-%m-%d')
    
    cursor.execute("select user_id,date from review where business_id='%s'"%business_id)
    rows = cursor.fetchall()
    df = pd.DataFrame(rows)
    df.columns = ['user_id', 'date']
    df = df[(df['date'] > date)]
    
    numberUsers = len(df.index)
    print numberUsers
    
def Q4(cursor):
    cursor.execute("select id,name from business")
    rows = cursor.fetchall()
    business_id = filter(lambda x: x[1] == "Chipotle Mexican Grill", rows)[0][0]
    
    cursor.execute("select text from review where business_id='%s'"%business_id)
    rows = cursor.fetchall()
    df = pd.DataFrame(rows)
    df.columns = ['text']
    df = df.head(10)
    
    allReviews = df['text'].tolist()
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
    cursor.execute("select id,name from business")
    rows = cursor.fetchall()
    business_id = filter(lambda x: x[1] == "Mon Ami Gabi", rows)[0][0]
    
    cursor.execute("select user_id from review where business_id='%s'"%business_id)
    users = "'"+ "', '".join(map(lambda x: x[0],cursor.fetchall())) + "'"
    
    cursor.execute("select count(*) from user where (review_count >= 11) and (id in (%s))"%users)
    count = float(cursor.fetchall()[0][0])
    cursor.execute("select count(*) from user")
    total = float(cursor.fetchall()[0][0])
    print str(count/total*100)+'%'
    
if __name__ == '__main__':
    main()