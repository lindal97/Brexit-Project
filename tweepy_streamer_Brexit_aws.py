# -*- coding: utf-8 -*-
"""
Created on Wed Oct 31 16:10:09 2018

@author: Linda
"""

"""
Based on Charles Rahals' work in Aug 2016, modified to save data into sql database
Listen to Twitter for multiple tags on multiple different subjects at once.
Set:	yourtagone and yourtagtwo for firstthing
	yourtagthree and yourtagfour for secondthing
	api keys
Last modified: Nov 2018

an updated version to write directly to AWS RDS repository, running on AWS EC2
"""
import tweepy
from tweepy import OAuthHandler
import time
import pandas as pd
import csv
import json
import sys
import MySQLdb
import boto3



consumer_key = ''
consumer_secret = ''
access_token = ''
access_secret = ''
auth = OAuthHandler(consumer_key, consumer_secret)
auth.set_access_token(access_token, access_secret)
api = tweepy.API(auth, wait_on_rate_limit = True, wait_on_rate_limit_notify = True)
from tweepy import Stream
from tweepy.streaming import StreamListener
timestamp_previous= time.strftime("%Y%m%d_%H")
hourscounter=0


loginfo= {'host': 'lil-twitterdb.cthpgorb9tpf.eu-west-2.rds.amazonaws.com',
  'username': 'lindal97',
  'password': 'brexit2019',
  'db': 'lil-twitterdb'}

conn = MySQLdb.connect(loginfo['host'],loginfo['username'],loginfo['password'], loginfo['db'])
c=conn.cursor()

c.execute('''CREATE TABLE if NOT EXISTS users
             (number INT NOT NULL AUTO_INCREMENT,
             user_id CHAR(24) NOT NULL, user_screen_name CHAR(32), username CHAR(32),
             user_description VARCHAR, user_location CHAR(255), user_followers INT, 
             user_friends INT, user_created at CHAR(255), PRIMARY KEY(user_id)''')

c.execute('''CREATE TABLE if NOT EXISTS keyword1
             (id CHAR(32) NOT NULL, user_id CHAR(24), user_screen_name CHAR(32), username CHAR(32),
              created_at CHAR(100), fulltext VARCHAR, users_mentioned VARCHAR, url VARCHAR,
              url_title VARCHAR, in_reply_to_userid CHAR(24), in_reply_to_userscreename CHAR(32)
              retweet_uid CHAR(24), retweet_id CHAR(32), retweet VARCHAR, PRIMARY KEY(id))''')

c.execute('''CREATE TABLE if NOT EXISTS keyword2
             (id CHAR(32) NOT NULL, user_id CHAR(24), user_screen_name CHAR(32), username CHAR(32),
              created_at CHAR(100), fulltext VARCHAR, users_mentioned VARCHAR, url VARCHAR,
              url_title VARCHAR, in_reply_to_userid CHAR(24), in_reply_to_userscreename CHAR(32)
              retweet_uid CHAR(24), retweet_id CHAR(32), retweet VARCHAR,PRIMARY KEY(id))''')

class MyListener(StreamListener):
    def on_status(self, status):
        global hourscounter
        global timestamp_previous
        timestamp= time.strftime("%Y%m%d_%H")
        table=None
        if 'yourtagone' in status.text or 'yourtagtwo' in status.text:
            table='keyword1'
        elif 'yourtagthree' in status.text or 'yourtagfour' in status.text:
            table='keyword2'
        if table is not None:
            if timestamp!=timestamp_previous:
                hourscounter+=1
                print('Stream has been up for:',hourscounter,' hours')
        
            status.text=status.text.replace('\n', ' ')
            status.text=status.text.replace('\r', ' ')
            if status.user.description is not None:
                status.user.description=status.user.description.replace('\n', ' ')
                status.user.description=status.user.description.replace('\r', ' ')
            users_mentioned=''
            retweet_id=''
            retweet_uid=''
            retweet=''
            url=''
            url_title=''
            user_info = (status.user.id_str, status.user.screen_name, status.user.name, 
                         status.user.description, status.user.location,status.user.followers_count,
                       status.user.friends_count,status.user.created_at)
            uid=status.user.id_str
            c.execute('''SELECT * FROM users WHERE user_id=uid ''')
            re=c.fetchall()
            if len(re)>0:
                c.execute(''' INSERT INTO users
                      (user_id, user_screen_name, username,user_description, 
                      user_location, user_followers, user_friends, user_created_at)
                      VALUES (%s,%s,%s,%s,%s,%s,%s,%s))''', user_info)
            twitter = (status.id_str, status.user.id_str, status.user.screen_name, status.user.name, 
                       status.created_at, status.text, users_mentioned, url, url_title,
                       status.in_reply_to_user_id_str, status.in_reply_to_screen_name,
                       retweet_id, retweet_uid,retweet)
            if status.entities.user_mentions != []:
                for i in range(len(status.entities.user_mentions)):
                    user=status.entities.user_mentions[i].id_str
                    users_mentioned.append += user+', '
            if status.truncated is True:
                try:
                    twitter[5]=status.extended_tweet.full_text
                except:
                    pass
            if status.retweeted_status is not None:
                retweet_id=status.retweeted_status.id_str
                retweet_uid=status.retweeted_status.user.id_str
                retweet=status.retweeted_status.text
            if status.entities.urls.unwound is not None:
                url=status.entities.urls.unwound.url
                url_title=status.entities.urls.unwound.title
            order='insert into '+ table + '''(id, user_id, 
                user_screen_name, username, created_at, fulltext, 
                users_mentioned, url, url_title, in_reply_to_userid, 
                in_reply_to_userscreename, retweet_uid, retweet_id, retweet)
                VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)'''
            c.execute(order, twitter)
            conn.commit()
        timestamp_previous=time.strftime("%Y%m%d_%H")
    def on_error(self, status):
        print(status)
        conn.rollback()
        
twitter_stream = Stream(auth, MyListener())
while True:
    try: 
        twitter_stream.filter(track=['yourtagone','yourtagtwo','yourtagthree','yourtagfour'], lang='en',async=True)
        
    except:
        e = sys.exc_info()[0]
        print('ERROR:',e) 
        continue