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
import pymysql
import boto3



consumer_key = 'h8FMRJuhE95FCPnOCKMZjsk3W'
consumer_secret = 'SfzG31fAs2r0gtPzN81WVoaeGy6gtiEnjbsFeN6AbXo5fIfE45'
access_token = '241046971-k1Z6p8STUA5NRNn2xPhEwildb1HoO7iutpcfHBLQ'
access_secret = 'LEg2Qip32CAStYeQGNQb5I0DC46jmWo2id9Kvp4A2nON4'
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

conn = pymysql.connect(loginfo['host'],loginfo['username'],loginfo['password'], loginfo['db'])
c=conn.cursor()

c.execute('''CREATE TABLE if NOT EXISTS users
             (number INT NOT NULL AUTO_INCREMENT,
             user_id CHAR(24) NOT NULL, user_screen_name CHAR(32), username CHAR(32),
             user_description VARCHAR, user_location CHAR(255), user_followers INT, 
             user_friends INT, user_created at CHAR(255), PRIMARY KEY(user_id)''')

c.execute('''CREATE TABLE if NOT EXISTS keyword1
             (id CHAR(32) NOT NULL, user_id CHAR(24), user_screen_name CHAR(32), username CHAR(32),
              created_at CHAR(100), fulltext VARCHAR, users_mentioned VARCHAR,
              in_reply_to_userid CHAR(24), in_reply_to_userscreename CHAR(32)
              retweet_uid CHAR(24), retweet_id CHAR(32), retweet VARCHAR, PRIMARY KEY(id))''')

c.execute('''CREATE TABLE if NOT EXISTS keyword2
             (id CHAR(32) NOT NULL, user_id CHAR(24), user_screen_name CHAR(32), username CHAR(32),
              created_at CHAR(100), fulltext VARCHAR, users_mentioned VARCHAR,
              in_reply_to_userid CHAR(24), in_reply_to_userscreename CHAR(32)
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
            text=status.text
            user_info = (status.user.id_str, status.user.screen_name, status.user.name, 
                         status.user.description, status.user.location,status.user.followers_count,
                       status.user.friends_count,status.user.created_at)
            uid=status.user.id_str
            c.execute('''SELECT * FROM users WHERE user_id LIKE ''' + uid)
            re=c.fetchall()
            if len(re)==0:
                c.execute(''' INSERT INTO users
                      (user_id, user_screen_name, username,user_description, 
                      user_location, user_followers, user_friends, user_created_at)
                      VALUES (%s,%s,%s,%s,%s,%s,%s,%s))''', user_info)
            
            if status.truncated is True:
                text=status.extended_tweet['full_text']
            if hasattr(status, 'retweeted_status'):
                retweet_id=status.retweeted_status.id_str
                retweet_uid=status.retweeted_status.user.id_str
                retweet=status.retweeted_status.text
            if hasattr(status, 'quote_status'):
                retweet_id=status.quoted_status.id_str
                retweet_uid=status.quoted_status.user.id_str
                retweet=status.quoted_status.text
            twitter = (status.id_str, status.user.id_str, status.user.screen_name, status.user.name, 
                       status.created_at, text, 
                       status.in_reply_to_user_id_str, status.in_reply_to_screen_name,
                       retweet_id, retweet_uid,retweet)
            order='insert into '+ table + '''(id, user_id, 
                user_screen_name, username, created_at, fulltext, 
                users_mentioned,  in_reply_to_userid, 
                in_reply_to_userscreename, retweet_uid, retweet_id, retweet)
                VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)'''
            twitter = (status.id_str, status.user.id_str, status.user.screen_name, status.user.name, 
                   status.created_at, status.text, users_mentioned, 
                   status.in_reply_to_user_id_str, status.in_reply_to_screen_name,
                   retweet_id, retweet_uid,retweet)
            c.execute(order, twitter)
            conn.commit()
        timestamp_previous=time.strftime("%Y%m%d_%H")
    def on_error(self, status):
        print(status)
        
twitter_stream = Stream(auth, MyListener())
while True:
    try: 
        twitter_stream.filter(track=['yourtagone','yourtagtwo','yourtagthree','yourtagfour'], lang='en',async=True)
        
    except:
        e = sys.exc_info()[0]
        print('ERROR:',e) 
        continue