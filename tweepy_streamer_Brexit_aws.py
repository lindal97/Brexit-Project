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

an updated version to write directly to AWS EBS repository, running on AWS EC2
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
import logging

logging.basicConfig(level = logging.INFO, datefmt = '%a, %d %b %Y %H:%M:%S',
                    format='%(asctime)s %(levelname)s %(message)s',
                    filename='twitter_scraper.log')

'''
note:the load_token takes a .txt file with these auth in sequence
consumer key, consumer secret, access token, access secret
'''

def load_token(textfile):
    try:
        with open(textfile, 'r') as file:
            auth = file.readlines()
            keys = []
            for i in auth:
                i = str(i).strip()
                keys.append(i)
        return keys
    except EnvironmentError:
        print('Error loading access token from file')


auth=load_token("auth.txt") 

costumer_token=auth[0]
costumer_key=auth[1]
access_token=auth[2]
access_key=auth[3]

auth = OAuthHandler(costumer_token, costumer_key)
auth.set_access_token(access_token, access_key)
api = tweepy.API(auth, wait_on_rate_limit = True, wait_on_rate_limit_notify = True)
from tweepy import Stream
from tweepy.streaming import StreamListener
timestamp_previous = time.strftime("%Y%m%d_%H")
hourscounter = 0


loginfo= load_token('SQL auth.txt')


conn = pymysql.connect(loginfo[0],loginfo[1],loginfo[2],loginfo[3])


c = conn.cursor()

c.execute('''CREATE TABLE if NOT EXISTS users(user_id CHAR(24) NOT NULL, user_screen_name CHAR(32), username CHAR(32),
             user_description TEXT(512), user_location CHAR(100), user_followers INT, 
             user_friends INT, user_created_at VARCHAR(100), PRIMARY KEY(user_id));''')

c.execute('''CREATE TABLE if NOT EXISTS twitter (
              id CHAR(32) NOT NULL,
              user_id CHAR(24),
              user_screen_name CHAR(32),
              username CHAR(32),
              created_at VARCHAR(100),
              full_text TEXT(512),
              in_reply_to_userid CHAR(24),
              in_reply_to_userscreename CHAR(32),
              retweet_uid CHAR(24),
              retweet_id CHAR(32),
              retweet TEXT(512),
              PRIMARY KEY(id));''')


class MyListener(StreamListener):
    def on_status(self, status):
        global hourscounter
        global timestamp_previous
        timestamp = time.strftime("%Y%m%d_%H")
        table = None
        for k in keywords:
            if k in status.text:
                table='twitters'
                break
        if table is not None:
            if timestamp!=timestamp_previous:
                hourscounter += 1
                logging.info('Stream has been up for:',hourscounter,' hours')
                logging.log("sample streaming data now: " + str(twitter) + " ," + str(user))
            status.text=status.text.replace('\n', ' ')
            status.text=status.text.replace('\r', ' ')
            if status.user.description is not None:
                status.user.description=status.user.description.replace('\n', ' ')
                status.user.description=status.user.description.replace('\r', ' ')
            users_mentioned=''
            retweet_id = ''
            retweet_uid = ''
            retweet = ''
            text = status.text
            user_info = (status.user.id_str, status.user.screen_name, status.user.name, 
                         status.user.description, status.user.location,status.user.followers_count,
                       status.user.friends_count,status.user.created_at)
            uid = status.user.id_str
            c.execute('''SELECT * FROM users WHERE user_id LIKE ''' + uid)
            re = c.fetchall()
            if len(re) == 0:
                c.execute(''' INSERT INTO users
                      (user_id, user_screen_name, username,user_description, 
                      user_location, user_followers, user_friends, user_created_at)
                      VALUES (%s,%s,%s,%s,%s,%s,%s,%s))''', user_info)
                      
            if status.truncated is True:
                text=status.extended_tweet['full_text']
            if hasattr(status, 'retweeted_status'):
                retweet_id = status.retweeted_status.id_str
                retweet_uid = status.retweeted_status.user.id_str
                retweet = status.retweeted_status.text
            if hasattr(status, 'quote_status'):
                retweet_id = status.quoted_status.id_str
                retweet_uid = status.quoted_status.user.id_str
                retweet=status.quoted_status.text
            twitter = (status.id_str, status.user.id_str, status.user.screen_name, status.user.name, 
                       status.created_at, text, 
                       status.in_reply_to_user_id_str, status.in_reply_to_screen_name,
                       retweet_id, retweet_uid,retweet)
            order = 'insert into '+ table + '''(id, user_id, 
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
        logging.warning(status)
        
keywords=load_token('keywords.txt')

        
twitter_stream = Stream(auth, MyListener())
while True:
    try: 
        twitter_stream.filter(track=keywords, async=True)
    except TypeError:
        e = sys.exc_info()[0]
        logging.error(e) 
        time.sleep(60)
        continue
