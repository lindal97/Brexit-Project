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
Last modified: August 2016
"""
import tweepy
from tweepy import OAuthHandler
import time
import pandas as pd
import csv
import json
import sys
import sqlite3 as sqlite


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

conn= sqlite.connect('insert path here/brexit')
c = conn.cursor()
c.execute('''CREATE TABLE if NOT EXISTS users
             (user_id unique, user_screen_name text, username text,
             user_description text, user_location text, user_followers int, 
             user_friends int, user_created at text,)''')

c.execute('''CREATE TABLE if NOT EXISTS keyword1
             (id unique, user_id text, user_screen_name text, username text,
              created_at text, fulltext text, users_mentioned text, url text,
              url_title text, in_reply_to_userid text, in_reply_to_userscreename text
              retweet_uid text, retweet_id text, retweet text)''')

c.execute('''CREATE TABLE if NOT EXISTS keyword2
             (id unique, user_id text, user_screen_name text, username text,
              created_at text, fulltext text, users_mentioned text, url text, 
              url_title text,in_reply_to_userid text, in_reply_to_userscreename text
              retweet_uid text, retweet_id text, retweet text)''')


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
                c.execute(''' insert or ignore into users
                      (user_id, user_screen_name, username,user_description, 
                      user_location, user_followers, user_friends, user_created at),
                      values(?, ?, ?, ?, ?, ?, ?, ?) ''',  user_info)
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
            order='insert or ignore into '+ table + '''(id, user_id, 
                user_screen_name, username, created_at, fulltext, 
                users_mentioned, url, url_title, in_reply_to_userid, 
                in_reply_to_userscreename, retweet_uid, retweet_id, retweet) 
		values(?,?,?,?,?,?,?,?,?,?,?,?,?,?)'''
            c.execute(order, twitter)
            conn.commit()
        timestamp_previous=time.strftime("%Y%m%d_%H")

    def on_error(self, status):
        print(status)
        
        
twitter_stream = Stream(auth, MyListener())
while True:
    try: 
        twitter_stream.filter(track=['yourtagone','yourtagtwo','yourtagthree','yourtagfour'], lang='en')
    except:
        e = sys.exc_info()[0]
        print('ERROR:',e) 
        continue
