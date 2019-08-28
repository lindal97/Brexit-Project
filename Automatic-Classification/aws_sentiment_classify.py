# -*- coding: utf-8 -*-
"""
Created on Mon Jun  3 14:37:56 2019

For sentiment analysis oF a set of tweets in a MySQL database on AWS platform.
"""

import pymysql
import pandas as pd
import os

from sklearn.feature_extraction.text import CountVectorizer, TfidfVectorizer
from sklearn.ensemble import RandomForestClassifier
from sklearn.externals import joblib
from sklearn.pipeline import Pipeline
from nltk.tokenize import TweetTokenizer 

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

#os.chdir("D:/grad/MA thesis/data")
tfv = joblib.load('tfidf_transform.pkl')
forest = joblib.load('random_forest_sentiment.pkl')

def predict_pipeline(tweet):
    tfv_tweet = tfv.transform([tweet])
    return int(forest.predict(tfv_tweet[0]))

tagged = pd.read_csv("tag_label.csv", encoding = "latin-1")
sentiment_labels = {}
for i in range(len(tagged)):
    score = tagged["tendency"].iloc[i]-2
    tag = tagged["tag"].iloc[i][1:]
    sentiment_labels[tag] = score
sentiment_labels['peoplesvote'] = -1 


tw = TweetTokenizer(preserve_case = True)    
                                  
def leave_remain_caculator(tweet):
    score = 0
    tokens = tw.tokenize(tweet)
    for t in tokens:
        t = t.lower()
        for label in sentiment_labels.keys():
            if t == label or t[1:] == label:
                score += sentiment_labels[label]
    return int(score)

loginfo= load_token('SQL auth.txt')
conn = pymysql.connect(loginfo[0],loginfo[1],loginfo[2],loginfo[3])
c = conn.cursor()
#c.execute('''ALTER TABLE twitter ADD COLUMN sentiment INT''')
#c.execute('''ALTER TABLE twitter ADD COLUMN leave_remain INT''')
c.execute('''SELECT MAX(serialno) FROM twitter''')
tweetnum = c.fetchone()[0]

#for i in range(1, tweetnum+1):
for i in range(571000, tweetnum+1):
    c.execute('''SELECT * FROM twitter WHERE serialno = '''+str(i))
    tweet = c.fetchone()
    if tweet is not None:
        try:
            full_text = tweet[5]
            score = leave_remain_caculator(full_text)
            senti = predict_pipeline(full_text)
            c.execute('''UPDATE twitter SET sentiment = ''' + str(senti) +
                        ''' WHERE serialno = ''' + str(i))
            c.execute('''UPDATE twitter SET leave_remain = ''' + str(score) +
                        ''' WHERE serialno = ''' + str(i))
            conn.commit()
        except:
            pass
    if i % 1000 == 0:
        print("{} tweets analyzed!".format(i))
    
conn.close()










