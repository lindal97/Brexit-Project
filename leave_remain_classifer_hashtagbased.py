# -*- coding: utf-8 -*-
"""
Created on Sat May 25 14:49:15 2019

@author: Linda

Leave/remain classifer on a hashtag_keyword basis. 
"""

import pandas as pd
import pymysql
import os
import re
from wordcloud import *
import csv

from sklearn.feature_extraction.text import TfidfVectorizer
from sklearn.cluster import KMeans
from sklearn.decomposition import PCA
from sklearn.metrics import *
import matplotlib.pyplot as plt
from nltk.tokenize import TweetTokenizer 

#Extract features using KNN, suggesting username/hashtag being excellent features
document = pd.read_excel(TWITTER_CORPUS)
train = document["full_text"]
train_new = [re.sub(r'(https?:\/\/)?([\da-z\.-]+)\.([a-z\.]{2,6})([\/\w \.-]*)*\/?$',
                    'TWITTER_LINK', t) for t in train]
    
vec = TfidfVectorizer(stop_words='english')

X = vec.fit_transform(train)
true_k = 8
model = KMeans(n_clusters=true_k, init='k-means++', max_iter=100, n_init=1)
model.fit(X)

order_centroids = model.cluster_centers_.argsort()[:, ::-1]
terms = vec.get_feature_names()
features = {}
for i in range(true_k):
    print("Cluster %d:" % i)
    feature_list = [terms[j] for j in order_centroids[i, :20]]
    features["Cluster {}".format(i)] = feature_list


#extracting username and hashtags
def pattern_finder(data, regexp):
    tag_counter = {}
    extracted_tag = []
    for i in data:
        tag = re.findall(regexp, i)
        if tag is None:
            extracted_tag.append(None)
        else:
            for t in tag:
                t = t.lower()
                if t in tag_counter:
                    tag_counter[t]  += 1
                else:
                    tag_counter[t] = 1
            extracted_tag.append(tag)
    return tag_counter,extracted_tag


tags_sensi = pattern_finder(train, r'([#][\w_-]+)')[0]
users = pattern_finder(train, r'([@][\w_-]+)')[0]

with open('tag_label.csv','w+', encoding = 'utf-8', newline='') as f:
    w = csv.writer(f)
    w.writerows(tags_sensi.items())
    w.writerows(users.items())

#Manually tagging hashtag by leave/remain and constantly referring to the traits by K-means
#Now with a list of nicely tagged username/hashtags, using a bag-of-world model to classify leave/remain/neutral
tagged = pd.read_csv("tag_label.csv", encoding = "latin-1")
sentiment_labels = {}
for i in range(len(tagged)):
    score = tagged["tendency"].iloc[i]-2
    tag = tagged["tag"].iloc[i][1:]
    sentiment_labels[tag] = score

tw = TweetTokenizer(preserve_case = True)    
                                  
def leave_remain_caculator(tweet):
    score = 0
    tokens = tw.tokenize(tweet)
    for t in tokens:
        t = t.lower()
        for label in sentiment_labels.keys():
            if t == label or t[1:] == label:
                score += sentiment_labels[label]
    return score

sentiment = [leave_remain_caculator(i) for i in train]
sentiment_rough = [1 if i>0 else (-1 if i<0 else 0) for i in sentiment]
valid_y = document["leave_tendency"][:2000]
valid_y = [int(i) if abs(i) <= 1 else 0 for i in valid_y]








