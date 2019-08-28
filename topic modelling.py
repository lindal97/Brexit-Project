# -*- coding: utf-8 -*-
"""
Created on Mon Jun 10 18:01:58 2019

@author: Linda
"""
import pandas as pd
import json
import collections
import numpy as np
import re
import os
import ast
import statistics
import pymysql

from gensim import *
from gensim.models.coherencemodel import CoherenceModel
from sklearn.feature_extraction.text import CountVectorizer, TfidfVectorizer
from nltk.stem import WordNetLemmatizer
from nltk.stem import *


def daily_reader(path):
    news = []
    json_as_string = open(path, 'r')    
    lines = [re.sub("[\[\{\]]*","",one_object.rstrip()) for one_object in json_as_string.readlines()]
    json_as_list = "".join(lines).split('}')
    for elem in json_as_list:
         if len(elem) > 0:
             data = ast.literal_eval(json.loads(json.dumps("{" + elem[::1]+"}")))["full_text"]
             data = data.lower()
             data = re.sub(r"\\u[a-z0-9]{0,4}","",data)
             data = re.sub(r"<[^>]*>","",data)
             news.append(data.strip())
    return news

def lemmatize_stemming(text):
    stemmer = SnowballStemmer("english")
    return stemmer.stem(WordNetLemmatizer().lemmatize(text, pos='v'))

def preprocess(text,stopwords):
    result=[]
    for token in utils.simple_preprocess(text) :
        if token not in stopwords and len(token) > 3:
            result.append(lemmatize_stemming(token))
    return result

def topicmodel(document,topicnum):
    dictionary = corpora.Dictionary()
    dictionary.add_documents(document)
    corpus = [dictionary.doc2bow(doc) for doc in document]
    lda = models.LdaModel(corpus=corpus, num_topics=topicnum, id2word=dictionary)
    print(lda.show_topics(num_words = 20))
    cm = models.CoherenceModel(model=lda, corpus=corpus, dictionary=dictionary, coherence='u_mass')
    ch = cm.get_coherence()
    per=lda.log_perplexity(corpus)
    return lda, ch, per

def create_counter(document,lda):
    dictionary = corpora.Dictionary()
    dictionary.add_documents(document)
    corpus = [dictionary.doc2bow(doc) for doc in document]
    counter = []
    for doc in corpus: 
        p = lda[doc]
        p = sorted(p, key=lambda x: x[1], reverse=True)
        counter.append(p[0][0])
    return counter

#dailymail
filelist = os.listdir()
copora = []
date = []
with open("./smart-common-words.txt", 'r') as f:
    stopwords = f.read().split(",")
for i in filelist:
    try:
        news = daily_reader(i)
        copora += news
        date += [i[:-5]] * len(news)
    except:
        pass

stemmed = [preprocess(i,stopwords) for i in copora]
coherence = []
perplexity = []
for i in range(2,21):
    lda, ch, per = topicmodel(stemmed,i)
    coherence.append(ch)
    perplexity.append(per)

lda_dailymail = topicmodel(stemmed, 6)[0]
mail_counter=create_counter(stemmed,lda_dailymail)
highest_topic=[]
for lists in mail_counter:
    highest_corpus=lists[0]
    highest=highest_corpus[0]
    highest_topic.append(highest)

dailymail = pd.DataFrame()
dailymail["date"] = date
dailymail["topic"] = highest_topic

def remove_emoji(string):
    emoji_pattern = re.compile("["
                           u"\U0001F600-\U0001F64F"  # emoticons
                           u"\U0001F300-\U0001F5FF"  # symbols & pictographs
                           u"\U0001F680-\U0001F6FF"  # transport & map symbols
                           u"\U0001F1E0-\U0001F1FF"  # flags (iOS)
                           u"\U00002702-\U000027B0"
                           u"\U000024C2-\U0001F251"
                           "]+", flags=re.UNICODE)
    return emoji_pattern.sub(r'', string)

dailymail["corpus"] = [remove_emoji(i) for i in copora]

dailymail.to_csv("./dailymail.csv", encoding = 'utf-16')

#guardian
filelist2 = os.listdir()
guardian_copora = []
date_guardian = []
for i in filelist2:
    with open(i, 'r') as f:
        news = json.loads(f.read())
        for new in news:
            text = new['fields']['body']
            text = text.lower()
            text = re.sub(r"\\u[a-z0-9]{0,4}","",text)
            text = re.sub(r"<[^>]*>","",text)
            guardian_copora.append(text)
        date_guardian += [i[:-5]] * len(news)

stemmed_guardian = [preprocess(i, stopwords) for i in guardian_copora]
coherence_guardian = []
perplexity_guardian = []
for i in range(2,10):
    lda, ch, per = topicmodel(stemmed_guardian,i)
    coherence_guardian.append(ch)
    perplexity_guardian.append(per)

guardian_dailymail = topicmodel(stemmed_guardian, 5)[0]
guardian_counter=create_counter(stemmed_guardian,guardian_dailymail)
highest_topic=[]
for lists in guardian_counter:
    highest_corpus=lists[0]
    highest=highest_corpus[0]
    highest_topic.append(highest)

guardian = pd.DataFrame()
guardian["date"] = date_guardian
guardian["topic"] = highest_topic

def remove_emoji(string):
    emoji_pattern = re.compile("["
                           u"\U0001F600-\U0001F64F"  # emoticons
                           u"\U0001F300-\U0001F5FF"  # symbols & pictographs
                           u"\U0001F680-\U0001F6FF"  # transport & map symbols
                           u"\U0001F1E0-\U0001F1FF"  # flags (iOS)
                           u"\U00002702-\U000027B0"
                           u"\U000024C2-\U0001F251"
                           "]+", flags=re.UNICODE)
    return emoji_pattern.sub(r'', string)

guardian["corpus"] = [remove_emoji(i) for i in guardian_copora]

guardian.to_csv("D:/grad/MA thesis/data/topic models/guardian.csv", encoding = 'utf-16')

'''The baseline LDA-style version for twitter, to compare with the biterm model in another file'''
text = pd.read_csv('./twitter copora.csv')
text['leave_remain'] = [int(i) for i in text['leave_remain']]
leave_text = text[text["leave_remain"] > 0]["full_text"]
remain_text = text[text["leave_remain"] < 0]["full_text"]

with open('twitter-stopwords.txt', 'r') as f:
    stopword = f.read()
stopwords = stopword.split(",")

#leave
loginfo= load_token('SQL auth.txt')
conn = pymysql.connect(loginfo[0],loginfo[1],loginfo[2],loginfo[3])
c = conn.cursor()
c.execute('''SELECT full_text, created_at FROM twitter WHERE (leave_remain > 0 AND created_at NOT LIKE "2019-06%" 
                                                              AND full_text NOT LIKE "RT%" AND created_at NOT LIKE "2019-07%") ''')
leave = c.fetchall()
leave_stemmed = [preprocess(i[0],stopwords) for i in leave]
'''
coherence = []
perplexity = []
for i in range(2,21):
    lda, ch, per = topicmodel(leave_stemmed,i)
    coherence.append(ch)
    perplexity.append(per)
'''
leave_lda = topicmodel(leave_stemmed, 5)[0]
leave_counter=create_counter(leave_stemmed,leave_lda)
highest_topic_leave=[]
for lists in leave_counter:
    highest_corpus=lists[0]
    highest=highest_corpus[0]
    highest_topic_leave.append(highest)

leave = text[text['leave_remain'] > 0]
leave["topic"] = highest_topic_leave
leave.to_csv("D:/grad/MA thesis/data/topic models/leave_topicmodel_original.csv")


#remain
c = conn.cursor()
#c.execute('''SELECT full_text, created_at FROM twitter WHERE (leave_remain < 0 AND NOT created_at LIKE "2019-06%"
                                                              #AND full_text NOT LIKE "RT%") ''')
c.execute('''SELECT full_text, created_at FROM twitter WHERE (leave_remain < 0 AND NOT created_at LIKE "2019-06%"
                                                              ) ''')                                                              
remain = c.fetchall()

remain_stemmed = [preprocess(i[0],stopwords) for i in remain]


remain_lda = topicmodel(remain_stemmed, 4)[0]
remain_counter=create_counter(remain_stemmed,remain_lda)
highest_topic_remain=[]
for lists in remain_counter:
    highest_corpus=lists[0]
    highest=highest_corpus[0]
    highest_topic_remain.append(highest)

remain = text[text['leave_remain'] < 0]
remain["topic"] = highest_topic_remain
remain.to_csv("D:/grad/MA thesis/data/topic models/remain_corpora.csv")

#save dict
leave_lda.save("leave_model.model")
remain_lda.save("remain_model.model")


#topic model metric visualization
import matplotlib.pyplot as plt
metrics = pd.read_excel(r"metrics.xlsx")
metrics["topics"] = [i for i in range(2,14)]

fig, axs = plt.subplots(1, 2, figsize=(10, 4), sharey=True)
axs[0].plot(metrics['topics'], metrics['twitter-leave-umass'])
axs[0].set_xlabel('Number of topics(leave - all)', fontsize = 14)
axs[0].set_ylabel('u_mass coherence', fontsize = 14)
axs[1].plot(metrics['topics'], metrics['twitter-remain'])
axs[1].set_xlabel("Number of topics(remain - all)", fontsize = 14)
axs[0].spines["top"].set_visible(False)      
axs[1].spines["top"].set_visible(False)      
axs[0].spines["right"].set_visible(False)   
axs[1].spines["right"].set_visible(False)  

plt.savefig("twitter_metric.pdf")
plt.savefig("twitter_metric.png")






