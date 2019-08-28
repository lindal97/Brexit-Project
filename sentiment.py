# -*- coding: utf-8 -*-
"""
Created on Sun May 12 15:09:19 2019

@author: Linda
"""

import numpy as np
import pandas as pd 
import nltk
import re
import os
from nltk.tokenize import TweetTokenizer 
import string
import json
from collections import Counter
from nltk.corpus import stopwords

import pickle
from numpy import loadtxt
from keras.models import load_model
from keras.preprocessing.text import Tokenizer
from keras.preprocessing.sequence import pad_sequences


from textblob import TextBlob
from textblob.classifiers import NaiveBayesClassifier
from textblob.classifiers import DecisionTreeClassifier
from vaderSentiment.vaderSentiment import SentimentIntensityAnalyzer
import senti_classifier 
from sklearn.feature_extraction.text import CountVectorizer, TfidfVectorizer
from sklearn.ensemble import RandomForestClassifier
from sklearn.svm import SVC
from sklearn.linear_model import LogisticRegression
from sklearn.grid_search import GridSearchCV
from sklearn import *
from sklearn.externals import joblib


def data_split(corpus):
    train = pd.read_csv(corpus, encoding = 'latin-1')
    neg = train[train["senti"]==1] 
    train = train.append(neg) #somesort of oversampling here because of the imbalance of pos vs neg tweets
    train_li = [train.iloc[i][FULL_TEXT_COLUMN] for i in range(len(train))]
    train_senti = [train.iloc[i][SENTIMENT_COLUMN] for i in range(len(train))]
    xtrain,ytrain,xvalid,yvalid = model_selection.train_test_split(train_li,train_senti, train_size = 0.7)
    return xtrain, ytrain, xvalid, yvalid

xtrain, xvalid, ytrain, yvalid = data_split(TRAINING_SET_PATH)
#This is because I used 1 and 2 to manually label tweets into neg and pos. 1==pos and 0==neg in the following codes.
#yvalid = [0 if i==1 else 1 for i in yvalid]
#ytrain = [0 if i==1 else 1 for i in ytrain]


def baseline(x):
    result = []
    for i in x:
        i = TextBlob(i).sentiment.polarity
        result.append(i)
    return result

baseline_result = baseline(xvalid)
baseline_rough = [1 if i > -0.05 else 0 for i in baseline_result]
metrics.f1_score(yvalid, baseline_rough)  
'''
#Naive Bayes, textblob, omitted because of inefficiency.
def bayes_classifer(xtrain, ytrain,xtest):
    train = [(xtrain[i], ytrain[i]) for i in range(len(xtrain))]
    bayes = NaiveBayesClassifier(train)
    test = []
    maxprob = []
    for i in xtest:
        test.append(bayes.prob_classify(i).prob("pos"))
        maxprob.append(bayes.prob_classify(i).max())
    return test,maxprob

bayes_score, bayes_result = bayes_classifer(xtrain,ytrain, xvalid)
'''

#vadersentiment style
def vader_classifer(x):
    analyzer = SentimentIntensityAnalyzer()
    score = []
    for sentence in x:
        vs = analyzer.polarity_scores(sentence)
        score.append(vs["compound"])
    return score

vader_result = vader_classifer(xvalid)
vader_rough = [1 if i>-0.05 else 0 for i in vader_result]
print('Vader f1: {}'.format(metrics.f1_score(yvalid,vader_rough)))

#logit + random forest
tfv = TfidfVectorizer(analyzer='word',max_features=500, norm = 'l2')
trait = xtrain+xvalid
tfv.fit(trait)
tfv_xtrain = tfv.transform(xtrain)
tfv_xvalid = tfv.transform(xvalid)
clf_2 = LogisticRegression()
clf_2.fit(tfv_xtrain,ytrain)
logit_result = list(clf_2.predict(tfv_xvalid))
print('Test Accuracy: %.2f'% clf_2.score(tfv_xvalid, yvalid))
print("F1 score of logistic regression: {}".format(metrics.f1_score(yvalid,logit_result)))


forest = RandomForestClassifier(n_estimators = 70)
forest.fit(tfv_xtrain,ytrain)
forest_result = list(forest.predict(tfv_xvalid))
print("F1 score of random forest: {}".format(metrics.f1_score(yvalid,forest_result))

      
joblib.dump(forest, PICKLE_PATH)
joblib.dump(tfv, PICKLE_PATH)


'''
The paramater tuning part for random forest, final f1 ~0.911 (0.817 for logit, dictionary-based 0.789, vader 0.792)

#random forest
rf_test1 = {'n_estimators':[i for i in range(10,201,10)]}
rf_search1 = GridSearchCV(estimator = RandomForestClassifier(min_samples_split=100,
                                  max_depth=8,max_features='sqrt' ,random_state=10), 
                       param_grid = rf_test1, scoring='roc_auc',cv=5)
rf_search1.fit(tfv_xtrain,ytrain)
rf_search1.grid_scores_, rf_search1.best_params_, rf_search1.best_score_


rf_test2 = {'max_depth':[i for i in range(3,14,2)]}
rf_search2 = GridSearchCV(estimator = RandomForestClassifier(n_estimators= 200, 
                                  min_samples_leaf=20,max_features='sqrt' ,oob_score=True, random_state=10),
   param_grid = rf_test2, scoring='roc_auc',iid=False, cv=5)
rf_search2.fit(tfv_xtrain,ytrain)
rf_search2.grid_scores_, rf_search2.best_params_, rf_search2.best_score_

rf_test3 = {'min_samples_split':[i for i in range(80,150,20)], 'min_samples_leaf':[i for i in range(10,60,10)]}
rf_search3 = GridSearchCV(estimator = RandomForestClassifier(n_estimators= 200, max_depth=9,
                                  max_features='sqrt' ,oob_score=True, random_state=10),
   param_grid = rf_test3, scoring='roc_auc',iid=False, cv=5)
rf_search3.fit(tfv_xtrain,ytrain)
rf_search3.grid_scores_, rf_search3.best_params_, rf_search3.best_score_

rf_test4 = {'max_features':[i for i in range(3,15,2)]}
rf_search4 = GridSearchCV(estimator = RandomForestClassifier(n_estimators= 200, max_depth=9, min_samples_split=80,
                                  min_samples_leaf=10 ,oob_score=True, random_state=10),
   param_grid = rf_test4, scoring='roc_auc',iid=False, cv=5)
rf_search4.fit(tfv_xtrain,ytrain)
rf_search4.grid_scores_, rf_search4.best_params_, rf_search4.best_score_

rf2 = RandomForestClassifier(n_estimators= 200)
rf2.fit(tfv_xtrain,ytrain)
rf2_result = rf2.predict(tfv_xvalid)
print(metrics.roc_auc_score(yvalid, rf2_result))
print(metrics.f1_score(yvalid, rf2_result))

wrong_result_vader = []
for i in range(len(yvalid)):
    if vader_rough[i] != yvalid[i]:
        wrong_result_vader.append(xvalid[i])


'''
