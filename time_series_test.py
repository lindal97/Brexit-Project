# -*- coding: utf-8 -*-
"""
Created on Tue Jun 18 15:56:39 2019

@author: Linda
"""


import pandas as pd
import os
from datetime import *
from statsmodels.tsa.stattools import adfuller,kpss,grangercausalitytests


#filling in missing data
twitter = pd.read_csv("leave_sentiment_count.csv")
newspaper = pd.read_csv("dailymail_senti.csv")
influential = pd.read_excel("influential.xlsx")
twitter["formatted"] = [datetime.strptime(i, "%Y/%m/%d") for i in twitter["date"]]
newspaper["formatted"] = [datetime.strptime(i, "%Y/%m/%d") for i in newspaper["date"]]

twitter = twitter.interpolate()
newspaper = newspaper.interpolate()
influential = influential.interpolate()
twitter.to_csv("interploated_twitter.csv")

twitter = twitter.drop(index = [i for i in range(208,213)])

#test for stationary
def stationary_test(data, fname):
    with open(fname + str('_stationary.txt'), 'w+') as f:
        for c in data.columns.values:
            if c != "formatted" and c != "date":
                f.write('kpss for ' + str(c) + str(kpss(data[c], regression = 'ct')) + '\n')
                f.write(str(adfuller(data[c], regression = 'ct')) + '\n\n')
    return

stationary_test(newspaper,'newspaper2')
stationary_test(twitter,'twitter')


      
#granger casuality
def casuality_test(col1, col2, start = 1, end = 208):
    test_data = [[col1[i],col2[i]] for i in range(start, end)]
    test = grangercausalitytests(test_data, maxlag = 10)
    return test

#remain
with open('remain_causality_not_full.txt', 'w+') as f:
    for c in twitter.columns.values:
        if "remain" in c:
            test = casuality_test(newspaper["guard_senti"], twitter[c],15,206)
            test_reverse = casuality_test(twitter[c], newspaper["guard_senti"],15,206)
            f.write('\n\n' + str(c) + ' relations: \n')
            for i in test.values():
                f.write(str(i[0]))
                f.write('\n')
            f.write('\n\n' + str(c) + ' reserve relations: \n')
            for i in test_reverse.values():
                f.write(str(i[0]))
                f.write('\n')
    f.close()

with open('leave_causality.txt', 'w+') as f:
    for c in twitter.columns.values:
        if "leave" in c and 'avg' not in c:
            test = casuality_test(newspaper["mail_senti"], twitter[c],15,206)
            test_reverse = casuality_test(twitter[c], newspaper["mail_senti"],15,206)
            f.write('\n\n' + str(c) + ' relations: \n')
            for i in test.values():
                f.write(str(i[0]))
                f.write('\n')
            f.write('\n\n' + str(c) + ' reserve relations: \n')
            for i in test_reverse.values():
                f.write(str(i[0]))
                f.write('\n')
    f.close()

with open('tendency.txt', 'w+') as f:
    for c in newspaper.columns.values:
        if "date" not in c and 'formatted' not in c:
            test = casuality_test(newspaper[c], twitter['avg_leave_score'])
            test_reverse = casuality_test(twitter['avg_leave_score'], newspaper[c])
            f.write('\n\n' + str(c) + ' relations: \n')
            for i in test.values():
                f.write(str(i[0]))
                f.write('\n')
            f.write('\n\n' + str(c) + ' reserve relations: \n')
            for i in test_reverse.values():
                f.write(str(i[0]))
                f.write('\n')
    f.close()


grangercausalitytests([[influential["leave_influential"][i],
                        influential["leave_senti"][i]] for i in range(141)], 10)
grangercausalitytests([[influential["leave_senti"][i],
                        influential["leave_influential"][i]] for i in range(141)], 10)    









