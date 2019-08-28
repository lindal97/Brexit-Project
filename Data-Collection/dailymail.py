# -*- coding: utf-8 -*-
"""
Created on Thu Jun  6 18:04:36 2019

@author: Linda
"""

import argparse
from pprint import pprint

import os
from os.path import join, exists
from lxml import html,etree,cssselect
import urllib
import requests
import re

import time
import csv
from datetime import datetime
import requests
from datetime import date, timedelta
import json
from webdriver_manager.chrome import ChromeDriverManager


def generate_urls():
    main_url="http://www.dailymail.co.uk/home/sitemaparchive/"
    d1 = date(2018,11,15)  # start date
    d2 = date.today()  # end date - Today's date
    delta = d2 - d1         # timedelta
    print (str(delta) + " days of news to be collected")
    for i in range(delta.days + 1):
        day=(d1 + timedelta(i)).strftime('%Y%m%d')
        url=main_url + "day_" + day + ".html"
        r=requests.get(url,timeout=30)
        html_tree=html.fromstring(r.text)
        article_links = html_tree.xpath('//ul[@class="archive-articles debate link-box"]/*/a/@href')
        fname = join(YOUR_PATH_HERE, day + '.json')                
        with open(fname, "w+") as f:  
            for url in article_links:
                try:
                    if 'brexit' in url.lower() and 'reuters' not in url.lower() and 'pa' not in url.lower():
                        full_url = "https://www.dailymail.co.uk/" + url
                        article = article_scraper(full_url)
                        article["url"] = full_url
                        article_json = json.dumps(article, indent = 2)
                        f.write(article_json)
                except:
                    print("parsing error")
        print(str(day)+ "scraped and saved!")
        time.sleep(20)
    return

def article_scraper(url):
    article = {}
    r = requests.get(url)
    driver = html.fromstring(r.text, 'lxml')
    full_text = driver.xpath("//*[@class='alpha']")[0].text_content().split('\n')
    body = full_text[2] + full_text[8]
    body = re.sub("RELATED ARTICLES.*","", body)
    article["full_text"] = body
    return article

generate_urls()









