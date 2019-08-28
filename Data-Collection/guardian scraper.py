# -*- coding: utf-8 -*-
"""
Created on Wed May 15 13:22:00 2019

A guardian crawler mainly used to scrape all Brexit related article since Nov 2018. Largely based on Dan Nguyen's 
excellent tutorial, modified a little for particular topic:  
https://gist.github.com/dannguyen/c9cb220093ee4c12b840
"""

import json
import requests
import os
from os import makedirs
from os.path import join, exists
from datetime import date, timedelta
import time

API_ENDPOINT = 'http://content.guardianapis.com/search'
my_params = {
    'from-date': "",
    'to-date': "",
    'order-by': "newest",
    "page-size":50,
    'show-fields': 'body',
    "tag": "eu/referendum",
    'api-key': YOUR_API_HERE
}


#get basic info of articles
start_date = date(2018, 11, 21)
end_date = date(2019, 6, 5)
dayrange = range((end_date - start_date).days + 1)
for daycount in dayrange:
    try:
        dt = start_date + timedelta(days=daycount)
        datestr = dt.strftime('%Y-%m-%d')
        fname = join(YOUR_FILE_NAME, datestr + '.json')
        if not exists(fname):
            # then let's download it
            print("Downloading", datestr)
            all_results = []
            my_params['from-date'] = datestr
            my_params['to-date'] = datestr
            current_page = 1
            total_pages = 1
            while current_page <= total_pages:
                print("...page", current_page)
                my_params['page'] = current_page
                resp = requests.get(API_ENDPOINT, my_params)
                data = resp.json()
                all_results.extend(data['response']['results'])
                # if there is more than one page
                current_page += 1
                total_pages = data['response']['pages']
            with open(fname, 'w+') as f:
                print("Writing to", fname)
                # re-serialize it for pretty indentation
                f.write(json.dumps(all_results, indent=2))
        time.sleep(20)
    except:
        pass





