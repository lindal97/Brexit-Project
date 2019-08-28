# -*- coding: utf-8 -*-
"""
Created on Thu May 30 14:15:33 2019

@author: Linda

For geocoding of twitter location, especially those in UK.
"""



import googlemaps
import postcodes_io_api
import csv
import pymysql
import os
import re


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



def preprocessing(location):
    location = location.strip().lower()
    location = remove_emoji(location)
    if re.search(postcode_full, location) is not None:
        to_code = (re.search(postcode_full,location).group(), "postcode")
    elif re.search(postcode_part, location) is not None:
        to_code = (re.search(postcode_part,location).group(), "postcode")
    elif in_uk_locations(location) is not None:
        if in_uk_locations(location) not in non_specific:
            if in_uk_locations(location) == "ldn":
                to_code = ("london, UK", "location")
            else:
                to_code = (in_uk_locations(location) + str(",UK"), "location")
        else:
            to_code = (in_uk_locations(location), "broad")
            if to_code[0] == "uk" or to_code[0] == "britian" or to_code[0] == "british" or to_code[0] == "gb":
                to_code = ("united kingdom", "broad")
    else:
        to_code = None
    return to_code
    
def in_uk_locations(location):
    uk_place = None
    for i in uk_locations:
        if i in location:
            uk_place = i
            return uk_place
    return

def geocoder(location_type):  
    lat_long = None
    if location_type is None:
        pass
    elif location_type[1] == "postcode":
        geo_info = postcode.get_postcode(postcode = location_type[0])
        if geo_info["status"] == 200:
            lat_long = (geo_info["result"]["latitude"], geo_info["result"]["longitude"])
        elif geo_info["status"] == 404:
            geo_to_code = postcode.get_autocomplete_postcode(postcode = location_type[0])["result"][0]
            geo_info = postcode.get_postcode(geo_to_code)
            lat_long = (geo_info["result"]["latitude"], geo_info["result"]["longitude"])
        else:
            pass
    else:
        geo_info = gmaps.geocode(location_type[0])
        print(geo_info)
        lat_long = (geo_info[0]["geometry"]["location"]["lat"], geo_info[0]["geometry"]["location"]["lng"])
        if location_type[1] == "broad":
            type_ = ("non_specific",)
            lat_long = lat_long + type_
    return lat_long

with open('uk_towns_counties.csv', newline = '') as csvfile:
    location_names = csv.reader(csvfile)
    uk_locations = [i[0].lower().strip() for i in location_names]
postcode_full, postcode_part, lat_long_reg = load_token("postcode_latlong.txt")
non_specific = load_token("broad_locations.txt")
    
'''
#test
place_list = pd.read_csv("geo_sample.csv")["user_location"]
codes = []
for i in place_list:
    to_code = preprocessing(i)
    codes.append(to_code)
print(codes)
'''

#c.execute('''UPDATE users ADD COLUMN geocode VARCHAR(255)''')

loginfo= load_token('SQL auth.txt')
USER_KEY = load_token('google auth.txt')[0]
conn = pymysql.connect(loginfo[0],loginfo[1],loginfo[2],loginfo[3])
gmaps = googlemaps.Client(key = USER_KEY)
postcode = postcodes_io_api.Api() 
cursor = conn.cursor()
#c.execute('SET GLOBAL max_allowed_packet=500*1024*1024')
geodata = []
coded = {}
cursor.execute('''SELECT hash_id FROM users''')
id_ = cursor.fetchall()

counter = 0
for i in id_:
    cursor.execute('''SELECT user_location from users WHERE hash_id =''' + "'" + str(i[0].strip()) + "'")
    location = cursor.fetchone()[0]
    if location is not None:
        geo_code = preprocessing(location)
        try:
            if geo_code is not None:
                if geo_code[0] in coded.keys():
                    lat_long = coded[geo_code[0]]
                else:
                    lat_long = geocoder(geo_code)
                    coded[geo_code[0]] = lat_long
                cursor.execute('''UPDATE users SET geocode = " '''+ str(lat_long) + 
                          ''' " WHERE hash_id = ''' + '"' + str(i[0].strip())+'"')
                conn.commit()
            else:
                pass
        except:
            pass
    counter += 1
    if counter % 1000 == 0:
        print(str(i) + "geoinfo coded!")



conn.close()








