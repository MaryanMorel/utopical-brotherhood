#! /usr/bin/python3.4
# -*- coding: utf8 -*-

import tweepy
import pymongo
import simplejson as json
from pprint import pprint
from tweet_utilities import connexion_handler

screen_name = "maryanmorel"

with open("../tokens/HoS.settings", "r") as infile:    
    settings = json.load(infile)

# Connexion to DB
client = pymongo.MongoClient(settings['mongo']['mongodbconnection']['host'])
db = client['utopical']
users_collection = db['users']
tweets_collection = db['tweets']

# Connexion to twitter API
api = connexion_handler(settings['twitter'])

# Get the main User object
ego = api.get_user(screen_name)
users_collection.insert(ego._json)

# Get ego's alters
i = 0
scrnames = [ego]
# Get pages of friends, 20 friends per page
for page in tweepy.Cursor(api.friends, screen_name=screen_name).pages():
    # Process the friend here    
    [users_collection.insert(friend._json) for friend in page]
    scrnames.append(friend.screen_name)
    # Put users ids in some list
    i += 20
    if(i % 100 == 0):
        pprint(i)
    # push the user list into the db
    # Hack the payload in order to set screen_name as key in mongoDB

for name in scrnames:
    res = api.user_timeline(screen_name=screen_name)
    for tweet in res:
        tweet = tweet._json
        tweet['friend'] = screen_name # ego screen name
        tweets_collection.insert(tweet)

"""
TODO:
Loop over retrieved users to get tweet samples for each of them

Do some machine learning

Think of a good DB organization (keep info that is useful to the model,
	i.e. the ego-alters map, + tweet collection for each user. 
	Think about hyperloglog for deduplication, plus update policy)

Use akka actors / streams to do the same thing in a more powerful way
"""