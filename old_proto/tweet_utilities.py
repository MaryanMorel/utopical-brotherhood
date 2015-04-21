#! /usr/bin/python3.4
# -*- coding: utf8 -*-
import tweepy
import pymongo
import simplejson as json
from pprint import pprint
from warnings import warn
from tweepy.models import ModelFactory

def connexion_handler(settings):
    auth = tweepy.OAuthHandler(settings["consumer_key"], settings["consumer_secret"])
    auth.set_access_token(settings["access_token_key"], settings["access_token_secret"])
    #api = tweepy.API(auth, parser=ModelParser(), wait_on_rate_limit=True, wait_on_rate_limit_notify=True)
    api = tweepy.API(auth, wait_on_rate_limit=True, wait_on_rate_limit_notify=True)
    return(api)

# class ModelParser(tweepy.parsers.ModelParser):

#     def parse(self, method, payload):
#         result = super(ModelParser, self).parse(method, payload)
#         try:
#             iter(result)
#         except TypeError:
#             result._payload = json.loads(payload)
#         else:
#             for item in result:
#                 try:
#                     item._payload = json.loads(payload)
#                 except AttributeError:
#                     warn("Cannot add payload to " + str(type(item)))
#         return result

"""Just use the regular parser and ._json on the object (or on each element of the object if
    iterable )
"""