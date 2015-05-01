#! /usr/bin/python2
# -*- coding: utf8 -*-

from __future__ import division
from math import ceil
import pykka, tweepy, time
from warnings import warn

class Fetcher(pykka.ThreadingActor):
    """Fetch friends and tweets for a given ego
    On actor per user must be created """

    def __init__(self, api, ego_id):
        super(Fetcher, self).__init__()
        self.api = api
        self.ego_id = ego_id

    def fetch_data(self, ids):
        """ Get timelines with GET statuses/user_timeline
        " up to 3,200 of a user’s most recent Tweets, 180 requests / 15 min
        " We hydrate users info with 'user' field in this API's answer
        " No need of GET users/lookup API
        """
        data = []
        processed = []
        errors = []
        try :
            for fid in ids:
                ## Add exception to handle user with no tweets
                try:
                    bulk = self.api.user_timeline(id=fid, count=100)
                    data.append(self.data_filter(bulk))
                    processed.append(fid)
                except IndexError:
                    # drop friend id without tweet
                    # except more general + stock error in errors list {fid, exception} ?
                    errors.append(fid)
            out_msg = {'status':0, 'data':data, \
                       'unprocessed_friends':None, 'errors':errors}
        except tweepy.TweepError as exc:
            warn('0xDEADFEED')
            unprocessed_friends = set(ids).difference(set(processed))
            out_msg = {'status':0xDEADFEED, 'data':data, \
                       'unprocessed_friends':unprocessed_friends, 'errors':errors}
        return out_msg

    def data_filter(self, bulk):
        ## Use closure + list comprehension for faster loop
        texts = []
        texts_lang = []
        texts_urls = []
        def insert(tw):
            texts.append(tw.text)
            texts_lang.append(tw.lang)
            texts_urls.append(tw.entities['urls'])
        [insert(tw) for tw in bulk]
        # Add user description data (not a tweet, but contains similar info)
        user = bulk[0].user
        texts.append(user.description)
        texts_lang.append(user.lang)
        texts_urls.append(user.entities['description']['urls'])
        return {'ego_id':self.ego_id ,'u_id':user.id, \
                'u_screen_name':user.screen_name, 'texts':texts, \
                'texts_lang':texts_lang, 'texts_urls':texts_urls }

