#! /usr/bin/python2
# -*- coding: utf8 -*-

import pykka
import time
import warnings
from Fetcher import Fetcher
from Parser import Tw_parser, Url_parser
from Learner import Learner
from time import sleep

class Manager(pykka.ThreadingActor):
    """ Manage one user + communicate with DB """
    def __init__(self, app_token, user_token):
        super(Manager, self).__init__()
        self.pool = {'fetcher':None, 'tw_parser':None, 'url_parsers':[], 'learner':None}
        ## Create connections
        # Twitter API
        auth = tweepy.OAuthHandler(app_token["key"], app_token["secret"])
        auth.set_access_token(user_token["key"], user_token["secret"])
        # self.api = tweepy.API(auth, wait_on_rate_limit=True, wait_on_rate_limit_notify=True)
        self.api = tweepy.API(auth)

        ## Mongo DB
        with open("../tokens/HoS.settings", "r") as infile:    
            settings = json.load(infile)
        client = pymongo.MongoClient(settings['mongo']['mongodbconnection']['host'])
        self.db = client['utopical']

        # get ego
        self.ego = self.api.me()

        # Get friends ids list
        friends_ids = []
        [friends_ids.extend(page) for page in \
             tweepy.Cursor(api.friends_ids(), id=self.ego.id).pages()]
        self.friends_ids = set(friends_ids)

        if len(friends_ids) < self.ego.friends_count:
            warnings.Warn('Did not get all the friends ids of %s' %self.ego.screen_name)
            ## Do something to handle this case. May happen if nb_friends > 75000
        self.remaining_friends = None
        self.data = []

    def fetch_data(self):
        self.remaining_friends = None
        self.pool['fetcher'] = Fetcher.start(api, self.ego.id).proxy()
        answer = self.pool['fetcher'].fetch_data(self.friends_ids)
        answer_data = answer.get()
        self.process_fetcher_answer(answer_data)
        while self.remaining_friends:
            sleep(300)
            answer = self.pool['fetcher'].fetch_data(self.remaining_friends)
            answer_data = answer.get()
            self.process_fetcher_answer(answer_data)
        out_msg = 0
        # if answer["status"] == 0:
        #     # push data into DB
        #     self.db['raw_data'].insert_many(answer['data'])
        #     self.data = answer['data']
        #     out_msg = 0 # All went well
        # elif answer["status"] == 0xDEADFEED: # Timeout
        #     while answer["status"] == 0xDEADFEED:
        #         # push data into DB
        #         self.db['raw_data'].insert_many(answer['data'])
        #         self.data.extend(answer['data'])
        #         # fetch remaining data
        #         self.remaining_friends = answer["unprocessed_friends"]
        #         sleep(300) # Wait 5 min an try again
        #         answer = self.pool['fetcher'].fetch_data(self.remaining_friends)
        #         answer.get()
        #     out_msg = 0 # All went well
        # else:
        #     out_msg = 0xDEADC0DE # Strange error, try to raise precise errors
        self.pool['fetcher'].stop()
        self.pool['fetcher'] = None
        return out_msg

    def process_fetcher_answer(self, answer):
        if len(answer['errors'])>0:
            # IndexErrors => no tweets : delete friend from list
            self.friends_ids = self.friends_ids.difference(set(answer['errors']))
        # update 
        self.remaining_friends = answer["unprocessed_friends"]
        # push data into DB
        self.db['raw_data'].insert_many(answer['data'])
        self.data.extend(answer['data'])
        return None

    def parse_data(self):
        if len(self.data) > 0:
            parsed_data = []
            self.pool['tw_parsers'] = Tw_parser.start()
            for friend_data in self.data:
                u_id = friend_data['u_id']
                # Tweet parsing
                # Non blocking:
                parsed_tweets = self.pool['tw_parsers'].parse_tweets(friend_data['texts'], friend_data['texts_lang'])

                # Url parsing
                urls = [url['expanded_url'] for elem in friend_data['texts_urls'] if len(elem) > 0 for url in elem ]
                pool_size = len(self.pool['url_parsers'])
                if pool_size < len(urls):
                    self.pool['url_parsers'].extend( \
                     [Url_parser.start().proxy() for _ in range(len(urls)-pool_size)] \
                     )

                # Distribute work by mapping urls to self.pool['url_parsers'] (not blocking)
                parsed_urls = [self.pool['url_parsers'][w].parse_url(url) for w,url in enumerate(urls)]

                # Gather parsed_data (blocking)
                documents = [parsed_tweets.get()]
                documents.extend([doc for doc in pykka.get_all(parsed_urls) if len(doc) > 0])
                parsed_data.append({'ego_id':self.ego_id ,'u_id':u_id, \
                                'u_document':" ".join(documents)})

            self.pool['tw_parsers'].stop()
            [worker.stop() for worker in self.pool['url_parsers']]
            # Save it to DB
            self.db['parsed_data'].insert_many(parsed_data)
            self.parsed_data = parsed_data
            out_msg = 0 # All went well
        # elif # some request to fech raw data from mongo:
        #    run same stuff ?
        # else:
        #     try:
        #         ## read data from mongoDB linked to this used
        #         # + do the same treatment
        #         # out_msg = 0
        #     except:
        #         out_msg = 0xDEADFEED ## No data
        return out_msg

    def learn(self, k=20):
        ## Usual : check if data in manager, else fetch it in MongoDB, etc.
        ## Can add some parameters to pass to the classifier ?
        self.pool['learner'] = Learner.start(k).proxy()
        clustering = self.pool['learner'].learn(self.parsed_data)
        ## Push into DB
        # from datetime import datetime
        # db['clusterings'].insert(clustering.get()) # "created_at":str(datetime.now())})
