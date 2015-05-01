#! /usr/bin/python2
# -*- coding: utf8 -*-

import pykka, pymongo, tweepy, warnings
from Fetcher import Fetcher
from Parser import Tw_parser, Url_parser
from Learner import Learner
from time import sleep

class Manager(pykka.ThreadingActor):
    """ Manage one user + communicate with DB """
    def __init__(self, app_token, user_token, mongodbconnection):
        super(Manager, self).__init__()
        self.pool = {'fetcher':None, 'tw_parser':None, 'url_parsers':[], 'learner':None}
        ## Create connections
        # Twitter API
        auth = tweepy.OAuthHandler(app_token["key"], app_token["secret"])
        auth.set_access_token(user_token["key"], user_token["secret"])
        self.api = tweepy.API(auth)

        ## Mongo DB
        client = pymongo.MongoClient(mongodbconnection['host'])
        self.db = client['utopical']

        # get ego
        self.ego = self.api.me()

        # Get friends ids list
        friends_ids = []
        [friends_ids.extend(page) for page in \
             tweepy.Cursor(self.api.friends_ids, id=self.ego.id).pages()]
        self.friends_ids = set(friends_ids)

        if len(friends_ids) < self.ego.friends_count:
            warnings.Warn('Did not get all the friends ids of %s' %self.ego.screen_name)
            ## Do something to handle this case. May happen if nb_friends > 75000 (not very likely)
        self.remaining_friends = None
        self.data = []

    def fetch_data(self):
        self.remaining_friends = None
        self.pool['fetcher'] = Fetcher.start(self.api, self.ego.id).proxy()
        answer = self.pool['fetcher'].fetch_data(self.friends_ids)
        answer_data = answer.get()
        self.process_fetcher_answer(answer_data)
        while self.remaining_friends:
            ## Find a way to start tweet parsing during the waiting time
            ## In process_fetcher_answer: run parser just after the push into MongoDB
            sleep(300) # Wait 5 min
            answer = self.pool['fetcher'].fetch_data(self.remaining_friends)
            answer_data = answer.get()
            self.process_fetcher_answer(answer_data)
        out_msg = 0
        self.pool['fetcher'].stop()
        return out_msg

    def process_fetcher_answer(self, answer):
        if answer['errors']:
            # IndexErrors => no tweets : delete friend from list
            self.friends_ids = self.friends_ids.difference(set(answer['errors']))
        # update 
        self.remaining_friends = answer["unprocessed_friends"]
        # push data into DB
        if answer['data']:
            self.db['raw_data'].insert_many(answer['data'])
            self.data.extend(answer['data'])
        return None

    def parse_data(self):
        if len(self.data) == 0:
            ## Read from mongo
            self.data = list(self.db['raw_data'].find({'ego_id':self.ego.id}))

        self.pool['tw_parsers'] = Tw_parser.start().proxy()
        for friend_data in self.data:
            u_id = friend_data['u_id']
            # Tweet parsing, Non blocking:
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
            self.db['parsed_data'].insert({'ego_id':self.ego.id ,'u_id':u_id, 'u_document':" ".join(documents)})

        self.pool['tw_parsers'].stop()
        [worker.stop() for worker in self.pool['url_parsers']]
        out_msg = 0 # All went well
        return out_msg

    def learn(self, k=20):
        ## Fetch data from DB
        parsed_data = list(self.db['parsed_data'].find({'ego_id':self.ego.id}))
        self.pool['learner'] = Learner.start(k).proxy()
        clustering = self.pool['learner'].learn(parsed_data)
        clustering = clustering.get()
        ## Push into DB
        self.db['clusterings'].insert(clustering)
        self.pool['learner'].stop()
        return 0

    def stop_slaves(self):
        if self.pool['fetcher']:
            self.pool['fetcher'].stop()
        if self.pool['tw_parser']:
            self.pool['tw_parser'].stop()
        if self.pool['url_parsers']:
            [worker.stop for worker in self.pool['url_parsers']]
        if self.pool['learner']:
            self.pool['learner'].stop()
        return 0
