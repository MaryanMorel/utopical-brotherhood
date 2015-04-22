import pykka
import time
import warnings
from Fetcher import Fetcher
from Parser import Tw_parser, Url_parser
from time import sleep

class Manager(pykka.ThreadingActor):
    """ Manage one user + communicate with DB """
    def __init__(self, app_token, user_token):
        super(Manager, self).__init__()
        self.pool = {'fetcher':None, 'tw_parser':None, 'url_parsers':None, 'learner':None}
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
             tweepy.Cursor(api.friends_ids, id=self.ego.id).pages()]
        self.friends_ids = set(friends_ids)

        if len(friends_ids) < self.ego.friends_count:
            warnings.Warn('Did not get all the friends ids of %s' %self.ego.screen_name)
            ## Do something to handle this case. May happen if nb_friends > 75000
        self.remaining_friends = None
        self.data = None

    def on_receive(self, message):
        if "fetch_data" in message:
            self.remaining_friends = None
            self.pool['fetcher'] = Fetcher.start(api, self.ego.id)
            answer = self.pool['fetcher'].ask( \
                {"fetch_friends":{'ids':self.friends_ids}}, timeout=180)
            if answer["status"] == 0:
                # push data into DB
                self.db['raw_data'].insert_many(answer['data'])
                self.data = answer['data']
                out_msg = 0 # All went well
            elif answer["status"] == 0xDEADFEED: # Timeout
                while answer["status"] == 0xDEADFEED:
                    # push data into DB
                    self.db['raw_data'].insert_many(answer['data'])
                    self.data.extend(answer['data'])
                    # fetch remaining data
                    self.remaining_friends = answer["unprocessed_friends"]
                    sleep(300) # Wait 5 min an try again
                    answer = self.pool['fetcher'].ask( \
                        {"fetch_friends":{'ids':self.remaining_friends}}, timeout=180)
                out_msg = 0 # All went well
            else:
                out_msg = 0xDEADC0DE # Strange error, try to raise precise errors
            self.pool['fetcher'].stop()
            self.pool['fetcher'] = None

        elif "parse_data" in message:
            if self.data:
                for i, friend_data in enumerate(data):
                    self.pool['tw_parsers'] = Tw_parser.start()
                    self.pool['tw_parsers'].ask(???????????????) ########################### IMPLEMENT

                    # Start workers
                    self.pool['tw_parsers'] = [Url_parser.start().proxy() for _ in range(pool_size)]

                    # Distribute work by mapping urls to self.pool['tw_parsers'] (not blocking)
                    parsed_urls = []
                    for j, url in enumerate(friend_data['texts_urls']):
                        parsed_urls.append(self.pool['tw_parsers'][j % len(self.pool['tw_parsers'])].resolve(url))

                    # Gather results (blocking)
                    ip_to_host = zip(i, pykka.get_all(parsed_urls))
                    pprint.pprint(list(ip_to_host))

                    ## Some elastic number of workers
                    # Clean up ## At the end of the loop only ? Startup is costly
                    # Just add a if to check if workers are alive
                    self.pool['tw_parsers'].stop()
                    [worker.stop() for worker in self.pool['tw_parsers']]

            # Save it to DB
            out_msg = 0 # All went well

        # Some updating of the data ?
        # just add a if at fetch step ?
        # elif "update_data" in message:
        #     # update friends + tweets + documents
        #     # save it to DB
        #     out_msg = 0 # All went well

        elif "learn" in message:
            # Read data from DB
            # run clustering then save it to DB
            out_msg = 0 # All went well

        else :
            out_msg = 0xBAADF00D

        return out_msg