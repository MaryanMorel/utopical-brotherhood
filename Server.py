#! /usr/bin/python2
# -*- coding: utf8 -*-

import zerorpc
from actors import Manager
import simplejson as json

class Master(object):
    ## Simple server-server communication interface

    def __init__(self, app_token, mongodbconnection):
        self.app_token = app_token
        self.mongodbconnection = mongodbconnection
        self.pool = {'managers':[]}

    def start_manager(self, user_token):
        DUMP THE JSON (TOKENS)
        self.pool['managers'].append(Manager.start(self.app_token, user_token, self.mongodbconnection).proxy())
        ## Now update strategy for now, erase old data (blocking):
        ansr = self.pool['managers'][manager_id].erase_raw_data()
        ansr.get()
        ansr = self.pool['managers'][manager_id].erase_parsed_data()
        ansr.get
        ansr = self.pool['managers'][manager_id].erase_clusterings()
        ansr.get
        ## Run data extraction, parsing, and learning (non blocking)
        self.pool['managers'][manager_id].runAll()
        return len(self.pool['managers']) #id of the manager

    ## Methods for fine control :
    def learn(self, manager_id, nb_clusters):
        self.pool['managers'][manager_id].learn(nb_clusters)

    def erase_manager_raw_data(self, manager_id):
        self.pool['managers'][manager_id].erase_raw_data()

    def erase_manager_parsed_data(self, manager_id):
        self.pool['managers'][manager_id].erase_parsed_data()

    def erase_manager_clusterings(self, manager_id):
        self.pool['managers'][manager_id].erase_clusterings()

    ## Cleaning :
    def stop_manager(self, manager_id):
        answer = self.pool['managers'][manager_id].stop_slaves()
        answer.get() # block thread
        self.pool['managers'][manager_id].stop()

    def sleep_and_wake(self, sleep_time):
        ##  method sleep user send nb sec for testing purpose
        return "Thanks for the nap, dude !"

if __name__ == '__main__':
    ## Read token
    with open("./dummy_user.settings", "r") as infile:    
        settings = json.load(infile)
    app_token = {'key':settings['twitter']["consumer_key"], 'secret':settings['twitter']["consumer_secret"]}
    mongodbconnection = {'host':settings['mongo']['mongodbconnection']['host']}
    ## Configure server
    server = zerorpc.Server(Master(app_token, mongodbconnection)) ## Expose manager interface
    server.bind("tcp://0.0.0.0:4242") # Listen all ips on port 4242 for tcp connections
    ## Run server
    server.run()