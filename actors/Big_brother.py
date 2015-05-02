#! /usr/bin/python2
# -*- coding: utf8 -*-

import pykka
import time
from Manager import Manager

class Big_Brother(pykka.ThreadingActor):
    def __init__(self):
        super(Big_Brother, self).__init__()
        self.pool = {'managers':[]}

    def start_manager(self, token):
        self.pool['managers'].append(Manager.start(token).proxy())
        return len(self.pool['managers']) #id of the manager

    def run_manager(self, manager_id):
        self.pool['managers'][manager_id].runAll()

    def learn(self, manager_id, k):
        self.pool['managers'][manager_id].learn(k)

    def erase_manager_raw_data(self, manager_id):
        self.pool['managers'][manager_id].erase_raw_data()

    def erase_manager_parsed_data(self, manager_id):
        self.pool['managers'][manager_id].erase_parsed_data()

    def erase_manager_clusterings(self, manager_id):
        self.pool['managers'][manager_id].erase_clusterings()

    def stop_manager(self, manager_id):
        answer = self.pool['managers'][manager_id].stop_slaves()
        answer.get() # block thread
        self.pool['managers'][manager_id].stop()
