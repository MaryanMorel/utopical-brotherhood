import zerorpc
import pykka
import time
from Big_brother import Big_brother

class brotherhoodRPC(object):
    ## Simple server-server communication interface

    def __init__(self, big_brother):
        self.big_brother = big_brother

    def start_fetch(self, token):
        answer = self.big_brother.tell({'start_fetcher':token})
        return answer

    def stop_worker(self, tweet):
        answer = self.big_brother.tell({'stop_fetcher':[0]})
        return answer

if __name__ == '__main__':
    big_brother = Big_brother.start()
    s = zerorpc.Server(brotherhoodRPC(big_brother))
    s.bind("tcp://0.0.0.0:4242")
    s.run()