import zerorpc
import pykka
import time
# from actors.Big_brother import Big_brother

class brotherhoodRPC(object):
    ## Simple server-server communication interface

    def __init__(self, big_brother):
        self.big_brother = big_brother

    # def start_fetch(self, token):
    #     answer = self.big_brother.tell({'start_fetcher':token})
    #     return answer

    def sleep_and_wake(self, sleep_time):
        time.sleep(float(sleep_time))
        return "Thanks for the nap, dude !"
        ##  method sleep user send nb sec for testing purpose

    # def stop_worker(self, tweet):
    #     answer = self.big_brother.tell({'stop_fetcher':[0]})
    #     return answer

if __name__ == '__main__':
    # big_brother = Big_brother.start()
    big_brother = "test"
    s = zerorpc.Server(brotherhoodRPC(big_brother))
    s.bind("tcp://0.0.0.0:4242")
    s.run()