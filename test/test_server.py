# -*- coding: utf-8 -*- 
import os
import time
import signal
import random
import logging
import threading
import traceback

from mp_manager import MP_MANAGER
from tcp_wrapper import TCP_SERVER


STOP_FLAG = False
def sig_handler(signum, frame):
    
    global STOP_FLAG
    STOP_FLAG = True
    


def job_func(key, request_data):
    # print key
    # time.sleep(1.5)
    # chance = int(random.random() * 100)
    # if chance <= 1:
        # print "[worker] {} 1 in 100 chance reached".format(key)
        # time.sleep(10)
    # else:
        # print "[worker] {} 98 in 100 chance reached".format(key)
        # pass
    return_dict = {"key":key, "ret_val": int(random.random() * 100) + 1}
    return return_dict
    

# set signal handler
signal.signal(signal.SIGQUIT, sig_handler)
signal.signal(signal.SIGTERM, sig_handler)
signal.signal(signal.SIGALRM, sig_handler)

worker_num = 2
worker_manager = MP_MANAGER(job_func, worker_num, worker_num*2, mode="PREFORK", enable_log=True, log_level=logging.DEBUG)
# worker_manager = None

tcp_server = TCP_SERVER(1234, worker_manager, enable_log=True, log_level=logging.DEBUG)
tcp_server.start()

print "start sleep"
while not STOP_FLAG:
    time.sleep(1)

tcp_server.stop()

worker_manager.stop_all_worker()



