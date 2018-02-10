
import sys
import time
import json
import signal
import socket
import datetime
import threading

from mp_manager import MP_MANAGER
from tcp_wrapper import TCP_CLIENT


ip = "10.10.1.216"
port = 1234
BUFFER_SIZE = 1024

STOP_FLAG = False


def sig_handler(signum, frame):
    
    global STOP_FLAG
    
    STOP_FLAG = True
    
    
    
    
def query_aiscore(i):

    global ip, port, BUFFER_SIZE

    client = TCP_CLIENT(ip, port, timeout=10)
    return client.request(i, None)



def input_thread_func():

    global mp_manager, STOP_FLAG
    
    while not STOP_FLAG:
    
        for i in xrange(1000):
            mp_manager.put(str(i))

print query_aiscore('abcde')   
sys.exit()


# set signal handler
signal.signal(signal.SIGINT, sig_handler)
signal.signal(signal.SIGQUIT, sig_handler)
signal.signal(signal.SIGTERM, sig_handler)
signal.signal(signal.SIGALRM, sig_handler)

worker_num = 16
mp_manager = MP_MANAGER(query_aiscore, worker_num, worker_num*2, mode="PREFORK")

start_time = time.time()

input_thread = threading.Thread(target=input_thread_func)
input_thread.start()


tot_cnt = 0
while not STOP_FLAG:
    result_list = mp_manager.get_bulk(10000)
    tot_cnt += len(result_list)
    if len(result_list):
        elapsed_time = time.time() - start_time
        print "[{}] tot_cnt:{}".format(elapsed_time, tot_cnt)
        if elapsed_time >= 60:
            STOP_FLAG = True
    time.sleep(0.01)
    
print "[{}] tot_cnt:{}".format(elapsed_time, tot_cnt)


# end testing

input_thread.join()

mp_manager.stop_all_worker()
