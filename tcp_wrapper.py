# -*- coding: utf-8 -*- 

__author__  = "Isaac Park(박이삭)"
__email__ = "is9117@me.com"
__version__ = "0.2.0"

''' 
=== history ===
20171212(0.1.0): First draft
20171228(0.2.0): Result processed through callback, added
20180404(0.2.1): Bug fix at process_job function
20180915(0.3.0): Python 3 support added
===============
'''


import time
import json
import socket
import signal
import random
import string
import logging
import threading
import traceback

from twisted.internet import protocol, reactor, endpoints
from twisted.internet import defer, threads



RESULT_CODE_MSG_MAP = {
    -500 : 'Internal error',
    -408 : "Request time-out",
    -404 : "Connection refused",
    -400 : "Invalid request",
    -1 : "Unknown error",
    1 : "Success"
}




#################################################
############ twisted factory classes ############
#################################################

class PROTOCOL(protocol.Protocol):

    def __init__(self, job_q, protocol_dict, logger):
    
        global RESULT_CODE_MSG_MAP
        
        # print "[PROTOCOL::__init__] called"
        self.job_q = job_q
        self.protocol_dict = protocol_dict
        self.logger = logger
        self.result_dict = {}
        # self.result = {"result_code": -1, "result_msg": RESULT_CODE_MSG_MAP[-1], "result_data": None, "id": None}
    
    def dataReceived(self, data):
        # print "[PROTOCOL::dataReceived] called", data
        
        threads.deferToThread(self.input_data, data)
        
        
            
    def input_data(self, json_str):
        
        global RESULT_CODE_MSG_MAP

        # print "[process_job] ", str(self), json_str

        # result_data = {}
        # check input valid
        try:
            json_data = json.loads(json_str.decode('utf-8'))
            id = json_data['id']
            self.result_dict['id'] = id
            request_data = json_data['data']
        except:
            self.result_dict['result_code'] = -400
            self.result_dict['result_msg'] = RESULT_CODE_MSG_MAP[-400]
            self.logger.error("[{}] input invalid. tb:{}".format(json_str, traceback.format_exc()))
            self.send_result(json.dumps(self.result_dict).encode('utf-8'))
            return

        try:
            # generate key
            while True:
                random_str = ''.join(random.choice(string.ascii_uppercase + string.digits) for _ in range(5))
                key = "{}_{}_{}".format(id, str(time.time()), random_str)
                if key not in self.protocol_dict:
                    break
            
            # insert self to protocol dict
            self.protocol_dict[key] = self
            
            # put job
            self.job_q.put(key, request_data)
            
        except:
            tb = traceback.format_exc()
            self.result_dict = {}
            self.result_dict['id'] = id
            self.result_dict['result_code'] = -500
            self.result_dict['result_msg'] = RESULT_CODE_MSG_MAP[-500]
            self.logger.error("[{}] internal error. tb:{}".format(id, tb))
            self.send_result(json.dumps(self.result_dict).encode('utf-8'))


    def send_result(self, result_str):

        # print "[send_result] ", str(self), result_str

        self.transport.write(result_str)
        self.transport.loseConnection()
        
        self.logger.debug("{} sent success".format(result_str))

        
    def process_job(self, result):

        global RESULT_CODE_MSG_MAP

        try:
            self.result_dict['result_code'] = 1
            self.result_dict['result_msg'] = RESULT_CODE_MSG_MAP[1]
            self.result_dict['result_data'] = result
            
        except:
            tb = traceback.format_exc()
            self.result_dict['result_code'] = -500
            self.result_dict['result_msg'] = RESULT_CODE_MSG_MAP[-500]
            self.logger.error("[{}] internal error. tb:{}".format(self.result_dict.get('id', 'None'), tb))
            
        self.send_result(json.dumps(self.result_dict).encode('utf-8'))




class FACTORY(protocol.Factory):

    def __init__(self, job_q, protocol_dict, logger):
        # print "[FACTORY::__init__] called"
        self.job_q = job_q
        self.protocol_dict = protocol_dict
        self.logger = logger

    def buildProtocol(self, addr):
        # print "[FACTORY::buildProtocol] called", str(PROTOCOL)
        return PROTOCOL(self.job_q, self.protocol_dict, self.logger)

        
    

#################################################
############### TCP Wrapper class ###############
#################################################

class TCP_SERVER:

    STOP_MODE = False

    """
    Parameter descriptions:
    - port: 
      Port number to listen to
      
    - job_manager:
      MP_MANAGER instance with following conditions..
      1. Job will be passed as follow: key, request_data
      2. Always return dict with the format as such: {"key":<parameter 'key'>, "ret_val": <any ret value>}
      
    - enable_log:
      A Flag of which whether to enables logging feature.
      Default value is False
    
    - log_handler:
      Log handler to add to logger object
      If no handler is passed, StreamHandler with 
        format of "<%(levelname)s::%(name)s> [%(asctime)s] %(message)s"
        will be added as default handler.
      For more information, visit: https://docs.python.org/2/library/logging.handlers.html
      Default value is None
      
    - log_level:
      Log level to be set to logger object
      For more information, visit: https://docs.python.org/2/library/logging.html
      Default value is logging.INFO
      
    - log_name:
      Log name to be set to logger object
      For more information, visit: https://docs.python.org/2/library/logging.html
      Default value is "tcp_server"
      
    """
    def __init__( self, 
                  port, 
                  job_manager,
                  enable_log            = False,
                  log_handler           = None,
                  log_level             = logging.INFO,
                  log_name              = "tcp_server" 
                ):

        self.port = port
        self.job_q = job_manager

         # logging init
        self.enable_log = enable_log
        self.log_handler = log_handler
        self.log_level = log_level
        self.log_name = log_name
        
        self.logger = logging.getLogger(log_name)
        self.logger.setLevel(log_level)
        
        # if enable_log is False disable logging feature
        if not enable_log:
            self.logger.disabled = True
        
        if len(self.logger.handlers) == 0:
            # add log_handler to local logger
            if log_handler:
                self.logger.addHandler(log_handler)
            # if no log_handler is added, default log handler(StreamHandler) will be added
            else:
                ch = logging.StreamHandler()
                ch.setLevel(log_level)
                formatter = logging.Formatter('<%(levelname)s::%(name)s> [%(asctime)s] %(message)s')
                ch.setFormatter(formatter)
                self.logger.addHandler(ch)
        
        # log settings
        self.logger.info("port: {}".format(self.port))
        self.logger.info("log handler: {}".format(log_handler))
        self.logger.info("log name: {}".format(log_name))
        
        
    def start(self):
    
        self.protocol_dict = {} # key: input key, value: PROTOCOL obj

        self.logger.debug("strting server")
        endpoints.serverFromString(reactor, "tcp:port={}".format(self.port)).listen(FACTORY(self.job_q, self.protocol_dict, self.logger))

        self.logger.debug("strting run thread")
        self.run_t = threading.Thread(target=reactor.run, args=(False,))
        self.run_t.start()

        self.logger.debug("strting job thread")
        self.job_t = threading.Thread(target=self.job_dist_func)
        self.job_t.start()



    def stop(self):

        self.logger.debug("stopping server")
        self.STOP_MODE = True

        self.logger.debug("stopping reactor")
        reactor.callFromThread(reactor.stop)

        self.job_t.join()
        self.run_t.join()
        self.logger.debug("threads ended")


        
    def job_dist_func(self):

        self.logger.debug("starting job_func")
        
        while not self.STOP_MODE:

            try:
                result_list = self.job_q.get_bulk(10)
                for result in result_list:
                    proto_obj = self.protocol_dict[result['key']]
                    reactor.callFromThread(proto_obj.process_job, result['ret_val'])
                    del self.protocol_dict[result['key']]
                if not result_list:
                    time.sleep(0.001)
            except:
                tb = traceback.format_exc()
                self.logger.error("job_dist_func loop has error: {}".format(tb))
            
        self.logger.debug("ending job_func")



class TCP_CLIENT:

    def __init__(self, ip, port, buf_size=4096, timeout=120):
    
        self.ip = ip
        self.port = port

        self.buf_size = buf_size
        self.timeout = timeout
        # socket.settimeout(timeout)


    """
    Parameter descriptions:
    id:
      Unique value for identifing requests.
    request_data:
      For requesting, will be encoded to json string
      
    Returns:
      dict with following format:
      {"result_code": <result_code>, "result_msg":<result_msg>, "result_data":<data returned by the server>}
      For more information about result_code and result_msg, see RESULT_CODE_MSG_MAP
    """
    def request(self, id, request_data):
    
        global RESULT_CODE_MSG_MAP
    
        try:
            json_data = {"id": id, "data":request_data}
            json_str = json.dumps(json_data).encode('utf-8')
        
            # print "start socket"
            s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            s.settimeout(self.timeout)
            # print "start connect"
            s.connect((self.ip, self.port))
            # print "start send"
            s.send(json_str)
            # print "start recv"
            result = s.recv(self.buf_size)
            # print "got result", result
            return json.loads(result.decode('utf-8'))
            
        except socket.timeout:
            # socket time-out
            return {"result_code": -408, "result_msg": RESULT_CODE_MSG_MAP[-408], "result_data": None}
            
        except Exception as e:
            traceback.print_exc()
            # print(dir(e))
            
            errno = 0
            result_code = -400
            result_msg = RESULT_CODE_MSG_MAP[-400]
            if hasattr(e, 'errno'):
                errno = e.errno
            
            # connection refused
            if errno == 111:
                result_code = -404
                result_msg = RESULT_CODE_MSG_MAP[-404]
            
            return {"result_code": result_code, "result_msg": result_msg, "result_data": None}
        finally:
            s.close()









