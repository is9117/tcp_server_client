# -*- coding: utf-8 -*-

__author__  = "Isaac Park(박이삭)"
__email__ = "is9117@me.com"
__version__ = "0.2.3"

''' 
=== history ===
20171122(0.2.0): Refactoring
20171127(0.2.1): Loggings at essential steps ared added
                 In PREFORK mode, function is handled by WORKER_PROCESS class instance,
                 thereby reused some codes.
                 Worker reuse mode worker time-out added
20171128(0.2.2): time.sleep(0) after every job finished added for better multi-processing scheduling optimization.
                 More optimizations.
                 restart_worker_flag(restart_worker_semaphore) is changed from Value to Semaphore, 
                 due-to some race-condition problem.
20171201(0.2.3): Use of restart_worker_semaphore, when not in reuse_worker mode is fixed.
===============
'''

import time
import logging
import datetime
import traceback
import threading

from copy import copy
from Queue import Empty, Full
from multiprocessing import Process, Queue, Value, Semaphore


class MP_MANAGER:

    """
    Parameter descriptions:
    - target:
      Target function or WORKER_PROCESS subclass' instance of which 
        worker will be processing their jobs with.
      Any kind of function is allowed, member function, top-level function, etc. 
      WORKER_PROCESS class is provided to ensure resource reusability.
        To use WORKER_PROCESS, inherit and override worker_function member function with your code.
        Pass the instance of the class to target.
      In DEFAULT mode, only function is allowed.
      In PREFORK mode, function and WORKER_PROCESS instance is allowed.
      Target's arguments are needed to be the same as jobs queue.
      
    - worker_num:
      Worker process' max number
      For more information, visit: https://docs.python.org/2/library/multiprocessing.html#multiprocessing.Queue
      Default value is 4
      
    - input_queue_size:
      Input queue max size
      0 for infinite size
      For more information, visit: https://docs.python.org/2/library/multiprocessing.html#multiprocessing.Queue
      Default value is 8
      
    - output_queue_size:
      Output queue max size
      Output queue will be added with return value of the target function.
      Note: Disabling of this feature is to NOT TO return any value in the target function.
      0 for infinite size.
      Default value is 0
      
    - mode:
      DEFAULT: Worker will be forked when any job is enqueued.
               After finishing the job, worker will be terminated.
      PREFORK: Number of worker processes will be forked at initialization.
               Worker processes will wait till jobs are queued.
               After finishing jobs, workers will not be terminated.
      Default value is "DEFAULT"
    
    - worker_reuse_num:
      Only used in PREFORK mode.
      If worker_reuse_num is over zero, worker will count each job they processed, 
        when the count is bigger then worker_reuse_num, worker will be restarted.
      This feature is to ensure worker target's memory leak protection.
      Default value is 0
    
    - worker_timeout:
      Only used in PREFORK mode.
      Worker time-out in seconds.
      Worker will restart after worker_timeout is reached.
      If value is 0, no time-out will be set.
      This feature is to ensure worker target's memory leak protection.
      Default value is 0
      
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
      Default value is "mp_manager"
      
    """
    def __init__( self,
                  target,
                  worker_num            = 2,
                  input_queue_size      = 4,
                  output_queue_size     = 0,
                  mode                  = "DEFAULT",
                  worker_reuse_num      = 0,
                  worker_timeout        = 0,
                  enable_log            = False,
                  log_handler           = None,
                  log_level             = logging.INFO,
                  log_name              = "mp_manager"
                ):
        
        # member init
        self.target = target
        self.is_target_func = callable(target)
        self.worker_num = worker_num
        self.in_q = Queue(input_queue_size)
        self.out_q = Queue(output_queue_size)
        self.stop_flag = Value('i', 0, lock=False)
        self.mode = mode
        self.worker_reuse_num = worker_reuse_num
        self.worker_timeout = worker_timeout
        if worker_reuse_num or worker_timeout:
            self.reuse_worker = True
            self.restart_worker_semaphore = Semaphore(0)
        else:
            self.reuse_worker = False
            self.restart_worker_semaphore = None
        self.pool = []
        
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
        self.logger.info("target: {}".format(self.target))
        self.logger.info("worker number: {}".format(self.worker_num))
        self.logger.info("input queue size: {}".format(input_queue_size))
        self.logger.info("output queue size: {}".format(output_queue_size))
        self.logger.info("mode: {}".format(self.mode))
        self.logger.info("worker reuse number: {}".format(self.worker_reuse_num))
        self.logger.info("worker time-out seconds: {}".format(self.worker_timeout))
        self.logger.info("reuse workers: {}".format(self.reuse_worker))
        self.logger.info("log handler: {}".format(log_handler))
        self.logger.info("log name: {}".format(log_name))
        
        # DEFAULT mode
        if mode == "DEFAULT":
            if not self.is_target_func:
                self.logger.critical("target is not callable in DEFAULT mode")
                raise Exception("target is not callable in DEFAULT mode")
                
            self.logger.debug("starting DEFAULT mode watcher thread")
            self.watcher_thread = threading.Thread(target=self.__default_mode_worker_watcher, args=())
            self.watcher_thread.start()
        
        # PREFORK mode
        elif mode == "PREFORK":
        
            if self.is_target_func:
                # if target is function
                self.target = WORKER_PROCESS(self.enable_log, self.log_handler, self.log_level, self.log_name)
                self.target.worker_function = target
                self.logger.debug("starting PREFORK(function) mode worker processes")
            
            else:
                # if target is WORKER_PROCESS instance
                self.logger.debug("starting PREFORK(instance) mode worker processes")
                
            for i in xrange(self.worker_num):
                p = copy(self.target)
                p.idx = i
                p.in_queue = self.in_q
                p.out_queue = self.out_q
                p.stop_flag = self.stop_flag
                p.restart_worker_semaphore = self.restart_worker_semaphore
                p.worker_reuse_num = self.worker_reuse_num
                p.worker_timeout = self.worker_timeout
                p.reuse_worker = self.reuse_worker
                p.start()
                self.pool.append(p)
                
            if self.reuse_worker:
                self.logger.debug("starting PREFORK mode worker reuse watcher thread")
                self.reuse_watcher_thread = threading.Thread(target=self.__reuse_worker_watcher, args=())
                self.reuse_watcher_thread.start()
            
        # error
        else:
            self.logger.critical("mode parameter({}) is invalid".format(mode))
            raise Exception("mode parameter({}) is invalid".format(mode))
        
        
    def __del__(self):
        self.logger.debug("__del__ called")
        if self.stop_flag.value == 0:
            self.stop_all_worker()
        
        
    def __enter__(self):
        self.logger.debug("__enter__ called")
        return self
        
        
    def __exit__(self, exc_type, exc_value, traceback):
        self.logger.debug("__exit__ called")
        if self.stop_flag.value == 0:
            self.stop_all_worker()
        
        
    def is_inqueue_full(self):
        return self.in_q.full()
        
        
    def is_inqueue_empty(self):
        return self.in_q.empty()
        
        
    def is_outqueue_full(self):
        return self.out_q.full()
        
        
    def is_outqueue_empty(self):
        return self.out_q.empty()

        
    ''' 
    args format: same args as target
    Descriptions:
      Will put args to inqueue without waiting.
      If inqueue is full, Queue.Full exception will be raised
    '''
    def put_nowait(self, *args):
        self.in_q.put_nowait(args)
        
        
    '''
    Descriptions:
      Will put args to inqueue with waiting.
      If inqueue is full, will hold till inqueue has any space left.
    '''
    def put(self, *args):
        self.in_q.put(args)
        
        
    '''
    Descriptions:
      Returns one outqueue result.
    Returns:
      Output of target function or None if no result exists
    '''
    def get(self):
    
        try:
            return self.out_q.get_nowait()
        except Empty:
            return None
        
           
    '''
    Descriptions:
      Returns bulk of outqueue results.
    Parameter:
      number: maximum number of items to retreive from outqueue
    Returns:
      List of output from target function, returns [] if no results are found
    '''
    def get_bulk(self, number):
    
        result = []
        try:
            for _ in xrange(number):
                ret = self.out_q.get_nowait()
                result.append(ret)
            return result
        except Empty:
            return result
        
            
    '''
    stops all worker
    '''
    def stop_all_worker(self):
    
        self.logger.info("stopping all worker")
    
        self.stop_flag.value = 1
        
        if self.mode == "DEFAULT":
            self.watcher_thread.join()
            
        elif self.mode == "PREFORK":
            if self.reuse_worker:
                self.reuse_watcher_thread.join()
            for p in self.pool:
                p.join()
                
        self.logger.info("successfully stopped all worker")
                
                
    '''
    Internal-use thread function
    Watches reuse workers in PREFORK mode
    '''
    def __reuse_worker_watcher(self):
        
        while self.stop_flag.value is 0 or not self.in_q.empty():
            
            if not self.restart_worker_semaphore.acquire(block=False):
                time.sleep(0.1)
                continue
            
            # Below routine is ran only if restart_worker_semaphore acquire() is successful, 
            # in other words, one of worker processes is to be terminated.
            
            # Extracts dead process' index
            dead_process_idx = None
            for idx, p in enumerate(self.pool, 0):
                p.join(0.01)
                if not p.is_alive():
                    # restart only one worker for semaphore synchronization.
                    dead_process_idx = self.pool[idx].idx
                    del self.pool[idx]
                    break
                    
            # if no dead processes are found, re-releases semaphore.
            # Sometimes, above routine could be ran after worker relaeses semaphore 
            # and before process finishes termination.
            # Hence, re-releasing semaphore and continuing the loop is needed.
            if dead_process_idx is None:
                self.restart_worker_semaphore.release()
                continue
                    
            # Restarts dead process
            p = copy(self.target)
            p.idx = dead_process_idx
            p.in_queue = self.in_q
            p.out_queue = self.out_q
            p.stop_flag = self.stop_flag
            p.restart_worker_semaphore = self.restart_worker_semaphore
            p.worker_reuse_num = self.worker_reuse_num
            p.worker_timeout = self.worker_timeout
            p.reuse_worker = self.reuse_worker
            p.start()
            self.pool.append(p)
                
            time.sleep(0.01)
            
            
    '''
    Internal-use thread function
    Watches workers in DEFAULT mode.
    '''
    def __default_mode_worker_watcher(self):
    
        while self.stop_flag.value is 0 or not self.in_q.empty():
        
            for idx, p in enumerate(self.pool, 0):
                p.join(0.01)
                if not p.is_alive():
                    del self.pool[idx]
                
            while len(self.pool) < self.worker_num:
                try:
                    job = self.in_q.get_nowait()
                except Empty:
                    if self.stop_flag.value == 1:
                        break
                    time.sleep(0.01)
                    continue
                p = Process(target=self.__default_mode_worker, args=(job, self.target, self.out_q))
                p.start()
                self.pool.append(p)
                
            time.sleep(0.01)
            
        for p in self.pool:
            p.join()
        
        
    '''
    Internal-use worker function
    Processes job as worker in DEFAULT mode
    '''
    def __default_mode_worker(self, job, target_func, out_q):
        
        try:
            ret = target_func(*job)
            if ret:
                out_q.put(ret)
        except:
            pass

   
    
                
"""
Worker process class to support MP_MANAGER.
Need to override "worker_function"
ex)  def worker_function(self, sha256, log): ...
"""
class WORKER_PROCESS(Process):
    
    # internal-use members init
    idx = None
    in_queue = None
    out_queue = None
    stop_flag = None
    reuse_worker = False
    worker_timeout = None
    worker_reuse_num = None
    restart_worker_semaphore = None

    def __init__( self, 
                  enable_log            = False,
                  log_handler           = None,
                  log_level             = logging.INFO,
                  log_name              = "mp_worker" 
                ):
    
        Process.__init__(self)

        # logging init
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
        
        
    def run(self):
        
        self.logger.info("[{}] ".format(self.idx) + "starting worker process")
        
        # init variables
        reuse_cnt = 0
        restart_check_cnt = 0
        restart_worker = False
        if self.worker_timeout:
            end_date = datetime.datetime.utcnow() + datetime.timedelta(seconds=self.worker_timeout)
        
        # check if members are valid
        if self.in_queue == None:
            self.logger.critical("[{}] ".format(self.idx) + "in_queue not set")
            raise Exception("in_queue not set")
        if self.out_queue == None:
            self.logger.critical("[{}] ".format(self.idx) + "out_queue not set")
            raise Exception("out_queue not set")
        if self.stop_flag == None:
            self.logger.critical("[{}] ".format(self.idx) + "stop_flag not set")
            raise Exception("stop_flag not set")
        # reuse_worker xor restart_worker_semaphore
        if bool(self.reuse_worker) != bool(self.restart_worker_semaphore):
            self.logger.critical("[{}] ".format(self.idx) + "reuse parameters ard not valid")
            raise Exception("reuse parameters ard not valid")
        
        
        # terminate if stop flag is 1 and in queue is empty
        while self.stop_flag.value is 0 or not self.in_queue.empty():
        
            try:
                # if reuse feature is activated
                if self.reuse_worker:
                    restart_check_cnt += 1
                    
                    # check to restart worker every 10 loop
                    if restart_check_cnt >= 10:
                        restart_check_cnt = 0
                        
                        # if worker_reuse_num is used, check count is over worker_reuse_num
                        if self.worker_reuse_num:
                            if reuse_cnt >= self.worker_reuse_num:
                                reuse_cnt = 0
                                self.logger.info("[{}] ".format(self.idx) + "reuse count reached")
                                restart_worker = True
                        
                        # if worker_timeout is used, check time-out is reached
                        if self.worker_timeout:
                            now_date = datetime.datetime.utcnow()
                            if end_date <= now_date:
                                self.logger.info("[{}] ".format(self.idx) + "time-out reached")
                                restart_worker = True
                                
                        if restart_worker:
                            # Releases restart_worker_semaphore to inform __reuse_worker_watcher
                            # that one of the workers is terminated.
                            self.restart_worker_semaphore.release()
                            break
            
            
                # get job from in queue, if no job found, raises "Empty" exception
                job = self.in_queue.get_nowait()
                
                # process job with target function
                ret = self.worker_function(*job)
                
                # returns result to out queue if exists
                if ret:
                    self.out_queue.put(ret)
                
                if self.worker_reuse_num:
                    reuse_cnt += 1
                
                # sleep 0 for better multi-processing scheduling optimization
                time.sleep(0)
            
            # if in queue is empty
            except Empty:
                time.sleep(0.1)
                continue
            
            # ignore any exceptions other then "Empty"
            except:
                tb = traceback.format_exc()
                self.logger.error("[{}] ".format(self.idx) + "error occurred at worker main loop : " + tb)
        
        self.logger.info("[{}] ".format(self.idx) + "ending worker process")
        
        
        
    """
    Need to override this
    """
    def worker_function(self, *args):
        pass 
    
