#!/usr/bin/env python

"""Multi-threaded jobs
"""

# This file is part of PASTA and is forked from SATe

# PASTA like SATe is free software: you can redistribute it and/or modify
# it under the terms of the GNU General Public License as published by
# the Free Software Foundation, either version 3 of the License, or
# (at your option) any later version.
#
# This program is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU General Public License for more details.
#
# You should have received a copy of the GNU General Public License
# along with this program.  If not, see <http://www.gnu.org/licenses/>.

# Jiaye Yu and Mark Holder, University of Kansas

import errno
import os
import traceback
import stat
import time
import sys
from Queue import Queue
from cStringIO import StringIO
from contextlib import contextmanager
from multiprocessing import Process, Manager, Value
from random import random
from shutil import rmtree
from subprocess import Popen, PIPE
from tempfile import mkdtemp
from threading import Thread, Event, Lock

from pasta import get_logger, TIMING_LOG, MESSENGER, configure_spark
from pasta.filemgr import open_with_intermediates

_LOG = get_logger(__name__)

class LoggingQueue(Queue):
    def put(self, job):
        TIMING_LOG.info("%s queued" % str(job.context_str))
        Queue.put(self, job)

jobq = LoggingQueue()

_all_dispatchable_jobs = []

merged_queue_events = []

def new_merge_event():
    global merged_queue_events
    e = Event()
    merged_queue_events.append(e)
    return e

def set_all_events():
    global merged_queue_events
    for e in merged_queue_events:
        e.set()
        
def kill_all_jobs():
    global _all_dispatchable_jobs
    for job in _all_dispatchable_jobs:
        job.kill()
    set_all_events()


@contextmanager
def TemporaryDirectory():
    name = mkdtemp()
    try:
        yield name
    finally:
        try:
            rmtree(name)
        except OSError as e:
            # Reraise unless ENOENT: No such file or directory
            # (ok if directory has already been deleted)
            if e.errno != errno.ENOENT:
                raise

class LightJobForProcess():

    def __init__(self, invocation, k, environ):
        self._invocation = invocation
        self._k = k
        self.error = None # NOTE: This is NOT sent back to the main process
        self.return_code = None # NOTE: This is sent back to the main process
        self.environ = environ

    def read_stderr(self, _stderr_fo):
        if os.path.exists(_stderr_fo.name):
            errFile = open(_stderr_fo.name, 'r')
            errorFromFile = errFile.read(-1)
            errFile.close()
            return errorFromFile
        else:
            return None

    def findFile(self, path, fileName):
        for root, dirs, files in os.walk(path):
            if fileName in files:
                return os.path.join(root, fileName)

        return ""

    def changePermissions(self, path, itemNumber):
        # Example: tempAHc28U/step0/centroid/pw/r5d2_r5d1/tempopal_waXAP/out.fasta
        # First file:
        finalItemNumber = len(path.split("/"))
        startItemNumber = finalItemNumber - itemNumber
        currentItem = 0

        # MESSENGER.send_info("[JMAbuin] The start item is " + str(startItemNumber))
        # MESSENGER.send_info("[JMAbuin] The final item is " + str(finalItemNumber))

        basePath = ""

        for itemPath in path.split("/"):

            if (itemPath):

                basePath = basePath + "/" + itemPath

                if ((currentItem < finalItemNumber) and (currentItem >= startItemNumber) and (os.path.isfile(basePath) or os.path.isdir(basePath))):
                    # MESSENGER.send_info("[JMAbuin] Changing permissions of " + basePath)
                    try:
                        os.chmod(basePath, stat.S_IRWXO | stat.S_IRWXG | stat.S_IRWXU)
                    except Exception as e:
                        MESSENGER.send_error("[JMAbuin] ERROR Changing permissions: "+e.message)

            currentItem += 1

        # MESSENGER.send_info("[JMAbuin] End of changing permissions for "+path)

    def run(self):
        _LOG.debug('launching %s.' % " ".join(self._invocation))
        k = self._k
        proc_cwd = k.get('cwd', os.curdir)
        stdout_file_path = k.get('stdout', None)
        stderr_file_path = k.get('stderr', None)
        if stdout_file_path:
            _stdout_fo = open_with_intermediates(stdout_file_path, 'w')
        else:
            _stdout_fo = open_with_intermediates(os.path.join(proc_cwd, '.Job.stdout.txt'), 'w')
        k['stdout'] = _stdout_fo
        if stderr_file_path:
            _stderr_fo = open_with_intermediates(stderr_file_path, 'w')
        else:
            _stderr_fo = open_with_intermediates(os.path.join(proc_cwd, '.Job.stderr.txt'), 'w')
        k['stderr'] = _stderr_fo

        for key, v in self.environ.items():
            os.environ[key] = v

        err_msg = []
        err_msg.append("PASTA failed because one of the programs it tried to run failed.")
        err_msg.append('The invocation that failed was: \n    "%s"\n' % '" "'.join(self._invocation))

        self.return_code = 0  # Initialization of return code

        try:

            command = ""
            for item in self._invocation:
                command = command + " "+item

            # MESSENGER.send_info("[JMAbuin] Initial command "+command)

            if configure_spark.isSpark():

                fileIndex = 0

                if(self._invocation[fileIndex] == "java"): # Case of opal
                    # MESSENGER.send_info("[JMAbuin] We are launching OPAL")
                    fileIndex = 3

                    # Check if Java is in our path
                    # MESSENGER.send_info("[JMAbuin] JAVA_HOME is " + os.getenv('JAVA_HOME'))
                    # MESSENGER.send_info("[JMAbuin] PATH is " + os.getenv('PATH'))

                    newJava = os.getenv('JAVA_HOME')+"/bin/java"

                    if newJava is not None:
                        self._invocation[0] = newJava

                    '''
                    current_dir = os.getcwd()
                    current_theoretical_dir = ""
                    currentItem = 0
                    totalItems = len(self._invocation[5].split("/"))

                    for item in self._invocation[5].split("/"):
                        if(currentItem > 0) and (totalItems - currentItem > 7):
                            current_theoretical_dir = current_theoretical_dir + "/" + item
                        currentItem += 1

                    if(current_dir != current_theoretical_dir):
                        self._invocation[5] = self._invocation[5].replace(current_theoretical_dir, current_dir)
                        self._invocation[7] = self._invocation[5].replace(current_theoretical_dir, current_dir)
                        self._invocation[9] = self._invocation[5].replace(current_theoretical_dir, current_dir)
                    '''
                    # Example: tempAHc28U/step0/centroid/pw/r5d2_r5d1/tempopal_waXAP/out.fasta
                    # First file:
                    self.changePermissions(self._invocation[5], 7)
                    self.changePermissions(self._invocation[7], 7)
                    self.changePermissions(self._invocation[9], 7)

                execname = os.path.basename(self._invocation[fileIndex])

                if (not os.path.isfile(self._invocation[fileIndex])):
                    MESSENGER.send_warning("[JMAbuin] " + self._invocation[fileIndex] + " does not exists! Finding it.")

                    if(os.path.isfile(os.getcwd() + "/pasta.zip/bin/" + execname)):
                        MESSENGER.send_info("[JMAbuin] Found " + execname + "!! => " + os.getcwd() + "/pasta.zip/bin/" + execname)
                        self._invocation[fileIndex] = os.getcwd() + "/pasta.zip/bin/" + execname

                    else:

                        newInvocationPath = self.findFile("../", execname)

                        if (os.path.isfile(newInvocationPath)):
                            MESSENGER.send_info("[JMAbuin] new " + execname + " path is " + newInvocationPath)
                            self._invocation[fileIndex] = newInvocationPath
                        else:
                            MESSENGER.send_error("[JMAbuin] Could not find " + execname + "!!")

                # MESSENGER.send_info("[JMAbuin] Final " + execname + " => " + self._invocation[fileIndex])

                command = ""
                for item in self._invocation:
                    command = command + " " + item

                #number_of_cpus = os.environ["OMP_NUM_THREADS"]

                #MESSENGER.send_info("[JMAbuin] Final command " + command + "with " + number_of_cpus + " CPUS")

            startTime = time.time()

            process = Popen(self._invocation, stdin=PIPE, **k)
            self.return_code = process.wait() # Chema aqui

            endTime = time.time()

            # process = Popen(self._invocation, stdin=PIPE, **k)
            # (output, output_err) = process.communicate()
            # self.return_code = process.returncode

            MESSENGER.send_info("[JMAbuin] :: run :: return code from " + self._invocation[0] + " is: " + str(self.return_code) + " and execution time is: " + str(endTime - startTime) + " seconds.")

            if "fasttreeMP" in self._invocation[0]:
                MESSENGER.send_info("[JMAbuin] running fastree in parallel")

                command = ""
                for item in self._invocation:
                    command = command + " " + item

                number_of_cpus = os.environ["OMP_NUM_THREADS"]

                MESSENGER.send_info("[JMAbuin] Final command " + command + " with " + number_of_cpus+" CPUS")

            _stdout_fo.close()
            _stderr_fo.close()
            process.stdin.close()

            if self.return_code < 0:
                errorFromFile = self.read_stderr(_stderr_fo)
                if errorFromFile:
                    err_msg.append(errorFromFile)
                    # err_msg.append(output_err)
                self.error = "\n".join(err_msg)
                raise Exception("")
            _LOG.debug('Finished %s.\n Return code: %s; %s' % (" ".join(self._invocation), self.return_code, self.error))

        except OSError as ose:
            err_msg.append(str(ose))
            self.error = "\n".join(err_msg)

            MESSENGER.send_error("[JMAbuin] " + ose.message)
            MESSENGER.send_error("[JMAbuin] " + ose.child_traceback)
            _LOG.error(self.error)
            sys.exit(ose.message + " :: "+ose.child_traceback)

        except Exception as e:
            err_msg.append(str(e.message))
            self.error = "\n".join(err_msg)
            MESSENGER.send_error("[JMAbuin] " + self.error)
            _LOG.error(self.error)
            sys.exit(e.message)

    def findMafft(self, path):
        for root, dirs, files in os.walk(path):
            if "mafft" in files:
                return os.path.join(root, "mafft")

        return ""

    def runwithpipes(self):
        k = self._k
        # Use a working temporary directory in the cluster nodes
        with TemporaryDirectory() as tempdir:
            k['cwd'] = tempdir
            k['stderr'] = PIPE
            k['stdout'] = PIPE

            for key, v in self.environ.items():
                os.environ[key] = v

            _LOG.debug('Launching %s.' % " ".join(self._invocation))
            _LOG.debug('Options %s.', k)

            command = ""
            for item in self._invocation:
               command = command + " "+item
            MESSENGER.send_info("[JMAbuin] Initial mafft command "+command)

            # process = Popen(command, stdin=PIPE, shell=True, **k)

            err_msg = []
            err_msg.append("PASTA failed because one of the programs it tried to run failed.")
            err_msg.append('The invocation that failed was: \n    "%s"\n' % '" "'.join(self._invocation))

            output = ""

            try:
                if (os.path.isfile(self._invocation[0])):
                    MESSENGER.send_info("[JMAbuin] " + self._invocation[0] + " exists!")
                else:
                    MESSENGER.send_warning("[JMAbuin] " + self._invocation[0] + " does not exists! Finding it.")

                    if (os.path.isfile(os.getcwd() + "/pasta.zip/bin/mafft")):
                        MESSENGER.send_info("[JMAbuin] Found mafft!! => " + os.getcwd() + "/pasta.zip/bin/mafft")
                        self._invocation[0] = os.getcwd() + "/pasta.zip/bin/mafft"

                    else:

                        newMafftPath = self.findMafft("../")
                        if(os.path.isfile(newMafftPath)):
                            MESSENGER.send_info("[JMAbuin] new found mafft path is "+newMafftPath)
                            self._invocation[0] = newMafftPath
                        else:
                            MESSENGER.send_error("[JMAbuin] Could not find mafft!!")

                    MESSENGER.send_info("[JMAbuin] Final mafft" + self._invocation[0])

                startTime = time.time()
                process = Popen(self._invocation, stdin=PIPE, **k)

                (output, output_err) = process.communicate()

                endTime = time.time()

                self.return_code = process.returncode

                MESSENGER.send_info("[JMAbuin] :: runwithpipes :: return code from " + self._invocation[0] + " is: " + str(self.return_code) + " and execution time is: " + str(endTime - startTime) + " seconds.")

                process.stdin.close()
                process.stdout.close()
                process.stderr.close()

                if self.return_code:
                    # errorFromFile = self.read_stderr(_stderr_fo)
                    if output_err:
                        err_msg.append(output_err)
                    self.error = "\n".join(err_msg)
                    raise Exception("")
                _LOG.debug(
                    'Finished %s.\n Return code: %s; %s' % (" ".join(self._invocation), self.return_code, self.error))

            except OSError as ose:
                err_msg.append(str(ose))
                MESSENGER.send_error("[JMAbuin] " + ose.message)
                MESSENGER.send_error("[JMAbuin] " + ose.child_traceback)
                sys.exit(ose.message+" :: "+ose.child_traceback)

            except Exception as e:
                err_msg.append(str(e))
                MESSENGER.send_error("[JMAbuin] " + str(e))
                self.error = "\n".join(err_msg)
                _LOG.error(self.error)
                sys.exit(e.message)
            return output


class pworker():
    def __init__(self, q, err_shared_obj):
        self.q = q
        self.err_shared_obj = err_shared_obj
        
    def __call__(self):
        while True:                                
            try:
                job = self.q.get()
                job.run()
                self.err_shared_obj.value = job.return_code
                self.q.task_done()
            except:
                err = StringIO()
                traceback.print_exc(file=err)
                _LOG.error("Process Worker dying.  Error in job.start = %s" % err.getvalue())
                raise
        return

_manager = Manager()

class worker():
    
    def __init__(self):
        global _manager
        self.pqueue = _manager.Queue()
        self.err_shared_obj = Value('i', 0)
        pw = pworker(self.pqueue, self.err_shared_obj)
        self.p = Process(target=pw)
        self.p.daemon = True
        self.p.start()

    def stop(self):
        self.p.terminate()
        
    def __call__(self):                            
        while True:            
            job = jobq.get()            
            ID = int(random() *10000000)
            TIMING_LOG.info("%s (%d) started" % (str(job.context_str),ID))
            try:
                if isinstance(job, DispatchableJob):
                    pa = job.start()
                    plj = LightJobForProcess(pa[0], pa[1], os.environ)
                    self.pqueue.put(plj)
                    
                    self.pqueue.join()   
                    
                    plj.return_code = self.err_shared_obj.value
                                             
                    if plj.error is not None:
                        job.error = Exception(plj.error)
                        
                    job.return_code = plj.return_code
                    
                    if job.return_code is not None and job.return_code != 0:
                        raise Exception("Job:\n %s\n failed with error code: %d" %(' '.join(plj._invocation), job.return_code))
                    
                    job.results = job.result_processor()
                    
                    job.finished_event.set() 
                    job.get_results()
                    job.postprocess()
                else:                    
                    job.start()
                    job.get_results()
                    job.postprocess()

            except Exception as e:
                err = StringIO()
                traceback.print_exc(file=err)
                _LOG.error("Worker dying.  Error in job.start = %s" % err.getvalue())
                job.error=e
                job.return_code = -1
                job.finished_event.set() 
                job.kill()
                kill_all_jobs()
                return                
            TIMING_LOG.info("%s (%d) completed" % (str(job.context_str),ID))
            jobq.task_done()
        return

# We'll keep a list of Worker threads that are running in case any of our code triggers multiple calls
_WORKER_THREADS = []
_WORKER_OBJECTS = []

def start_worker(num_workers):
    """Spawns worker threads such that at least `num_workers` threads will be
    launched for processing jobs in the jobq.

    The only way that you can get more than `num_workers` threads is if you
    have previously called the function with a number > `num_workers`.
    (worker threads are never killed).
    """
    assert num_workers > 0, "A positive number must be passed as the number of worker threads"
    num_currently_running = len(_WORKER_THREADS)
    for i in range(num_currently_running, num_workers):
        _LOG.debug("Launching Worker thread #%d" % i)
        w = worker()
        t = Thread(target=w)
        _WORKER_THREADS.append(t)
        _WORKER_OBJECTS.append(w)
        t.setDaemon(True)
        t.start()
        
def stop_worker():
    for w in _WORKER_OBJECTS:
        w.stop()

class JobBase(object):
    def __init__(self, **kwargs):
        self.context_str = kwargs.get("context_str")
        if "context_str" in kwargs:
            del kwargs['context_str']
        self._kwargs = kwargs


class DispatchableJob(JobBase):
    def __init__(self, invocation, result_processor, **kwargs):
        global _all_dispatchable_jobs
        JobBase.__init__(self, **kwargs)
        self._invocation = invocation
        # _LOG.debug('DispatchableJob.__init__(invocation= %s )' % " ".join(self._invocation))
        # Not sure why it does not work with datatype in treebuild.create_job
        self.result_processor = result_processor
        self.return_code = None
        self.results = None
        self._id = None
        self._stdout_fo = None
        self._stderr_fo = None
        self.finished_event = Event()        
        self.error = None
        _all_dispatchable_jobs.append(self)

    def get_id(self):
        return self._id

    def set_id(self, i):
        self._id = i

    id = property(get_id, set_id)

    def start(self):
        try:
            #_LOG.debug('launching %s.\n setting event' % " ".join(self._invocation))            
            k = dict(self._kwargs)
            return (self._invocation, k)
            #self.process = Popen(self._invocation, stdin = PIPE, **k)
            #self.set_id(self.process.pid)
            #f = open('.%s.pid' % self.get_id(), 'w')
            #f.close()            
        except:
            self.error = RuntimeError('The invocation:\n"%s"\nfailed' % '" "'.join(self._invocation))
            raise

####
#   Polling does not appear to be needed in the current impl.
#   def poll(self):
#       "Not blocking"
#       if self.return_code is None:
#           self.return_code = self.process.poll()
#       return self.return_code

    def wait(self):
        """Blocking.

        I'm not sure that it is safe for multiple threads calling self.process.wait
        so we'll only have the first thread do this.  All other threads that enter
        wait will wait for the finished_event
        """
        
        # this branch is actually monitoring the process
        if self.error is not None:
            raise self.error

        self.finished_event.wait() 
       
        if self.error is not None:
            raise self.error                        
        
        return self.return_code

    def get_results(self):
        if self.error is not None:
            raise self.error
        if self.results is None:
            self.wait()
        else:
            pass
            # os.remove('.%s.pid' % self.get_id())  # treebuild_job will not go through this after finish
        return self.results
    
    def postprocess(self):
        pass
    
    def kill(self):
        self.finished_event.set()
            
            
class TickableJob():

    def __init__(self):
        self._parents = []
        self._unfinished_children = []
        self._childrenlock = Lock()
        self._parentslock = Lock()

    def on_dependency_ready(self):
        ''' This function needs to be implemented by the children. 
                    This will be called when all children are done'''
        raise NotImplementedError("on_dependency_ready() method is not implemented.")

    def add_parent(self,parentjob):
        self._parentslock.acquire()
        self._parents.append(parentjob)
        self._parentslock.release()	

    def add_child(self,childJob):
        self._childrenlock.acquire()
        self._unfinished_children.append(childJob)
        self._childrenlock.release()

    def tick(self, finishedjob):
        #_LOG.debug("ticking %s" %str(self))
        self._childrenlock.acquire()
        self._unfinished_children.remove(finishedjob)   
        #_LOG.debug("children ... %d" %len(self._unfinished_children))     
        nochildleftbehind = not bool(self._unfinished_children)
        self._childrenlock.release()
        if nochildleftbehind:
            self.on_dependency_ready()
            
    def tick_praents(self):
        self._parentslock.acquire()
        for parent in self._parents:
            parent.tick(self)
        self._parentslock.release()  
        
    def kill(self):    
        self._parentslock.acquire()
        for parent in self._parents:
            parent.kill()
        self._parentslock.release()          

class TickingJob():
    def __init__(self):
        self.parent_tickable_job = []
        self.killed = False
    
    def add_parent_tickable_job(self, tickableJob):
        self.parent_tickable_job.append(tickableJob)
        
    def postprocess(self):
        for parent in self.parent_tickable_job:
            parent.tick(self)
            
    def kill(self):
        if not self.killed:
            self.killed = True
            for parent in self.parent_tickable_job:
                parent.kill()            

class TickingDispatchableJob(DispatchableJob, TickingJob):
    def __init__(self, invocation, result_processor, **kwargs):
        DispatchableJob.__init__(self, invocation, result_processor, **kwargs)
        TickingJob.__init__(self)

    def kill(self):
        DispatchableJob.kill(self)
        TickingJob.kill(self)

    def postprocess(self):
        TickingJob.postprocess(self)


class FakeJob(JobBase, TickingJob):
    """FakeJob instances are used in cases in which we know have the results of
    an operation, but need to emulate the API of DispatchableJob.
    """
    def __init__(self, results, **kwargs):
        JobBase.__init__(self, **kwargs)
        TickingJob.__init__(self)
        self.results = results

    def start(self):
        pass

    def wait(self):
        pass

    def get_results(self):
        return self.results
    
    def kill(self):
        pass
