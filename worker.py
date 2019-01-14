import sys
sys.path = ['', '/home/www-data-login/anaconda3/lib/python36.zip', '/home/www-data-login/anaconda3/lib/python3.6', '/home/www-data-login/anaconda3/lib/python3.6/lib-dynload', '/home/www-data-login/anaconda3/lib/python3.6/site-packages', '/home/www-data-login/anaconda3/lib/python3.6/site-packages/torchvision-0.2.1-py3.6.egg']
sys.path.append("/home/www-data-login/visart_2018/cbir_1.0_master/source")

import json, multiprocessing, requests
import os, time, yaml, subprocess
from enum import Enum
import main_retrieval as SearchModule
from main_initialization import main_init

import logging

class Status(Enum):

    EMPTY = 10
    
    JOB_RUNNING = 21

    STOPPED_WITH_ERRORS = 31
    STOPPED_ON_REQUEST = 32

    FINISHED = 40


def helper_initialization(path):
    from main_initialization import main_init
    main_init(path)

def data_merge(a, b):
    key = None
    # ## debug output
    # sys.stderr.write("DEBUG: %s to %s\n" %(b,a))
    try:
        if a is None or isinstance(a, (str, float, int)):
            # border case for first run or if a is a primitive
            a = b
        elif isinstance(a, list):
            # lists can be only appended
            if isinstance(b, list):
                # merge lists
                a.extend(b)
            else:
                # append to list
                a.append(b)
        elif isinstance(a, dict):
            # dicts must be merged
            if isinstance(b, dict):
                for key in b:
                    if key in a:
                        a[key] = data_merge(a[key], b[key])
                    else:
                        a[key] = b[key]
            else:
                raise Error('Cannot merge non-dict "%s" into dict "%s"' % (b, a))
        else:
            raise Error('NOT IMPLEMENTED "%s" into "%s"' % (b, a))
    except TypeError as e:
        raise Error('TypeError "%s" in key "%s" when merging "%s" into "%s"' % (e, key, b, a))
    return a

def own_that_fXXXing_folder(path):
    subprocess.call(['echo "yC57QAf" | sudo -S chgrp -R www-data-login {:s}'.format(path)], shell = True)
    subprocess.call(['echo "yC57QAf" | sudo -S chmod -R g+w  {:s}'.format(path)], shell = True)

class Worker:

    API_TOKEN = '7sdf123sdfaqasdf7809a8'
    DATA_ROOT = '/media/dataDrive/www/dataNewInterface'
    TYPE = 0

    def __init__(self, id, main_config_path, description):

        self.id = id

        logging.debug('Created Worker (id: {}, type: {})'.format(id, self.TYPE))

        self.main_config_path = main_config_path
        with open(self.main_config_path) as f:
            self.main_config = yaml.load(f)

        logging.debug('Main config loaded (path: {})'.format(main_config_path))

        self.description = description

        self.status = Status.EMPTY

        self.proc = None
        self.thread = None

    def loop(self):
        while True:
            if self.proc is None:
                self.status = Status.EMPTY
                resp = self.request({
                    "description": self.description
                })

                logging.debug('Response from Webserver: take action '+resp["action"])

                if(resp["action"] == "new_job"):
                    self.run_job(resp["job"])

            else:
                logging.error('Method loop called without self.proc beeing None, this should not happen!')

            time.sleep(5)

    def request(self, data):

        req_data = {** {'status': self.status.value, 'token': self.API_TOKEN, 'type': self.TYPE}, ** data}

        r = requests.post("http://compvis10.iwr.uni-heidelberg.de/api/job/{}/update".format(self.id), json = req_data)
        response = json.loads(r.text)

        return response

    def set_status_by_exitcode(self, exitcode):
        if exitcode < 0:
            self.status = Status.STOPPED_ON_REQUEST
        elif exitcode == 0:
            self.status = Status.FINISHED
        else:
            self.status = Status.STOPPED_WITH_ERRORS

    def run_job(self, job):

        defaults = self.main_config.copy()
        job_params = json.loads(job["params"])
        params = data_merge(defaults, job_params)

        root_path = os.path.join(self.DATA_ROOT, 'images_{}'.format(job["collection_id"]), 'index_{}'.format(job["id"]))
        
        if "PATH" not in params:
            params["PATH"] = {}
        
        params["PATH"]["ROOT"] = root_path
        logging.debug('Start new index generation (path: {})'.format(root_path))

        # TODO: fix permissions
        own_that_fXXXing_folder(root_path)
        with open(os.path.join(root_path, 'config.json'), 'w') as f:
            json.dump(params, f)
    
        self.proc = multiprocessing.Process(target = helper_initialization, args=(os.path.join(root_path, 'config.json'), ))
        self.proc.start()
        self.status = Status.JOB_RUNNING

        while True:

            resp = self.request({

            })

            if resp["action"] == "cancel":
                logging.debug('Webserver requested end of job, terminate process now')
                self.proc.terminate()


            if not self.proc.is_alive():
                break
            
            time.sleep(5)

        self.proc.join()
        exitcode = self.proc.exitcode
        self.set_status_by_exitcode(exitcode)
        

        logging.debug('Process finished with exitcode {}, set status to {}'.format(exitcode, self.status))

        resp = self.request({
            "exitcode": exitcode
        })

        self.proc = None

class SearchWorker(Worker):

    TYPE = 1

    def __init__(self, id, description):

        self.id = id

        logging.debug('Created SearchWorker (id: {}, type: {})'.format(id, self.TYPE))

        self.description = description
        self.status = Status.EMPTY

        self.proc = None

    def run_job(self, job):

        counter = 0
        self.current_index = 0

        while True:

            if job is not None:
                search_params = json.loads(job["params"])
                self.current_index = job["index_id"]
                root_path = os.path.join(self.DATA_ROOT, 'images_{}'.format(job["collection_id"]), 'index_{}'.format(job["index_id"]))
                self.status = Status.JOB_RUNNING
                resp = self.request({

                })
                exitcode = self.run_search(root_path, job["id"])
                self.set_status_by_exitcode(exitcode)
                resp = self.request({
                    "exitcode": exitcode
                })
                job = None
                self.status = Status.EMPTY

            else:
                resp = self.request({
                    "loaded_index": self.current_index
                })

                logging.debug('Response from Webserver: take action '+resp["action"])

                if(resp["action"] == "new_job"):
                    job = resp["job"]
                    counter = 0
                else:
                    counter += 1
                    logging.debug('No new job found, increase counter to {}'.format(counter))
                
                if counter == 120:
                    break

                time.sleep(5)
        
        self.proc = None

    def run_search(self, search_root, search_id):
        logging.debug('Start search with id {}'.format(search_id))
        own_that_fXXXing_folder(search_root)
        exitcode = SearchModule.main_ret(os.path.join(search_root, 'config.json'), search_id)
        logging.debug('Process finished with exitcode {}'.format(exitcode))
        return exitcode




worker_id = sys.argv[1]
worker_type = int(sys.argv[2])
Worker.API_TOKEN = sys.argv[3]
worker_description = sys.argv[4]
logging.basicConfig(filename='worker_'+worker_id+'.log', level=logging.ERROR)

worker = None
if worker_type == 0:
    worker_config = sys.argv[5]
    worker = Worker(worker_id, worker_config, worker_description)
else:
    worker = SearchWorker(worker_id, worker_description)

worker.loop()