"""Utils file.

This file is to house helper code for the Master

"""

import pathlib
import shutil
import os
import glob
import heapq
import logging
import time
from itertools import cycle
from mapreduce.utils import tcp_send


def setup_files(self):
    """Format tmp folder. Uses relative path per piaze post @1505."""
    logging.debug("Setting up files for master port %s", self.port)
    pathlib.Path('tmp').mkdir(parents=True, exist_ok=True)
    bad_files = glob.glob(os.path.join('tmp', '*'))
    for file in bad_files:
        shutil.rmtree(file)


def query_workers(self, statuses):
    """Return tuple of worker pids and workers with any matching status."""
    # STATUSES MUST BE ITERABLE
    ret = []
    for worker_pid, worker in self.registered_workers.items():
        worker_status = worker["status"]
        matches = [worker_status == test_status for test_status in statuses]

        if any(matches):
            ret.append((worker_pid, self.registered_workers[worker_pid]))

    return ret


def setup_job_files(job_number):
    """Set up tmp job files."""
    job_number = str(job_number)
    pathlib.Path('tmp/job-'+job_number).mkdir()
    pathlib.Path('tmp/job-'+job_number+'/mapper-output').mkdir()
    pathlib.Path('tmp/job-'+job_number+'/grouper-output').mkdir()
    pathlib.Path('tmp/job-'+job_number+'/reducer-output').mkdir()


def get_map_tasks(msg):
    """Split input directory according to number of mappers."""
    map_tasks = [[] for _ in range(msg["num_mappers"])]

    cnt = 0
    for fle in os.listdir(msg["input_directory"]):
        fname = msg["input_directory"] + f"/{fle}"
        map_tasks[cnt % len(map_tasks)].append(fname)
        cnt += 1

    return map_tasks


def send_map_jobs(self, job, tasks):
    """Send new worker task of map or reduce to available workers."""
    job_number = job["job_number"]
    message = {
        "message_type":     "new_worker_job",
        "input_files":      [],                             # set in loop
        "executable":       job["mapper_executable"],
        "output_directory": f"tmp/job-{job_number}/mapper-output",
        "worker_pid": -1                                    # set in loop
    }

    while True:
        time.sleep(0.1)
        available_workers = query_workers(self, ["alive"])
        if len(available_workers) > 0:
            break

    for worker_pid, worker in cycle(available_workers):
        # get task to assign
        try:
            task = tasks.pop()
        except IndexError:
            # all tasks assigned
            break

        # finish message
        message["input_files"] = task
        message["worker_pid"] = worker_pid

        # send message to worker
        tcp_send(message, worker["port"])

        # record pending job
        for fle in task:
            smallf = fle.split("/")[-1]
            self.pending_jobs.add(smallf)
            logging.debug("Marked map %s as TODO.", smallf)

        # record as registered worker's task
        worker["tasks"].append(message)
        logging.debug("Assigned map %s to worker %s.", task, worker_pid)


def send_sort_jobs(self, job):
    """Send number of sort jobs equivilent to number of reducers."""
    message = {
        "message_type": "new_sort_job",
        "input_files": [],                      # set in loop
        "output_file": "",                      # set in loop
        "worker_pid": -1                        # set in loop
    }

    # wait for available workers
    while True:
        time.sleep(0.1)
        available_workers = query_workers(self, ["alive"])
        if len(available_workers) > 0:
            break

    # make sort tasks
    tasks = [[] for _ in range(len(available_workers))]
    cnt = 0
    job_number = job["job_number"]
    for fle in os.listdir(f"tmp/job-{job_number}/mapper-output"):
        fname = f"tmp/job-{job_number}/mapper-output/{fle}"
        tasks[cnt % len(tasks)].append(fname)
        cnt += 1

    # assign sort tasks to workers
    cnt = 0
    for worker_pid, worker in cycle(available_workers):
        # get task to assign
        try:
            task = tasks.pop()
        except IndexError:
            # all tasks assigned
            break

        # finish message
        message["input_files"] = task
        message["worker_pid"] = worker_pid
        message["output_file"] = (f"tmp/job-{job_number}"
                                  f"/grouper-output/sorted{cnt+1:02d}")
        cnt += 1

        # send message to worker
        tcp_send(message, worker["port"])

        # record pending job small name
        self.pending_jobs.add(message["output_file"].split("/")[-1])

        # record as registered worker's task
        worker["tasks"].append(message)
        logging.debug("Assigned sort %s to worker %s.", task, worker_pid)


def merge_sorted(self, job):
    """Merge k sorted files to k files with no overlapping bondaries."""
    logging.info("Merging sorts for master port %s", self.port)
    # to store opened I/O files
    sorted_files = []
    reduce_files = []

    job_number = job["job_number"]  # gave me a syntax error otherwise...

    # open all of the 'sortedX' files
    for fle in os.listdir(f"tmp/job-{job_number}/grouper-output"):
        if "sorted" in fle:
            o_p = open(f"tmp/job-{job_number}/grouper-output/{fle}", "r")
            sorted_files.append(o_p)

    # open all reduce files
    for i in range(job["num_reducers"]):
        o_p = open(f"tmp/job-{job_number}/grouper-output/reduce{i+1:02d}", "w")
        reduce_files.append(o_p)

    # merge all of the opened files
    # h is an iterator
    heap = heapq.merge(*sorted_files)

    # start number of keys at 0
    key_num = 0
    last_key = None

    # iterate through h
    while True:
        try:
            i = next(heap)
        except StopIteration:
            break

        # get key
        # logging.debug(f"{i}")
        try:
            curr_key, _ = i.split()
        except ValueError:
            # to handle ValueError: not enough values to unpack
            curr_key = " "

        # check if the key has changed (indicates whether to switch files)
        if last_key != curr_key:
            if last_key is not None:
                key_num += 1
            last_key = curr_key

        # compute which file and write to it
        reduce_num = (key_num % job["num_reducers"])
        reduce_files[reduce_num].write(i)

    # close all of the 'sortedX' files
    for fle in sorted_files:
        fle.close()

    # close all of the 'reduceX' files
    for fle in reduce_files:
        fle.close()


def send_reduce_tasks(self, job):
    """Get reduce tasks from grouper-output and sends to workers."""
    job_number = job["job_number"]
    message = {
        "message_type":     "new_worker_job",
        "input_files":      [],                             # set in loop
        "executable":       job["reducer_executable"],
        "output_directory": f"tmp/job-{job_number}/reducer-output",
        "worker_pid": -1                                    # set in loop
    }

    # get reduce tasks
    reduce_tasks = []
    job_number = job["job_number"]
    for fle in os.listdir(f"tmp/job-{job_number}/grouper-output"):
        if "reduce" in fle:
            fname = f"tmp/job-{job_number}/grouper-output/{fle}"
            reduce_tasks.append([fname])

    # wait for available workers
    while True:
        time.sleep(0.1)
        available_workers = query_workers(self, ["alive"])
        if len(available_workers) > 0:
            break

    for worker_pid, worker in cycle(available_workers):
        # get task to assign
        try:
            task = reduce_tasks.pop()
        except IndexError:
            # all tasks assigned
            break

        # finish message
        message["input_files"] = task
        message["worker_pid"] = worker_pid

        # send message to worker
        tcp_send(message, worker["port"])

        # record pending job
        for fle in message["input_files"]:
            smallf = fle.split("/")[-1]
            logging.debug("Adding %s as pending reduce job", smallf)
            self.pending_jobs.add(smallf)

        # record as registered worker's task
        worker["tasks"].append(message)
        logging.debug("Assigned reduce %s to worker %s.", task, worker_pid)


def move_output(job):
    """Move outputfile to output_directory."""
    dst = job["output_directory"]
    pathlib.Path(job["output_directory"]).mkdir(parents=True)
    job_number = job["job_number"]
    for fle in os.listdir(f"tmp/job-{job_number}/reducer-output"):
        src = f"tmp/job-{job_number}/reducer-output/{fle}"

        if job["output_directory"][-1] != "/":
            job["output_directory"] += "/"
        dst = job["output_directory"] + "outputfile" + fle[-2:]
        shutil.move(src, dst)

        logging.debug("Moving %s to %s", src, dst)


def redistribute(self, pids):
    """Redistribute jobs of dead worker."""
    # look for workers to redistribute to
    available_workers = query_workers(self, ["alive"])
    if len(available_workers) == 0:
        return
    for pid in pids:
        messages = self.registered_workers[pid]["tasks"]
        for worker_pid, worker in cycle(available_workers):
            # get task to assign
            try:
                task = messages.pop()
            except IndexError:
                # all tasks assigned
                break
            task["worker_pid"] = worker_pid
            print(task)
            tcp_send(task, worker["port"])

            worker["tasks"].append(task)
            logging.debug("Reassigned task to worker %s.", worker_pid)
        self.registered_workers[pid]["status"] = "dead"
