import psycopg2, psycopg2.extras

import random
import re
import argparse
import shlex
from subprocess import Popen, PIPE
from datetime import datetime
import time
import threading
from queue import Queue
import logging



def get_cursor(config):
    conn = psycopg2.connect("dbname={database} user={user} host={host} port={port}".format(**config))
    conn.autocommit = False
    cursor =  conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
    return cursor

def out(cursor, sql, params = {}):
    cursor.execute(sql, params)
    return_val = {}
    try:
        return_val = cursor.fetchall()
    except psycopg2.ProgrammingError:
        pass
    return return_val

def worker(config, q):
    cursor = get_cursor(config)
    while q.qsize() > 0:
        item = q.get()
        logging.info('Start ' + item['cmd'])
        # out(cursor, item['cmd'])
        logging.info('Finish ' + item['cmd'])
        q.task_done()

def run_parallel(config, commands):
    q = Queue()
    threads = []
    for i in range(config['threads']):
        t = threading.Thread(target=worker, args=(config, q))
        t.start()
        threads.append(t)

    for item in commands:
        q.put(item)
    q.join()
    for t in threads:
        t.join()

def vacuum_ao_tables(config):
    GET_BLOATED_SQL="""
        SELECT 'VACUUM' || ' ' || table_name as cmd
    	FROM    (
    	        SELECT n.nspname || '.' || c.relname AS table_name, (__gp_aovisimap_compaction_info(c.oid)).compaction_possible AS compaction_possible
    	        FROM pg_appendonly a
    	        JOIN pg_class c ON c.oid = a.relid
    	        JOIN pg_namespace n ON c.relnamespace = n.oid
    	        WHERE c.relkind = 'r'
    	        AND c.reltuples > 0
    	        ) AS sub
    	WHERE compaction_possible
    	GROUP BY table_name
    """
    tabbles_to_vacuum = out(get_cursor(config), GET_BLOATED_SQL)
    run_parallel(config, tabbles_to_vacuum)


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO,
                    format='%(asctime)s - %(threadName)s - %(message)s',
                    datefmt='%Y-%m-%d %H:%M:%S')
    parser = argparse.ArgumentParser()

    parser.add_argument("--database", type=str, help="db name", default="db")
    parser.add_argument("--host", type=str, help="hostname", default="localhost")
    parser.add_argument("--port", type=int, help="port", default=6543)
    parser.add_argument("--threads", type=int, help="threads count", default=5)
    parser.add_argument("--user", type=str, help="username", default='gpadmin')


    params = parser.parse_args()
    vacuum_ao_tables(vars(params))
