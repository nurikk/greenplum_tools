import psycopg2, psycopg2.extras

import random
import re
import argparse
import shlex
from subprocess import Popen, PIPE
from datetime import datetime
import time
from multiprocessing.dummy import Pool
import logging



def get_cursor(config):
    conn = psycopg2.connect("dbname={database} user={user} host={host} port={port}".format(**config))
    conn.autocommit = True
    cursor =  conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
    return cursor

def out(cursor, sql, params = {}):
    cursor.execute(sql, params)
    return_val = []
    try:
        return_val = cursor.fetchall()
    except psycopg2.ProgrammingError:
        pass
    return return_val

def worker(item, config):
    cursor = get_cursor(config)
    logging.info('Start ' + item['cmd'])
    # out(cursor, item['cmd'])
    logging.info('Finish ' + item['cmd'])


def run_parallel(config, commands):
    thread_params = []
    for command in commands:
        thread_params.append((command, config))
    results = Pool(config['threads']).starmap(worker, thread_params)

def vacuum_ao_tables(config):
    logging.info("VACUUM all append optimized tables with bloat")
    logging.info("Utilize the toolkit schema to identify ao tables that have excessive bloat and need")
    logging.info("to be vacuumed.")
    SQL = """
        SELECT 'VACUUM' || ' ' || table_name as cmd
    	FROM    (
    	        SELECT n.nspname || '.' || c.relname AS table_name, (gp_toolkit.__gp_aovisimap_compaction_info(c.oid)).compaction_possible AS compaction_possible
    	        FROM pg_appendonly a
    	        JOIN pg_class c ON c.oid = a.relid
    	        JOIN pg_namespace n ON c.relnamespace = n.oid
    	        WHERE c.relkind = 'r'
    	        AND c.reltuples > 0
    	        ) AS sub
    	WHERE compaction_possible
    	GROUP BY table_name
    """
    commands = out(get_cursor(config), SQL)
    run_parallel(config, commands)

def vacuum_system_catalog(config):
    logging.info("VACUUM ANALYZE the pg_catalog")
    logging.info("Creating and dropping database objects will cause the catalog to grow in size so that")
    logging.info("there is a read consistent view.  VACUUM is recommended on a regular basis to prevent")
    logging.info("the catalog from suffering from bloat. ANALYZE is also recommended for the cost based")
    logging.info("optimizer to create the best query plans possble when querying the catalog.")
    SQL = """
    SELECT 'VACUUM ANALYZE ' || n.nspname || '.' || c.relname as cmd
    FROM pg_class c
    JOIN pg_namespace n ON c.relnamespace = n.oid
    WHERE n.nspname = 'pg_catalog'
    AND c.relkind = 'r'
    """
    commands = out(get_cursor(config), SQL)
    run_parallel(config, commands)

def remove_orphaned_tables(config):
    logging.info("Remove orphaned tables")
    SQL = """
        SELECT 'drop schema if exists ' || nspname || ' cascade;' as cmd
        FROM
            (SELECT nspname
             FROM pg_namespace
             WHERE nspname LIKE 'pg_temp%%'
             UNION SELECT nspname
             FROM gp_dist_random('pg_namespace')
             WHERE nspname LIKE 'pg_temp%%'
             EXCEPT SELECT 'pg_temp_' || sess_id::varchar
             FROM pg_stat_activity
           ) AS foo
    """

    commands = out(get_cursor(config), SQL)
    run_parallel(config, commands)


def analyze_missing_stats_tables(config):
    logging.info("ANALYZE all tables/partitions with missing statistics.")
    SQL = """
        SELECT 'ANALYZE  ' || n.nspname || '.' || c.relname as cmd
        FROM pg_class c
        JOIN pg_namespace n ON c.relnamespace = n.oid
        JOIN gp_toolkit.gp_stats_missing g ON g.smischema = n.nspname AND g.smitable = c.relname
        LEFT JOIN       (--top level partitioned tables
                        SELECT c.oid
                        FROM pg_class c
                        LEFT JOIN pg_inherits i ON c.oid = i.inhrelid
                        WHERE i.inhseqno IS NULL
                        ) pt ON c.oid = pt.oid
        WHERE c.relkind = 'r' and c.reltuples > 0
        AND pt.oid IS NULL
    """
    commands = out(get_cursor(config), SQL)
    run_parallel(config, commands)


def vacuum_vacuum_freeze_min_age(config):
    logging.info("VACUUM all tables near the vacuum_freeze_min_age to prevent transaction wraparound")
    SQL = """
        SELECT 'VACUUM  ' || n.nspname || '.' || c.relname as cmd
        FROM pg_class c
        JOIN pg_namespace n ON c.relnamespace = n.oid
        WHERE age(relfrozenxid) > (select setting from pg_settings where name = 'vacuum_freeze_min_age')::bigint
        AND c.relkind = 'r';
    """
    commands = out(get_cursor(config), SQL)
    run_parallel(config, commands)


def vaccum_heap(config):
    logging.info("VACUUM all heap tables with bloat")
    logging.info("Utilize the toolkit schema to identify heap tables that have excessive bloat and need")
    logging.info("to be vacuumed.")
    SQL = """
    SELECT 'VACUUM  ' || bdinspname || '.' || bdirelname  as cmd
    FROM gp_toolkit.gp_bloat_diag WHERE bdinspname <> 'pg_catalog'
    """
    commands = out(get_cursor(config), SQL)
    run_parallel(config, commands)

def reindexdb_system_catalog(config):
    logging.info("REINDEX the pg_catalog.")
    logging.info("Reindexing the catalog indexes will help prevent bloat or poor performance when")
    logging.info("querying the catalog")
    SQL = 'REINDEX SYSTEM {database}'.format(**config)
    out(get_cursor(config), SQL)

if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO,
                    format='%(asctime)s - %(threadName)s - %(message)s',
                    datefmt='%Y-%m-%d %H:%M:%S')
    parser = argparse.ArgumentParser()

    parser.add_argument("--database", type=str, help="db name", default="db")
    parser.add_argument("--host", type=str, help="hostname", default="localhost")
    parser.add_argument("--port", type=int, help="port", default=5432)
    parser.add_argument("--threads", type=int, help="threads count", default=5)
    parser.add_argument("--user", type=str, help="username", default='gpadmin')


    params = parser.parse_args()
    remove_orphaned_tables(vars(params))
    vacuum_vacuum_freeze_min_age(vars(params))
    analyze_missing_stats_tables(vars(params))
    vacuum_system_catalog(vars(params))
    reindexdb_system_catalog(vars(params))
    vaccum_heap(vars(params))
    vacuum_ao_tables(vars(params))
