import psycopg2, psycopg2.extras

import random
import re
import argparse
import shlex
from subprocess import Popen, PIPE
from datetime import datetime
import time


filter_string_re = re.compile('^(SET|CREATE INDEX|CREATE  PROTOCOL|--|$)')
def get_table_ddl(config):
    cmd = 'pg_dump --host={host} --port={port} --username={user} --no-owner --no-privileges --schema-only --table={schema}.{table} {database}'.format(**config)
    args = shlex.split(cmd)
    proc = Popen(args, stdout=PIPE, stderr=PIPE, env={'PATH': config['root']})
    out, err = proc.communicate()
    out_data = list(filter(lambda st: not filter_string_re.match(st), out.decode('utf-8').split('\n')))
    return '\n'.join(out_data).strip()

def get_cursor(config):
    conn = psycopg2.connect("dbname={database} user={user} host={host} port={port}".format(**config))
    conn.autocommit = False
    cursor =  conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
    return cursor


GET_INDEX_SQL = """
    SELECT c2.relname as index_name, pg_catalog.pg_get_indexdef(i.indexrelid, 0, true) as index_def
    FROM pg_catalog.pg_class c, pg_catalog.pg_class c2, pg_catalog.pg_index i
    WHERE c.oid = %(table)s::regclass::oid AND c.oid = i.indrelid AND i.indexrelid = c2.oid and i.indisvalid
    ORDER BY i.indisprimary DESC, i.indisunique DESC, c2.relname
"""

def format_seconds_to_readable_format(seconds):
    return time.strftime("%H:%M:%S.{0}".format(round((seconds % 1)*1000)), time.gmtime(seconds))

def out(cursor, sql, params = {}):
    print(cursor.mogrify(sql, params).decode('utf-8'), end=' ', flush=True)
    start = time.time()
    cursor.execute(sql, params)
    print('--', format_seconds_to_readable_format(time.time() - start), cursor.statusmessage)

def repack(config):






if __name__ == "__main__":
    parser = argparse.ArgumentParser()

    parser.add_argument("--database", type=str, help="db name", default="db")
    parser.add_argument("--host", type=str, help="hostname", default="localhost")
    parser.add_argument("--port", type=int, help="port", default=6543)
    parser.add_argument("--user", type=str, help="username", default='gpadmin')
    parser.add_argument("--root", type=str, help="$GPHOME/bin", default='/usr/local/greenplum-db/bin')

    parser.add_argument("-t", "--table", type=str, help="table name", required=True)
    parser.add_argument("-s", "--schema", type=str, help="schema name", required=True)
    parser.add_argument("-o", "--order-col", type=str, help="schema name")

    params = parser.parse_args()
    repack(vars(params))
