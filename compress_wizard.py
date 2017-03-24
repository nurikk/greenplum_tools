import psycopg2, psycopg2.extras

import random
import re
import argparse
from subprocess import Popen, PIPE
from datetime import datetime
import time
import threading


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

RLE_COMPRESSION_MAP = {
    'RLE_ONLY': 1,
    'RLE_ZLIB_1': 2,
    'RLE_ZLIB_5': 3,
    'RLE_ZLIB_9': 4,

}
compressions = {
    'RLE_TYPE': [1, 2, 3, 4],
    'ZLIB': [1, 5, 9],
    'QUICKLZ': [1]
}
def out_info(best, column_info, config, original_column_info):
    best_bage = ''
    if best:
        best_bage = '<<<BEST COMPRESSION RATIO'
        #TODO: suggest alter table
        ALTER_SQL = """ALTER TABLE {schema}.{table} ALTER COLUM {column_name}""".format(schema=config['schema'], table=config['table'], column_name=column_info['column_name'])
        # print(ALTER_SQL)
    current_bage = ''
    if original_column_info['compresslevel'] == column_info['compresslevel'] and original_column_info['compresstype'].lower() == column_info['compresstype'].lower():
        current_bage = '<<<CURRENT TYPE'
    print('--', column_info['column_name'], column_info['compresstype'], column_info['compresslevel'], column_info['size_h'], current_bage, best_bage)

def bench_column(config, column):
    curr = get_cursor(config)
    sample_table_sql = """
        CREATE TEMPORARY TABLE wizard_tmp
        AS
        SELECT * from {schema}.{table}
        LIMIT {lines}
    """.format(**config)
    out(curr, sample_table_sql)
    results = []
    for compresstype, levels in compressions.items():
        for compresslevel in levels:
            SQL = """
                CREATE TABLE compres_test_table
                WITH (
                  appendonly=true,
                  orientation=column,
                  compresstype={compresstype},
                  compresslevel={compresslevel}
                )
                AS (SELECT {column_name} from wizard_tmp)
            """.format(compresstype=compresstype,compresslevel=compresslevel, column_name=column['column_name'])
            out(curr, SQL)
            SIZE_SQL = """
                SELECT
                '{column_name}' as column_name,
                '{compresslevel}' as compresslevel,
                '{compresstype}' as compresstype,
                pg_size_pretty(pg_relation_size('compres_test_table'::regclass::oid)) as size_h,
                pg_relation_size('compres_test_table'::regclass::oid) as size
            """.format(compresstype=compresstype, compresslevel=compresslevel, column_name=column['column_name'])
            size_info = out(curr, SIZE_SQL)[0]
            results.append(size_info)
            out(curr, 'drop table compres_test_table')

    sorted_results = sorted(results, key=lambda k: k['size'])
    for idx, row in enumerate(sorted_results):
        out_info(idx == 0, row, config, column)

def format_col(source_col):
    col = {
        'column_name': source_col['column_name']
    }
    for opt in source_col['col_opts']:
        [param, value] = opt.split('=')
        col[param] = value
    return col

def make_magic(config):
    curr = get_cursor(config)
    TABLE_DESC_SQL = """
        SELECT a.attname as column_name,
        e.attoptions col_opts
        FROM pg_attribute a
        JOIN pg_class b ON (a.attrelid = b.relfilenode)
        JOIN pg_namespace n on (n.oid = b.relnamespace)
        LEFT OUTER JOIN pg_catalog.pg_attribute_encoding e ON e.attrelid = a.attrelid AND e.attnum = a.attnum
        WHERE
        b.relname = %(table)s
        and n.nspname = %(schema)s
        and a.attstattarget = -1
    """
    table_info = out(curr, TABLE_DESC_SQL, config)


    threads = []
    for column in table_info:
        column = format_col(column)
        thread = threading.Thread(target=bench_column, args=(config, column,))
        thread.start()
        threads.append(thread)

    for thread in threads:
        thread.join()


if __name__ == "__main__":
    parser = argparse.ArgumentParser()

    parser.add_argument("--database", type=str, help="db name", default="db")
    parser.add_argument("--host", type=str, help="hostname", default="localhost")
    parser.add_argument("--port", type=int, help="port", default=6543)
    parser.add_argument("--user", type=str, help="username", default='gpadmin')

    parser.add_argument("-t", "--table", type=str, help="table name", required=True)
    parser.add_argument("-s", "--schema", type=str, help="schema name", required=True)
    parser.add_argument("-l", "--lines", type=str, help="rows to examine", default=10000000)

    params = parser.parse_args()
    make_magic(vars(params))
