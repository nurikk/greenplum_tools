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
    # print(cursor.mogrify(sql, params).decode('utf-8'))
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
def is_current_compression_method(original_column_info, column_info):
    return original_column_info.get('compresslevel', None) == column_info.get('compresslevel', None) and original_column_info.get('compresstype', '') == column_info.get('compresstype', '').lower()

def out_info(sorted_results, original_column_info):
    current_column = {'size': sorted_results[0]['size']}
    for column_info in sorted_results:
        if is_current_compression_method(original_column_info, column_info):
            current_column = column_info

    print('-----', original_column_info['column_name'], '-----')

    #TODO: suggest alter table alter column if it posible
    for column_info in sorted_results:
        current_text = ''
        if  column_info == current_column:
            current_text = '<<<CURRENT'
        if current_column:
            diff = str(round(100.0 / current_column['size'] * column_info['size'], 2)) + ' %'
            print('--', column_info['column_name'], column_info['compresstype'], column_info['compresslevel'], column_info['size_h'], diff, current_text)
        else:
            print('--', column_info['column_name'], column_info['compresstype'], column_info['compresslevel'], column_info['size_h'], current_text)

def bench_column(config, column):
    curr = get_cursor(config)
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
                AS (SELECT {column_name} from {schema}.{table} LIMIT {lines})
            """.format(compresstype=compresstype,compresslevel=compresslevel, column_name=column['column_name'], schema=config['schema'], table=config['table'], lines=config['lines'])
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
    out_info(sorted_results, column)

def format_col(source_col):
    col = {
        'column_name': source_col['column_name']
    }
    opts = source_col.get('col_opts', [])

    if opts is None:
        return col

    for opt in opts:
        [param, value] = opt.split('=')
        col[param] = value.lower()
    return col

def make_magic(config):
    curr = get_cursor(config)
    TABLE_DESC_SQL = """
        SELECT a.attname as column_name,
        e.attoptions as col_opts
        FROM pg_catalog.pg_attribute a
        LEFT  JOIN pg_catalog.pg_attribute_encoding e ON  e.attrelid = a.attrelid AND e.attnum = a.attnum
        LEFT JOIN pg_catalog.pg_class c ON c.oid = a.attrelid
        LEFT JOIN pg_catalog.pg_namespace n ON n.oid = c.relnamespace
        WHERE  a.attnum > 0
        AND NOT a.attisdropped
        AND c.relname = %(table)s and n.nspname = %(schema)s
        ORDER BY a.attnum
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
