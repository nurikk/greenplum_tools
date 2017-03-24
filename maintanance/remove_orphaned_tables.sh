#!/bin/bash
set -e
cd /tmp
cat /dev/null > execute_drop_on_all.sh | psql -d template1 -Atc "select datname from pg_database where datname != 'template0'" | while read a; do echo "Checking database ${a}"; psql -Atc "select 'drop schema if exists ' || nspname || ' cascade;' from (select nspname from pg_namespace where nspname like 'pg_temp%' union select nspname from gp_dist_random('pg_namespace') where nspname like 'pg_temp%' except select 'pg_temp_' || sess_id::varchar from pg_stat_activity) as foo" ${a} > drop_temp_schema_$a.ddl ; echo "psql -f drop_temp_schema_$a.ddl -d ${a}" >> execute_drop_on_all.sh ; done
chmod +x execute_drop_on_all.sh
./execute_drop_on_all.sh
