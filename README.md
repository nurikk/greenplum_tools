greenplum tools


gp_repack.py - make advanced CTAS instruction, with copying indexes from source table


gp_cw.py - greenplum compression wizard. This script benchmark different column encoding type and (hopefully) choose best column encoding. Generate useful debug info and sql statement, needed to migrate data.
example: `python3 compress_wizard.py -s faa -t d_airlines --user=gpadmin --host=localhost --database=gpadmin --port=5432 --lines=10000` 
