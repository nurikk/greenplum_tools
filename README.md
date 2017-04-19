# greenplum tools


`gp_repack.py` - make advanced CTAS instruction, with copying indexes from source table


`gp_cw.py` - greenplum compression wizard. This script benchmark different column encoding type and (hopefully) choose best column encoding. Generate useful debug info and sql statement, needed to migrate data.
example:
```bash
python3 compress_wizard.py -s faa -t d_airlines --user=gpadmin --host=localhost --database=gpadmin --port=5432 --lines=10000
```


`maintanance/maintain.py` - multithreaded python port of https://www.pivotalguru.com/?p=80

better combine with bash script
```bash
#!/bin/bash
set -e
err_report() {
    mail -s "subj" your@mail.com <<< "Some error accured"
}

trap 'err_report $LINENO' ERR
source /home/gpadmin/setup.sh
gpstop -a -M fast
gpstart -a -R
python3 maintain.py
gpstop -r -a -M fast
```


```bash
python3 maintain.py --host=172.16.70.128 --user=gpadmin --database=db
```
