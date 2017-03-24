#!/bin/bash
set -e
err_report() {
    mail -s "subj" at@aviasales.ru <<< "при тех обслужвании случилась хуйня"
}

trap 'err_report $LINENO' ERR
# export PGOPTIONS='-c gp_session_role=utility'
source /home/gpadmin/setup.sh
gpstop -a -M fast
gpstart -a -R
./remove_orphaned_tables.sh
./maintanance.sh
gpstop -r -a -M fast
