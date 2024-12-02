#!/bin/bash

PRIMARY_PATH="primary_data"
REPLICA_PATH="replica_data"
OLAP_PATH="olap_data"

pg_ctl -D ${PRIMARY_PATH} -l logfile_primary start
pg_ctl -D ${REPLICA_PATH} -l logfile_replica start
pg_ctl -D ${OLAP_PATH} -l logfile_olap start
