#!/bin/bash

PRIMARY_PATH="primary_data"
REPLICA_PATH="replica_data"
OLAP_PATH="olap_data"

pg_ctl -D ${PRIMARY_PATH} stop
pg_ctl -D ${REPLICA_PATH} stop
pg_ctl -D ${OLAP_PATH} stop
