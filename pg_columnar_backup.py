#!/usr/bin/env python3

import os
import subprocess
import time
import argparse
import configparser


config = configparser.ConfigParser()
config.read('settings.ini')

OLAP_PORT = int(config['DEFAULT']['OLAP_PORT'])
OLAP_COPY_PORT = int(config['DEFAULT']['OLAP_COPY_PORT'])
PGDATA_OLAP = config['DEFAULT']['PGDATA_OLAP']
PGDATA_OLAP_COPY = config['DEFAULT']['PGDATA_OLAP_COPY']
REPL_USER = config['DEFAULT']['REPL_USER']
REPL_PASSWORD = config['DEFAULT']['REPL_PASSWORD']


def run_command(command):
    result = subprocess.run(command, shell=True, text=True, capture_output=True)
    if result.returncode != 0:
        raise Exception(f"{command} command execution error : {result.stderr}")
    return result.stdout


def stop_olap_node():
    run_command(f"pg_ctl -D {PGDATA_OLAP} stop")


def copy_olap_node():
    if os.path.exists(PGDATA_OLAP_COPY):
        run_command(f"rm -rf {PGDATA_OLAP_COPY}")
    os.environ["PGPASSWORD"] = REPL_PASSWORD
    # тут вместо basebackup нужно остановить olap ноду и скопировать с помощью cp и снова запустить
    
    run_command(f"pg_basebackup -h localhost -p {OLAP_PORT} -D {PGDATA_OLAP_COPY} -P -v")
    print("OLAP copy node copied from OLAP node")


def configure_olap_copy():
    run_command(f"rm -rf {PGDATA_OLAP_COPY}/standby.signal")
    print("standby.singnal removed")
    with open(f"{PGDATA_OLAP_COPY}/postgresql.conf", "a") as conf:
        conf.write(f"\nport = {OLAP_COPY_PORT}")
        print("port specified")
        conf.write(f"\nshared_preload_libraries=citus")
        print("shared_preload_libraries set to citus")


def run_olap_copy():
    run_command(f"pg_ctl -D {PGDATA_OLAP_COPY} -o '-p {OLAP_COPY_PORT}' -l logfile_olap_copy start")
    print(f"{PGDATA_OLAP_COPY} is started")


def stop_olap_copy_if_started():
    if os.path.exists(f"{PGDATA_OLAP_COPY}/postmaster.pid"):
        run_command(f"pg_ctl -D {PGDATA_OLAP_COPY} stop")
        print("OLAP copy node has been stopped")


# подается список из таблиц
def create_columnar_tables(relations):
    run_command(f"psql -d postgres -p {OLAP_COPY_PORT} -c 'create extension citus;'")
    for relation in relations:
        run_command(f"psql -d postgres -p {OLAP_COPY_PORT} -c \"select alter_table_set_access_method('{relation}', 'columnar')\"")


def wait_timeout(timeout):
    try:
        print("Ready for processing analytical queries.")
        time.sleep(timeout)
        print("Next synchronization iteration")
    except KeyboardInterrupt as e:
        exit(1)


def columnar_backup(table_list):
    stop_olap_copy_if_started()
    copy_olap_node()
    configure_olap_copy()
    run_olap_copy()
    create_columnar_tables(table_list)


def main():
    print("Olap node will be configured as soon as possible")
    parser = argparse.ArgumentParser()
    parser.add_argument('table_names', nargs='*', help='Tables to analyze')
    args = parser.parse_args()
    table_list = args.items
    try:
        while True:
            columnar_backup(table_list)
            wait_timeout(30)
    except Exception as e:
        print(f"Error: {e}")


if __name__ == "__main__":
    main()
