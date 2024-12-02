#!/usr/bin/env python3

import os
import subprocess
import time
import socket
import configparser


config = configparser.ConfigParser()
config.read('settings.ini')

PRIMARY_PORT = int(config['DEFAULT']['PRIMARY_PORT'])
REPLICA_PORT = int(config['DEFAULT']['REPLICA_PORT'])
OLAP_PORT = int(config['DEFAULT']['OLAP_PORT'])
PGDATA_PRIMARY = config['DEFAULT']['PGDATA_PRIMARY']
PGDATA_REPLICA = config['DEFAULT']['PGDATA_REPLICA']
PGDATA_OLAP = config['DEFAULT']['PGDATA_OLAP']
REPL_USER = config['DEFAULT']['REPL_USER']
REPL_PASSWORD = config['DEFAULT']['REPL_PASSWORD']


def run_command(command):
    result = subprocess.run(command, shell=True, text=True, capture_output=True)
    if result.returncode != 0:
        raise Exception(f"Command execution error {command}: {result.stderr}")
    return result.stdout

def is_server_running(host="localhost", port=5432):
    try:
        with socket.create_connection((host, port), timeout=2):
            return True
    except OSError:
        return False

def setup_primary():
    print("=== Configuring primary node ===")

    # Инициализация и настройка основного узла, если он еще не инициализирован
    os.makedirs(PGDATA_PRIMARY, exist_ok=True)
    if len(os.listdir(PGDATA_PRIMARY)) == 0:
        run_command(f"pg_ctl -D {PGDATA_PRIMARY} initdb")

        # Конфигурируем сервер для дальнейшей репликации
        with open(f"{PGDATA_PRIMARY}/postgresql.conf", "a") as conf:
            conf.write("\nlisten_addresses = 'localhost'")
            conf.write(f"\nport = {PRIMARY_PORT}")
            conf.write("\nmax_wal_senders = 10")
            conf.write("\nwal_keep_size = '64MB'")  # оставляем для совместимости, если нужно больше WAL логов

        # Настраиваем доступ для пользователя репликации
        with open(f"{PGDATA_PRIMARY}/pg_hba.conf", "a") as hba:
            hba.write(f"\nhost replication {REPL_USER} 127.0.0.1/32 md5")

    # Запускаем основной узел, если он еще не запущен
    if not is_server_running("localhost", PRIMARY_PORT):
        run_command(f"pg_ctl -D {PGDATA_PRIMARY} -o '-p {PRIMARY_PORT}' -l logfile_primary start")
        time.sleep(2)

    # Создаем пользователя для репликации, если его еще не существует
    if run_command(f'psql -d postgres -p {PRIMARY_PORT} -tAc "SELECT 1 FROM pg_roles WHERE rolname = \'{REPL_USER}\'"') != '1\n':
        run_command(
            f"psql -p {PRIMARY_PORT} -d postgres -c \"CREATE USER {REPL_USER} REPLICATION LOGIN ENCRYPTED PASSWORD '{REPL_PASSWORD}';\"")
    print("The primary node is configured and running.")


def setup_replica(replica_port, replica_data_path):
    print(f"=== Configuring replica node: {replica_data_path} ===")

    # Останавливаем реплику, если она уже запущена
    try:
        run_command(f"pg_ctl -D {replica_data_path} stop")
    except Exception:
        pass

    # Удаляем старые данные и создаем реплику основного узла с помощью pg_basebackup
    if os.path.exists(replica_data_path):
        run_command(f"rm -rf {replica_data_path}")

    os.environ["PGPASSWORD"] = REPL_PASSWORD
    run_command(f"pg_basebackup -h localhost -p {PRIMARY_PORT} -D {replica_data_path} -U {REPL_USER} -Fp -Xs -P -R")

    # Изменяем порт для реплики и запускаем её
    with open(f"{replica_data_path}/postgresql.conf", "a") as conf:
        conf.write(f"\nport = {replica_port}")

    run_command(f"pg_ctl -D {replica_data_path} -o '-p {replica_port}' -l logfile_replica_{replica_port} start")
    print("Replica node is configured and running.")


def main():
    try:
        setup_primary()
        setup_replica(REPLICA_PORT, PGDATA_REPLICA)
        setup_replica(OLAP_PORT, PGDATA_OLAP)
        print("=== Replication is configured ===")
    except Exception as e:
        print(f"Cluster configuration error: {e}")


if __name__ == "__main__":
    main()
