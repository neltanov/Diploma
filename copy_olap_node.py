import os
import subprocess

OLAP_PORT = 5502
OLAP_COPY_PORT = 5503
PGDATA_OLAP = "olap_data"
PGDATA_OLAP_COPY = "olap_copy_data"
REPL_USER = "replica_user"
REPL_PASSWORD = "replica_pass"

def run_command(command):
    result = subprocess.run(command, shell=True, text=True, capture_output=True)
    if result.returncode != 0:
        raise Exception(f"Ошибка выполнения команды {command}: {result.stderr}")
    return result.stdout


def stop_olap_node():
    run_command(f"pg_ctl -D {PGDATA_OLAP} stop")


def copy_olap_node():
    os.environ["PGPASSWORD"] = REPL_PASSWORD
    run_command(f"pg_basebackup -h localhost -p {OLAP_PORT} -D {PGDATA_OLAP_COPY} -P -v")


def configure_olap_copy():
    run_command(f"rm -rf {PGDATA_OLAP_COPY}/standby.signal")
    with open(f"{PGDATA_OLAP_COPY}/postgresql.conf", "a") as conf:
        conf.write(f"\nport = {OLAP_COPY_PORT}")


def run_olap_copy():
    run_command(f"pg_ctl -D {PGDATA_OLAP_COPY} -o '-p {OLAP_COPY_PORT}' -l logfile_olap_copy start")


def main():
    try:
        copy_olap_node()
        configure_olap_copy()
        run_olap_copy()
    except Exception as e:
        print(f"Error: {e}")


if __name__ == "__main__":
    main()
