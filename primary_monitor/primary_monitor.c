#include "postgres.h"
#include "fmgr.h"
#include "miscadmin.h"
#include "storage/ipc.h"
#include "storage/proc.h"
#include "storage/latch.h"
#include "postmaster/bgworker.h"
#include "utils/elog.h"
#include "utils/guc.h"
#include "executor/spi.h"
#include "libpq-fe.h"

PG_MODULE_MAGIC;

void _PG_init(void);
void MonitorPrimaryWorker(Datum main_arg);

/* Global variable for shutdown flag */
static volatile sig_atomic_t got_sigterm = false;

/* Address of replica node (hardcoded for now) */
static const char *replica_conninfo = "host=localhost port=5501 user=replica_user password=replica_pass";

/* Signal handler */
static void
handle_sigterm(SIGNAL_ARGS)
{
    int save_errno = errno;
    got_sigterm = true;
    SetLatch(MyLatch);
    errno = save_errno;
}

/* Function to parse and fetch primary_conninfo */
static char *
GetPrimaryConninfo(void)
{
    char *primary_conninfo;

    DefineCustomStringVariable("primary_conninfo",
                               "Connection string for the primary node",
                               NULL,
                               &primary_conninfo,
                               NULL,
                               PGC_SIGHUP,
                               0,
                               NULL,
                               NULL,
                               NULL);
    return primary_conninfo;
}

/* Function to check the primary node status */
static bool
CheckPrimaryStatus(const char *primary_conninfo)
{
    PGresult *res;
    bool is_primary_alive;
    PGconn *conn = PQconnectdb(primary_conninfo);

    if (PQstatus(conn) != CONNECTION_OK)
    {
        PQfinish(conn);
        return false;
    }

    res = PQexec(conn, "SELECT 1");
    is_primary_alive = (PQresultStatus(res) == PGRES_TUPLES_OK);

    PQclear(res);
    PQfinish(conn);

    return is_primary_alive;
}

/* Function to perform failover */
static void
PerformFailover(void)
{
    PGresult *res;
    PGconn *conn = PQconnectdb(replica_conninfo);

    if (PQstatus(conn) != CONNECTION_OK)
    {
        elog(WARNING, "Failover: Could not connect to replica node");
        PQfinish(conn);
        return;
    }

    res = PQexec(conn, "SELECT pg_promote()");

    if (PQresultStatus(res) != PGRES_COMMAND_OK)
        elog(WARNING, "Failover: Could not promote replica to primary");
    else
        elog(LOG, "Failover: Replica promoted to primary");

    PQclear(res);
    PQfinish(conn);
}

/* Background worker main function */
void
MonitorPrimaryWorker(Datum main_arg)
{
    pqsignal(SIGTERM, handle_sigterm);
    BackgroundWorkerUnblockSignals();

    elog(LOG, "Failover extension: Background worker started");

    while (!got_sigterm)
    {
        char *primary_conninfo = GetPrimaryConninfo();

        if (!primary_conninfo || !CheckPrimaryStatus(primary_conninfo))
        {
            elog(WARNING, "Primary node is down, initiating failover");
            PerformFailover();
        }

        /* Sleep for 1 second */
        WaitLatch(MyLatch, WL_LATCH_SET | WL_TIMEOUT | WL_POSTMASTER_DEATH, 1000L, 0);
        ResetLatch(MyLatch);
    }

    elog(LOG, "Failover extension: Background worker shutting down");
}


void
_PG_init(void)
{
    BackgroundWorker worker;

    memset(&worker, 0, sizeof(worker));
    sprintf(worker.bgw_name, "Primary Monitor Worker");
    worker.bgw_flags = BGWORKER_SHMEM_ACCESS | BGWORKER_BACKEND_DATABASE_CONNECTION;
    
    worker.bgw_start_time = BgWorkerStart_ConsistentState;
    
    worker.bgw_restart_time = 1;

    sprintf(worker.bgw_library_name, "primary_monitor");
    sprintf(worker.bgw_function_name,
			 "MonitorPrimaryWorker");
    
    worker.bgw_main_arg = (Datum) 0;

    RegisterBackgroundWorker(&worker);
}
