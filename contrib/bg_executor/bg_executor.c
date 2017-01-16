/*-------------------------------------------------------------------------
 *
 * bg_executor.c
 *		  Background executor for registering dynamic background workers.
 *
 * Portions Copyright (c) 2012-2016, PostgreSQL Global Development Group
 *
 * IDENTIFICATION
 *			contrib/bg_executor/bg_executor.c
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

/* These are always necessary for a bgworker */
#include "miscadmin.h"
#include "postmaster/bgworker.h"
#include "storage/ipc.h"
#include "storage/latch.h"
#include "storage/lwlock.h"
#include "storage/proc.h"
#include "storage/shmem.h"

/* these headers are used by this particular worker's code */
#include "pgstat.h"
#include "utils/guc.h"
#include "utils/memutils.h"
#include "utils/resowner.h"

#include <zmq.h>

PG_MODULE_MAGIC;

void		_PG_init(void);
void		bg_executor_main(Datum) pg_attribute_noreturn();

/* flags set by signal handlers */
static volatile sig_atomic_t got_sighup = false;
static volatile sig_atomic_t got_sigterm = false;

/* GUC */
bool UseBgwLauncher = true;
int LauncherPort = 5555;

/* extern variables */
extern MemoryContext TopMemoryContext;
extern ResourceOwner CurrentResourceOwner;

/*
 * Signal handler for SIGTERM
 *		Set a flag to let the main loop to terminate, and set our latch to wake
 *		it up.
 */
static void
bg_executor_sigterm(SIGNAL_ARGS)
{
	int			save_errno = errno;

	got_sigterm = true;
	SetLatch(MyLatch);

	errno = save_errno;
}

/*
 * Signal handler for SIGHUP
 *		Set a flag to tell the main loop to reread the config file, and set
 *		our latch to wake it up.
 */
static void
bg_executor_sighup(SIGNAL_ARGS)
{
	int			save_errno = errno;

	got_sighup = true;
	SetLatch(MyLatch);

	errno = save_errno;
}

void
bg_executor_main(Datum main_arg)
{
	int32 port;
	void *context;
	void *responder;
	char address[NAMEDATALEN];

	port = DatumGetInt32(main_arg);

	/* Establish signal handlers before unblocking signals. */
	pqsignal(SIGHUP, bg_executor_sighup);
	pqsignal(SIGTERM, bg_executor_sigterm);

	/* We're now ready to receive signals */
	BackgroundWorkerUnblockSignals();

	/* Set up a memory context and resource owner. */
	Assert(CurrentResourceOwner == NULL);
	CurrentResourceOwner = ResourceOwnerCreate(NULL, "bgw launcher");
	CurrentMemoryContext = AllocSetContextCreate(TopMemoryContext,
			"bgw launcher",
			ALLOCSET_DEFAULT_SIZES);

	/* Connect to our database */
	BackgroundWorkerInitializeConnection("postgres", NULL);

	/* socket to talk to clients */
	context = zmq_ctx_new();
	Assert(context != NULL);
	responder = zmq_socket(context, ZMQ_REP);
	Assert(responder != NULL);
	sprintf(address, "tcp://*:%d", port);
	if (zmq_bind(responder, address) != 0)
	{
		ereport(ERROR, (errmsg("cann't bind to %s", address)));
	}

	/*
	 * Main loop: do this until the SIGTERM handler tells us to terminate
	 */
	while (!got_sigterm)
	{
		int			rc;
		char *buf;
		BackgroundWorker *worker;
		BackgroundWorkerHandle *handle;
		int size;

		/*
		 * Background workers mustn't call usleep() or any direct equivalent:
		 * instead, they may wait on their process latch, which sleeps as
		 * necessary, but is awakened if postmaster dies.  That way the
		 * background process goes away immediately in an emergency.
		 */
		rc = WaitLatch(MyLatch,
					   WL_LATCH_SET | WL_TIMEOUT | WL_POSTMASTER_DEATH,
					   5 * 1000L,
					   PG_WAIT_EXTENSION);
		ResetLatch(MyLatch);

		/* emergency bailout if postmaster has died */
		if (rc & WL_POSTMASTER_DEATH)
		{
			if (responder)
				zmq_close(responder);
			if (context)
				zmq_ctx_destroy(context);
			proc_exit(1);
		}

		/*
		 * In case of a SIGHUP, just reload the configuration.
		 */
		if (got_sighup)
		{
			got_sighup = false;
			ProcessConfigFile(PGC_SIGHUP);
		}

		size = sizeof(BackgroundWorker);
		buf = palloc0(size);
		Assert(buf != NULL);
		do {
			rc = zmq_recv(responder, buf, size, 0);
		} while (rc == -1 && zmq_errno() == EINTR);
		if (rc == -1)
		{
			ereport(WARNING, (errmsg("zmq_recv fail: %s", zmq_strerror(errno))));
		}
		else
		{
			Assert(rc == size);
			worker = (BackgroundWorker *) buf;
			if (!RegisterDynamicBackgroundWorker(worker, &handle))
			{
				ereport(WARNING, (errmsg("register dynamic bgworker fail")));
				zmq_send(responder, "Fail", 4, 0);
			}
			else
			{
				int size = 16; // sizeof(BackgroundWorkerHandle)
				do {
					rc = zmq_send(responder, handle, size, 0);
				} while (rc == -1 && zmq_errno() == EINTR);
				if (rc != size)
				{
					ereport(WARNING, (errmsg("zmq_send fail: %s", zmq_strerror(errno))));
				}
			}
		}
		if (buf)
		{
			pfree(buf);
			buf = NULL;
		}
		if (handle)
		{
			pfree(handle);
			handle = NULL;
		}
	}

	if (responder)
		zmq_close(responder);
	if (context)
		zmq_ctx_destroy(context);
	proc_exit(1);
}

/*
 * Entrypoint of this module.
 *
 * We register more than one worker process here, to demonstrate how that can
 * be done.
 */
void
_PG_init(void)
{
	BackgroundWorker worker;

	if (!process_shared_preload_libraries_in_progress)
		return;

	DefineCustomBoolVariable(
		"bg_executor.use_bgw_launcher",
		gettext_noop("Use the background worker launcher."),
		gettext_noop("When enabled, background workers of ParallelContext is"
			" registered by a permanent background worker launcher"),
		&UseBgwLauncher,
		UseBgwLauncher,
		PGC_USERSET,
		0,
		NULL, NULL, NULL);

	DefineCustomIntVariable(
		"bg_executor.launcher_port",
		gettext_noop("Port of the background worker launcher listening on."),
		gettext_noop("Port of the background worker launcher listening on."),
		&LauncherPort,
		LauncherPort,
		1,
		65535,
		PGC_USERSET,
		0,
		NULL, NULL, NULL);

	worker.bgw_flags = BGWORKER_SHMEM_ACCESS |
		BGWORKER_BACKEND_DATABASE_CONNECTION;
	worker.bgw_start_time = BgWorkerStart_ConsistentState;
	worker.bgw_restart_time = BGW_DEFAULT_RESTART_INTERVAL;
	worker.bgw_main = bg_executor_main;
	worker.bgw_notify_pid = 0;
	snprintf(worker.bgw_name, BGW_MAXLEN, "bgw launcher");
	worker.bgw_main_arg = Int32GetDatum(LauncherPort);
	RegisterBackgroundWorker(&worker);
}
