/*-------------------------------------------------------------------------
 *
 * bg_executor.h
 *		  Background executor for executing PlannedStmt generated by postgres
 *
 * Portions Copyright (c) 2012-2016, PostgreSQL Global Development Group
 *
 * IDENTIFICATION
 *			contrib/bg_executor/bg_executor.h
 *
 *-------------------------------------------------------------------------
 */
#ifndef BG_EXECUTOR_H
#define BG_EXECUTOR_H

#include "lib/stringinfo.h"
#include "postmaster/bgworker.h"
#include "storage/lwlock.h"

#define MaxQueueSize 64

typedef struct QueueElem
{
	char *ptr;
	int size;
} QueueElem;

typedef struct Queue
{
	int trancheId;
	int32 executorId;
	LWLock lwlock;
	QueueElem elems[MaxQueueSize];
	int size;
	int head;
	int tail;
} Queue;

typedef struct BgExecutorHashEnt
{
	int32 executorId;
	Queue *queue;
} BgExecutorHashEnt;

#endif
