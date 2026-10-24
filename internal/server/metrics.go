package server

import "github.com/olayanju-1234/durable-workflow-engine/internal/metrics"

// engineMetrics bundles the engine's Prometheus instruments. Counters are
// incremented at the exact point the corresponding event is durably recorded
// (or, for dispatch/retry/lease facts, where the engine acts), so the numbers
// on /metrics agree with what a replay of the histories would say.
type engineMetrics struct {
	workflowsStarted    *metrics.Counter
	workflowsFinished   *metrics.CounterVec // status: completed | failed | terminated | timed_out | cancelled
	workflowsActive     *metrics.Gauge      // started or recovered, not yet terminal
	tasksDispatched     *metrics.Counter
	taskResults         *metrics.CounterVec // result: completed | failed (worker-reported)
	leasesExpired       *metrics.Counter
	retriesScheduled    *metrics.Counter
	signalsReceived     *metrics.Counter
	timersFired         *metrics.Counter
	stepDuration        *metrics.Histogram  // dispatch (lease grant) → worker result
	workflowsPurged     *metrics.Counter    // finished histories deleted by retention
	schedulesFired      *metrics.Counter    // workflows started by a schedule
	signalsDeduplicated *metrics.Counter    // retried signals recognised by their request id
	schedulesSkipped    *metrics.Counter    // fires skipped because a run was still going
	activityTimeouts    *metrics.CounterVec // timeout: start_to_close | schedule_to_close
	stepsCancelled      *metrics.Counter    // open steps workflow code cancelled
	historyLimit        *metrics.CounterVec // kind: warned | failed
	cancelRequests      *metrics.Counter    // graceful cancels requested
	startsRefused       *metrics.CounterVec // reason: max_active_workflows | max_starts_per_minute
	runsArchived        *metrics.Counter    // finished runs exported before retention deleted them
	archiveFailures     *metrics.Counter    // exports that failed (the run is kept)

	// Polyglot: workflow decisions made by external SDK workers.
	workflowTasksDispatched  *metrics.Counter
	workflowTasksCompleted   *metrics.Counter
	workflowTaskTimeouts     *metrics.Counter
	workflowTaskReplayErrors *metrics.Counter
}

func newEngineMetrics(reg *metrics.Registry, queueDepth, workflowQueueDepth func() float64) *engineMetrics {
	reg.GaugeFunc("workflowd_task_queue_depth",
		"Activity tasks buffered in the queue awaiting a worker.", queueDepth)
	reg.GaugeFunc("workflowd_workflow_task_queue_depth",
		"Workflow decisions awaiting an external SDK worker.", workflowQueueDepth)
	return &engineMetrics{
		workflowsStarted: reg.Counter("workflowd_workflows_started_total",
			"Workflow executions started."),
		workflowsFinished: reg.CounterVec("workflowd_workflows_finished_total",
			"Workflow executions reaching a terminal state, by status.", "status"),
		workflowsActive: reg.Gauge("workflowd_workflows_active",
			"Workflows currently running or waiting (not terminal)."),
		tasksDispatched: reg.Counter("workflowd_tasks_dispatched_total",
			"Tasks handed to workers (lease grants, including retries)."),
		taskResults: reg.CounterVec("workflowd_task_results_total",
			"Worker-reported task results, by outcome.", "result"),
		leasesExpired: reg.Counter("workflowd_leases_expired_total",
			"Task leases reclaimed because the worker went dark."),
		retriesScheduled: reg.Counter("workflowd_retries_scheduled_total",
			"Activity retries armed after a failed attempt."),
		signalsReceived: reg.Counter("workflowd_signals_received_total",
			"External signals durably recorded."),
		timersFired: reg.Counter("workflowd_timers_fired_total",
			"Durable timers that reached their deadline."),
		stepDuration: reg.Histogram("workflowd_step_duration_seconds",
			"Seconds from task dispatch to the worker's result.", nil),
		workflowsPurged: reg.Counter("workflowd_workflows_purged_total",
			"Finished workflow histories deleted by the retention policy."),
		signalsDeduplicated: reg.Counter("workflowd_signals_deduplicated_total",
			"Signals sent again with a request id already recorded, and not recorded twice."),
		schedulesFired: reg.Counter("workflowd_schedule_runs_started_total",
			"Workflows started by a cron schedule."),
		schedulesSkipped: reg.Counter("workflowd_schedule_runs_skipped_total",
			"Schedule fires skipped because an earlier run was still running."),
		stepsCancelled: reg.Counter("workflowd_steps_cancelled_total",
			"Open steps (activities, timers, signal waits) cancelled by workflow code."),
		cancelRequests: reg.Counter("workflowd_cancel_requests_total",
			"Graceful cancels requested: the workflow code was told, and may compensate before it ends."),
		startsRefused: reg.CounterVec("workflowd_starts_refused_total",
			"Workflow starts refused by a namespace quota, by quota.", "reason"),
		runsArchived: reg.Counter("workflowd_runs_archived_total",
			"Finished runs exported to the archive before retention deleted them."),
		archiveFailures: reg.Counter("workflowd_archive_failures_total",
			"Archive exports that failed; the run stays on disk and is retried at the next retention pass."),
		historyLimit: reg.CounterVec("workflowd_history_limit_total",
			"Histories that crossed the warning share of their event or size limit (warned) or reached it and failed their workflow (failed).", "kind"),
		activityTimeouts: reg.CounterVec("workflowd_activity_timeouts_total",
			"Activity attempts (start_to_close) and steps (schedule_to_close) that ran out of time.", "timeout"),
		workflowTasksDispatched: reg.Counter("workflowd_workflow_tasks_dispatched_total",
			"Workflow decisions handed to external SDK workers."),
		workflowTasksCompleted: reg.Counter("workflowd_workflow_tasks_completed_total",
			"Workflow commands returned by SDK workers and applied."),
		workflowTaskTimeouts: reg.Counter("workflowd_workflow_task_timeouts_total",
			"Workflow tasks re-dispatched because an SDK worker went dark."),
		workflowTaskReplayErrors: reg.Counter("workflowd_workflow_task_replay_errors_total",
			"Workflows failed because an SDK could not replay their history."),
	}
}
