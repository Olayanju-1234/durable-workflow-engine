package server

import "github.com/olayanju-1234/durable-workflow-engine/internal/metrics"

// engineMetrics bundles the engine's Prometheus instruments. Counters are
// incremented at the exact point the corresponding event is durably recorded
// (or, for dispatch/retry/lease facts, where the engine acts), so the numbers
// on /metrics agree with what a replay of the histories would say.
type engineMetrics struct {
	workflowsStarted  *metrics.Counter
	workflowsFinished *metrics.CounterVec // status: completed | failed | terminated
	workflowsActive   *metrics.Gauge      // started or recovered, not yet terminal
	tasksDispatched   *metrics.Counter
	taskResults       *metrics.CounterVec // result: completed | failed (worker-reported)
	leasesExpired     *metrics.Counter
	retriesScheduled  *metrics.Counter
	signalsReceived   *metrics.Counter
	timersFired       *metrics.Counter
	stepDuration      *metrics.Histogram // dispatch (lease grant) → worker result

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
