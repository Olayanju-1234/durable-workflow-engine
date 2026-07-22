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
}

func newEngineMetrics(reg *metrics.Registry, queueDepth func() float64) *engineMetrics {
	reg.GaugeFunc("workflowd_task_queue_depth",
		"Tasks buffered in the queue awaiting a worker.", queueDepth)
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
	}
}
