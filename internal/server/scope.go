package server

import (
	"context"
	"fmt"

	"github.com/olayanju-1234/durable-workflow-engine/internal/history"
)

// Scope is the engine as one namespace sees it, and the only way the API
// layers reach the engine. Every id that comes in is a client's id within the
// namespace, checked to be one (it can never name another namespace's
// workflow), and every workflow that goes out is named the same way. The
// Engine's own methods are the default namespace's.
type Scope struct {
	e  *Engine
	ns string
}

// In returns the engine scoped to a namespace ("" is the default). The
// namespace needs no configuration; who may use it is the API's decision.
func (e *Engine) In(namespace string) *Scope {
	if namespace == "" {
		namespace = DefaultNamespace
	}
	return &Scope{e: e, ns: namespace}
}

// Namespace is the scope's namespace.
func (s *Scope) Namespace() string { return s.ns }

// key turns a client's workflow id into a store key, or says why it can't be one.
func (s *Scope) key(id string) (string, error) {
	if err := checkNamespace(s.ns); err != nil {
		return "", err
	}
	if err := validateLocalID(id); err != nil {
		return "", err
	}
	return history.Key(s.ns, id), nil
}

// lookupKey is key for calls that look a workflow up: a malformed id names
// no workflow, so it is not found rather than invalid.
func (s *Scope) lookupKey(id string) (string, error) {
	k, err := s.key(id)
	if err != nil {
		return "", fmt.Errorf("%w: %q", ErrNotFound, id)
	}
	return k, nil
}

func (s *Scope) local(sum *WorkflowSummary) *WorkflowSummary {
	if sum != nil {
		sum.WorkflowId = localID(sum.WorkflowId)
	}
	return sum
}

// Start starts a workflow in the namespace; see Engine.Start.
func (s *Scope) Start(workflowType string, input []byte, workflowID string, opts ...StartOption) (Started, error) {
	return s.e.Start(workflowType, input, workflowID, append(opts, inNamespace(s.ns))...)
}

// ListWorkflows lists the namespace's workflows; see Engine.ListWorkflows.
func (s *Scope) ListWorkflows(filter WorkflowStatus) ([]*WorkflowSummary, error) {
	if err := checkNamespace(s.ns); err != nil {
		return nil, err
	}
	sums, err := s.e.listWorkflows(s.ns, filter)
	for _, sum := range sums {
		s.local(sum)
	}
	return sums, err
}

// GetWorkflowRun describes a workflow run; see Engine.GetWorkflowRun.
func (s *Scope) GetWorkflowRun(workflowID, runID string) (*GetWorkflowResponse, error) {
	k, err := s.lookupKey(workflowID)
	if err != nil {
		return nil, err
	}
	resp, err := s.e.GetWorkflowRun(k, runID)
	if resp != nil {
		s.local(resp.Summary)
	}
	return resp, err
}

// ListWorkflowRuns lists a workflow id's runs; see Engine.ListWorkflowRuns.
func (s *Scope) ListWorkflowRuns(workflowID string) ([]*RunInfo, error) {
	k, err := s.lookupKey(workflowID)
	if err != nil {
		return nil, err
	}
	return s.e.ListWorkflowRuns(k)
}

// TerminateWorkflow ends a workflow; see Engine.TerminateWorkflow.
func (s *Scope) TerminateWorkflow(workflowID, reason string) error {
	k, err := s.lookupKey(workflowID)
	if err != nil {
		return err
	}
	return s.e.TerminateWorkflow(k, reason)
}

// CancelWorkflow cancels a workflow; see Engine.CancelWorkflow.
func (s *Scope) CancelWorkflow(workflowID, reason string) error {
	k, err := s.lookupKey(workflowID)
	if err != nil {
		return err
	}
	return s.e.CancelWorkflow(k, reason)
}

// RequestCancelWorkflow cancels a workflow gracefully; see
// Engine.RequestCancelWorkflow.
func (s *Scope) RequestCancelWorkflow(workflowID, reason string) error {
	k, err := s.lookupKey(workflowID)
	if err != nil {
		return err
	}
	return s.e.RequestCancelWorkflow(k, reason)
}

// SignalWorkflowOnce signals a workflow; see Engine.SignalWorkflowOnce.
func (s *Scope) SignalWorkflowOnce(workflowID, signalName, requestID string, payload []byte) error {
	k, err := s.key(workflowID)
	if err != nil {
		return err
	}
	return s.e.SignalWorkflowOnce(k, signalName, requestID, payload)
}

// RegisterWorkflowTypes registers SDK-run workflow types in the namespace;
// see Engine.RegisterWorkflowTypes.
func (s *Scope) RegisterWorkflowTypes(workflowTypes []string, workerID, sdk string) error {
	if err := checkNamespace(s.ns); err != nil {
		return err
	}
	return s.e.registerWorkflowTypes(s.ns, workflowTypes, workerID, sdk)
}

// RegisterWorkflow makes an in-process planner runnable in the namespace.
func (s *Scope) RegisterWorkflow(workflowType string, planner Planner) {
	s.e.registerWorkflow(s.ns, workflowType, planner)
}

// PollWorkflowTask waits for a decision on one of the namespace's task
// queues ("" is the default); see Engine.PollWorkflowTask. The engine hands
// the task over: nothing else holds it, so it is renamed in place.
func (s *Scope) PollWorkflowTask(ctx context.Context, workerID, taskQueue string, workflowTypes []string) (*WorkflowTask, error) {
	if err := checkNamespace(s.ns); err != nil {
		return nil, err
	}
	if err := checkQueue(taskQueue); err != nil {
		return nil, err
	}
	t, err := s.e.pollWorkflowTask(ctx, s.ns, taskQueue, workerID, workflowTypes)
	if t != nil {
		t.WorkflowId = localID(t.WorkflowId)
	}
	return t, err
}

// CompleteWorkflowTask applies an SDK's commands; see
// Engine.CompleteWorkflowTask.
func (s *Scope) CompleteWorkflowTask(workflowID, taskToken string, cmds []*WorkflowCommand, replayError string) error {
	k, err := s.key(workflowID)
	if err != nil {
		return err
	}
	return s.e.CompleteWorkflowTask(k, taskToken, cmds, replayError)
}

// PollTask waits for an activity task on one of the namespace's task queues
// ("" is the default); see Engine.PollTask.
func (s *Scope) PollTask(ctx context.Context, workerID, taskQueue string, stepNames ...string) (*Task, error) {
	if err := checkNamespace(s.ns); err != nil {
		return nil, err
	}
	if err := checkQueue(taskQueue); err != nil {
		return nil, err
	}
	t, err := s.e.pollTask(ctx, s.ns, taskQueue, workerID, stepNames)
	if t != nil {
		t.WorkflowId = localID(t.WorkflowId)
	}
	return t, err
}

// CompleteTask records an activity's result; see Engine.CompleteTask.
func (s *Scope) CompleteTask(workflowID, stepName string, sequence int64, leaseToken string, output []byte) error {
	k, err := s.key(workflowID)
	if err != nil {
		return err
	}
	return s.e.completeTask(k, stepName, sequence, leaseToken, output, nil)
}

// FailTask records a failed activity attempt; see Engine.FailTask.
func (s *Scope) FailTask(workflowID, stepName string, sequence int64, leaseToken string, f TaskFailure) error {
	k, err := s.key(workflowID)
	if err != nil {
		return err
	}
	return s.e.FailTask(k, stepName, sequence, leaseToken, f)
}

// Heartbeat extends an activity's lease; see Engine.Heartbeat.
func (s *Scope) Heartbeat(workflowID, leaseToken string) error {
	k, err := s.key(workflowID)
	if err != nil {
		return err
	}
	return s.e.Heartbeat(k, leaseToken)
}

// CreateSchedule creates a schedule in the namespace; see Engine.CreateSchedule.
func (s *Scope) CreateSchedule(spec *Schedule) error {
	if err := checkNamespace(s.ns); err != nil {
		return err
	}
	return s.e.createSchedule(s.ns, spec)
}

// DeleteSchedule deletes one of the namespace's schedules.
func (s *Scope) DeleteSchedule(scheduleID string) error {
	if checkNamespace(s.ns) != nil || validateLocalID(scheduleID) != nil {
		return fmt.Errorf("%w: schedule %q", ErrNotFound, scheduleID)
	}
	return s.e.deleteSchedule(s.ns, scheduleID)
}

// ListSchedules lists the namespace's schedules.
func (s *Scope) ListSchedules() ([]*ScheduleInfo, error) {
	if err := checkNamespace(s.ns); err != nil {
		return nil, err
	}
	return s.e.listSchedules(s.ns)
}
