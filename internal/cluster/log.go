package cluster

import (
	"context"
	"fmt"

	"google.golang.org/protobuf/proto"

	"github.com/olayanju-1234/durable-workflow-engine/internal/history"
	"github.com/olayanju-1234/durable-workflow-engine/internal/raft"
	"github.com/olayanju-1234/durable-workflow-engine/internal/server"
)

// replicatedLog is the server.HistoryLog of one leadership term: each write is
// proposed to Raft and returns once this node has applied it, so the engine's
// next read of the history store sees it. Proposals carry the term, so a log
// left over from an earlier term can never write.
type replicatedLog struct {
	node *raft.Node
	term uint64
}

func (l *replicatedLog) Append(workflowID string, ev *history.HistoryEvent) error {
	return l.propose(&Command{Command: &Command_Append{Append: &AppendEvent{WorkflowId: workflowID, Event: ev}}})
}

func (l *replicatedLog) Delete(workflowID string) error {
	return l.propose(&Command{Command: &Command_Delete{Delete: &DeleteWorkflow{WorkflowId: workflowID}}})
}

func (l *replicatedLog) NewRun(workflowID, archiveID string, first *history.HistoryEvent) error {
	return l.propose(&Command{Command: &Command_NewRun{NewRun: &NewRun{WorkflowId: workflowID, ArchiveId: archiveID, Event: first}}})
}

func (l *replicatedLog) propose(cmd *Command) error {
	data, err := proto.Marshal(cmd)
	if err != nil {
		return fmt.Errorf("cluster: encode command: %w", err)
	}
	// No deadline: while this node leads the term the entry either commits or
	// leadership ends (CheckQuorum guarantees one of them), and giving up
	// earlier would leave the outcome unknown to an engine that is still live.
	res, err := l.node.Propose(context.Background(), l.term, data)
	if err != nil {
		// Every Raft error here means leadership changed: the engine is about
		// to be discarded, and the client should retry on the new leader.
		return fmt.Errorf("%w: %v", server.ErrNotLeader, err)
	}
	if rejected, ok := res.(error); ok {
		return rejected
	}
	return nil
}
