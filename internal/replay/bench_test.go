package replay

import (
	"fmt"
	"testing"

	"google.golang.org/protobuf/proto"

	"github.com/olayanju-1234/durable-workflow-engine/internal/history"
	"github.com/olayanju-1234/durable-workflow-engine/workflow"
)

// BenchmarkReplayDecision is what a worker spends replaying a sequential
// workflow from the top for one decision, by history length: the cost a sticky
// worker-side cache would save.
func BenchmarkReplayDecision(b *testing.B) {
	for _, steps := range []int{10, 100, 1000} {
		evs := []*history.HistoryEvent{started("in")}
		for i := 1; i <= steps; i++ {
			name := fmt.Sprint("s", i)
			evs = append(evs, scheduled(name, int64(i)), completed(name, int64(i), "out"))
		}
		evs = at(evs...)
		fn := func(ctx workflow.Context, in []byte) ([]byte, error) {
			for i := 1; i <= steps+1; i++ {
				if _, err := ctx.ExecuteActivity(fmt.Sprint("s", i), in); err != nil {
					return nil, err
				}
			}
			return nil, nil
		}
		size := 0
		for _, ev := range evs {
			size += proto.Size(ev)
		}
		b.Run(fmt.Sprintf("steps=%d", steps), func(b *testing.B) {
			for range b.N {
				if _, err := DecideAll(fn, evs, nil); err != nil {
					b.Fatal(err)
				}
			}
			b.ReportMetric(float64(size), "history-bytes")
		})
	}
}
