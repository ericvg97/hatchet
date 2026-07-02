package client

import (
	"sync"
	"sync/atomic"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	dispatchercontracts "github.com/hatchet-dev/hatchet/internal/services/dispatcher/contracts"
	contracts "github.com/hatchet-dev/hatchet/internal/services/shared/proto/v1"
)

// TestDurableEventsListenerStaleRollbackDoesNotDropNewHandler exercises the
// tombstone protocol: a rollback closure captured against a tombstoned bucket
// must not remove handlers from the successor bucket stored under the same
// tuple.
func TestDurableEventsListenerStaleRollbackDoesNotDropNewHandler(t *testing.T) {
	listener := &DurableEventsListener{}
	tuple := listenTuple{taskId: "task-1", signalKey: "signal-1"}

	firstCalls := atomic.Int32{}
	secondCalls := atomic.Int32{}

	rollbackFirst := listener.storeDurableEventHandler(tuple, func(e DurableEvent) error {
		firstCalls.Add(1)
		return nil
	})

	rollbackFirst()
	assert.False(t, listener.hasHandlers())

	rollbackSecond := listener.storeDurableEventHandler(tuple, func(e DurableEvent) error {
		secondCalls.Add(1)
		return nil
	})

	// Stale rollback references the tombstoned bucket and must be a no-op.
	rollbackFirst()
	assert.True(t, listener.hasHandlers())

	require.NoError(t, listener.handleEvent(&contracts.DurableEvent{
		TaskId:    tuple.taskId,
		SignalKey: tuple.signalKey,
	}))

	assert.Equal(t, int32(0), firstCalls.Load())
	assert.Equal(t, int32(1), secondCalls.Load())
	assert.False(t, listener.hasHandlers())

	rollbackSecond()
	assert.False(t, listener.hasHandlers())
}

// TestDurableEventsListenerConcurrentStoreAndHandleDoesNotOrphanHandlers
// races handleEvent (which tombstones emptied buckets) against
// storeDurableEventHandler (which must retry on tombstoned buckets). A handler
// stored concurrently with an event must remain reachable: it either fires
// during the concurrent handleEvent or on the follow-up one.
func TestDurableEventsListenerConcurrentStoreAndHandleDoesNotOrphanHandlers(t *testing.T) {
	listener := &DurableEventsListener{}
	tuple := listenTuple{taskId: "task-1", signalKey: "signal-1"}
	event := &contracts.DurableEvent{
		TaskId:    tuple.taskId,
		SignalKey: tuple.signalKey,
	}

	for i := 0; i < 500; i++ {
		fired := atomic.Bool{}

		var wg sync.WaitGroup
		wg.Add(2)

		go func() {
			defer wg.Done()
			assert.NoError(t, listener.handleEvent(event))
		}()
		go func() {
			defer wg.Done()
			listener.storeDurableEventHandler(tuple, func(e DurableEvent) error {
				fired.Store(true)
				return nil
			})
		}()

		wg.Wait()

		if !fired.Load() {
			require.NoError(t, listener.handleEvent(event))
			require.True(t, fired.Load(), "handler stored concurrently with handleEvent was orphaned")
		}
	}

	assert.False(t, listener.hasHandlers())
}

// TestWorkflowRunsListenerStaleRollbackDoesNotDropNewHandler mirrors the
// durable stale-rollback test for the workflow run listener: a rollback
// closure for a tombstoned bucket must not touch the successor bucket, even
// across distinct session ids.
func TestWorkflowRunsListenerStaleRollbackDoesNotDropNewHandler(t *testing.T) {
	listener := &WorkflowRunsListener{}
	workflowRunID := "run-1"

	firstCalls := atomic.Int32{}
	secondCalls := atomic.Int32{}

	rollbackFirst := listener.storeWorkflowRunHandler(workflowRunID, "session-1", func(event WorkflowRunEvent) error {
		firstCalls.Add(1)
		return nil
	})

	rollbackFirst()
	assert.False(t, listener.hasHandlers())

	rollbackSecond := listener.storeWorkflowRunHandler(workflowRunID, "session-2", func(event WorkflowRunEvent) error {
		secondCalls.Add(1)
		return nil
	})

	rollbackFirst()
	assert.True(t, listener.hasHandlers())

	require.NoError(t, listener.handleWorkflowRun(&dispatchercontracts.WorkflowRunEvent{
		WorkflowRunId: workflowRunID,
	}))

	assert.Equal(t, int32(0), firstCalls.Load())
	assert.Equal(t, int32(1), secondCalls.Load())

	rollbackSecond()
	assert.False(t, listener.hasHandlers())
}

// TestWorkflowRunsListenerStaleRollbackSameSessionDoesNotDropNewHandler covers
// the ABA variant: the same session id re-registered after a stale rollback
// must survive another invocation of that stale rollback.
func TestWorkflowRunsListenerStaleRollbackSameSessionDoesNotDropNewHandler(t *testing.T) {
	listener := &WorkflowRunsListener{}
	workflowRunID := "run-1"
	sessionID := "session-1"

	firstCalls := atomic.Int32{}
	secondCalls := atomic.Int32{}

	rollbackFirst := listener.storeWorkflowRunHandler(workflowRunID, sessionID, func(event WorkflowRunEvent) error {
		firstCalls.Add(1)
		return nil
	})

	rollbackFirst()

	rollbackSecond := listener.storeWorkflowRunHandler(workflowRunID, sessionID, func(event WorkflowRunEvent) error {
		secondCalls.Add(1)
		return nil
	})

	rollbackFirst()
	assert.True(t, listener.hasHandlers())

	require.NoError(t, listener.handleWorkflowRun(&dispatchercontracts.WorkflowRunEvent{
		WorkflowRunId: workflowRunID,
	}))

	assert.Equal(t, int32(0), firstCalls.Load())
	assert.Equal(t, int32(1), secondCalls.Load())

	rollbackSecond()
	assert.False(t, listener.hasHandlers())
}
