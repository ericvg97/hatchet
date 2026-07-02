// Deprecated: This package is part of the legacy v0 workflow definition system.
// Use the new Go SDK at github.com/hatchet-dev/hatchet/sdks/go instead. Migration guide: https://docs.hatchet.run/home/migration-guide-go
package client

import (
	"context"
	"sync"

	grpc_retry "github.com/grpc-ecosystem/go-grpc-middleware/v2/interceptors/retry"
	"github.com/rs/zerolog"
	"golang.org/x/sync/errgroup"

	contracts "github.com/hatchet-dev/hatchet/internal/services/shared/proto/v1"
	"github.com/hatchet-dev/hatchet/pkg/client/retry"
)

type DurableEvent *contracts.DurableEvent

type DurableEventHandler func(e DurableEvent) error

type DurableEventsListener struct {
	base streamListenerBase[contracts.V1Dispatcher_ListenForDurableEventClient]

	constructor func(context.Context) (contracts.V1Dispatcher_ListenForDurableEventClient, error)

	client contracts.V1Dispatcher_ListenForDurableEventClient

	l *zerolog.Logger

	// map of workflow run ids to a list of handlers
	handlers sync.Map
}

type listenTuple struct {
	taskId    string
	signalKey string
}

func (w *DurableEventsListener) streamCore() *reconnectingStream[contracts.V1Dispatcher_ListenForDurableEventClient] {
	return w.base.streamCore(streamCoreConfig[contracts.V1Dispatcher_ListenForDurableEventClient]{
		constructor: w.constructor,
		closeSend: func(client contracts.V1Dispatcher_ListenForDurableEventClient) error {
			return client.CloseSend()
		},
		replay: w.replayHandlers,
		initialClient: func() (contracts.V1Dispatcher_ListenForDurableEventClient, bool) {
			return w.client, w.client != nil
		},
	})
}

func (r *subscribeClientImpl) getDurableEventsListener(
	ctx context.Context,
) (*DurableEventsListener, error) {
	r.durableEventsListenerMu.Lock()
	defer r.durableEventsListenerMu.Unlock()

	constructor := func(ctx context.Context) (contracts.V1Dispatcher_ListenForDurableEventClient, error) {
		return r.clientv1.ListenForDurableEvent(r.ctx.newContext(ctx), grpc_retry.Disable())
	}

	return startStreamListener(ctx, streamListenerStartConfig[DurableEventsListener]{
		existing: r.durableEventsListener,
		build: func() *DurableEventsListener {
			return &DurableEventsListener{
				constructor: constructor,
				l:           r.l,
			}
		},
		connect: func(w *DurableEventsListener, ctx context.Context) error {
			return w.retryListenSync(ctx)
		},
		start: func(w *DurableEventsListener) bool {
			return w.startListening()
		},
		closeStream: func(w *DurableEventsListener) error {
			return w.closeStream()
		},
		stop: func(w *DurableEventsListener) {
			w.stopListening()
		},
		listen: func(w *DurableEventsListener, ctx context.Context) error {
			return w.listen(ctx)
		},
		lifecycleContext: func(w *DurableEventsListener) context.Context {
			return w.lifecycleContext()
		},
		store: func(w *DurableEventsListener) {
			r.durableEventsListener = w
		},
		clear: func(w *DurableEventsListener) {
			r.durableEventsListenerMu.Lock()
			if r.durableEventsListener == w {
				r.durableEventsListener = nil
			}
			r.durableEventsListenerMu.Unlock()
		},
		l:              r.l,
		closeErrorMsg:  "failed to close durable events listener",
		listenErrorMsg: "failed to listen for durable events",
	})
}

func (w *DurableEventsListener) lifecycleContext() context.Context {
	return w.streamCore().lifecycleContext()
}

func (w *DurableEventsListener) retryListenSync(ctx context.Context) error {
	return w.streamCore().retryConnectSync(
		ctx,
		func(err error, attempt int) {
			w.l.Error().Ctx(ctx).Err(err).Msgf("could not resubscribe to the durable event listener (attempt %d/%d)", attempt, retry.StreamSyncMaxAttempts)
		},
		"could not listen for durable events on the worker after %d retries",
	)
}

func (w *DurableEventsListener) retryListenBackground(ctx context.Context) error {
	return w.streamCore().retryConnectBackground(
		ctx,
		func(err error, attempt int) {
			w.l.Error().Ctx(ctx).Err(err).Msgf("could not resubscribe to the durable event listener (background attempt %d)", attempt)
		},
	)
}

func (w *DurableEventsListener) replayHandlers(ctx context.Context, client contracts.V1Dispatcher_ListenForDurableEventClient) error {
	var rangeErr error

	w.handlers.Range(func(key, value interface{}) bool {
		t := key.(listenTuple)

		err := client.Send(&contracts.ListenForDurableEventRequest{
			TaskId:    t.taskId,
			SignalKey: t.signalKey,
		})

		if err != nil {
			w.l.Error().Ctx(ctx).Err(err).Msgf("could not listen for durable events on the worker")
			rangeErr = err
			return false
		}

		return true
	})

	if rangeErr != nil {
		return rangeErr
	}

	return nil
}

func (w *DurableEventsListener) getClientSnapshot() (contracts.V1Dispatcher_ListenForDurableEventClient, uint64) {
	client, generation, _ := w.streamCore().getClientSnapshot()
	return client, generation
}

func (w *DurableEventsListener) isListening() bool {
	return w.base.isListening()
}

func (w *DurableEventsListener) isClosed() bool {
	return w.streamCore().isClosed()
}

func (w *DurableEventsListener) startListening() bool {
	return w.base.startListening(w.isClosed)
}

func (w *DurableEventsListener) stopListening() {
	w.base.stopListening()
}

func (w *DurableEventsListener) ensureListening(ctx context.Context) error {
	return w.base.ensureListening(ctx, streamListenerRunConfig{
		isClosed:         w.isClosed,
		retryConnectSync: w.retryListenSync,
		lifecycleContext: w.lifecycleContext,
		listen:           w.listen,
		l:                w.l,
		listenErrorMsg:   "failed to listen for durable events",
	})
}

type threadSafeDurableEventHandlers struct {
	handlers map[uint64]DurableEventHandler
	nextID   uint64
	deleted  bool
	mu       sync.RWMutex
}

type durableEventHandlerSnapshot struct {
	handler DurableEventHandler
	id      uint64
}

func (l *DurableEventsListener) AddSignal(
	taskId string,
	signalKey string,
	handler DurableEventHandler,
) error {
	if l.isClosed() {
		return errListenerClosed
	}

	t := listenTuple{
		taskId:    taskId,
		signalKey: signalKey,
	}

	if !l.isListening() {
		if err := l.ensureListening(l.lifecycleContext()); err != nil {
			return err
		}
	}

	rollback := l.storeDurableEventHandler(t, handler)

	if err := l.retrySend(t); err != nil {
		if !l.isListening() {
			if listenErr := l.ensureListening(l.lifecycleContext()); listenErr != nil {
				rollback()
				return listenErr
			}

			if retryErr := l.retrySend(t); retryErr != nil {
				rollback()
				return retryErr
			}

			return nil
		}

		rollback()
		return err
	}

	if err := l.ensureListening(l.lifecycleContext()); err != nil {
		rollback()
		return err
	}

	return nil
}

func (l *DurableEventsListener) storeDurableEventHandler(t listenTuple, handler DurableEventHandler) func() {
	for {
		handlers, _ := l.handlers.LoadOrStore(t, &threadSafeDurableEventHandlers{
			handlers: map[uint64]DurableEventHandler{},
		})

		h := handlers.(*threadSafeDurableEventHandlers)

		h.mu.Lock()
		if h.deleted {
			h.mu.Unlock()
			l.handlers.CompareAndDelete(t, h)
			continue
		}

		id := h.nextID
		h.nextID++
		h.handlers[id] = handler
		h.mu.Unlock()

		return func() {
			l.removeDurableEventHandlersFromBucket(t, h, id)
		}
	}
}

func (l *DurableEventsListener) removeDurableEventHandlersFromBucket(
	t listenTuple,
	h *threadSafeDurableEventHandlers,
	ids ...uint64,
) {
	h.mu.Lock()
	defer h.mu.Unlock()

	if h.deleted {
		return
	}

	for _, id := range ids {
		delete(h.handlers, id)
	}

	if len(h.handlers) == 0 {
		h.deleted = true
		l.handlers.CompareAndDelete(t, h)
	}
}

func (l *DurableEventsListener) retrySend(t listenTuple) error {
	return l.streamCore().retrySend(
		l.lifecycleContext(),
		func(client contracts.V1Dispatcher_ListenForDurableEventClient) error {
			return client.Send(&contracts.ListenForDurableEventRequest{
				TaskId:    t.taskId,
				SignalKey: t.signalKey,
			})
		},
		func(err error, attempt int) {
			l.l.Warn().Err(err).Msgf("failed to send durable event subscription, attempt %d/%d", attempt, retry.StreamSyncMaxAttempts)
		},
		func(err error) {
			l.l.Error().Err(err).Msg("failed to relisten after send failure")
		},
		func(err error, attempt int) {
			l.l.Error().Err(err).Msgf("could not resubscribe to the durable event listener (attempt %d/%d)", attempt, retry.StreamSyncMaxAttempts)
		},
		"could not send to the worker after %d retries",
	)
}

func (l *DurableEventsListener) Listen(ctx context.Context) error {
	return l.base.Listen(ctx, streamListenerRunConfig{
		isClosed:         l.isClosed,
		retryConnectSync: l.retryListenSync,
		lifecycleContext: l.lifecycleContext,
		listen:           l.listen,
		l:                l.l,
		listenErrorMsg:   "failed to listen for durable events",
	})
}

func (l *DurableEventsListener) listen(ctx context.Context) error {
	return listenReconnectingStream(
		ctx,
		l.streamCore(),
		streamListenConfig[contracts.V1Dispatcher_ListenForDurableEventClient, *contracts.DurableEvent]{
			recv: func(client contracts.V1Dispatcher_ListenForDurableEventClient) (*contracts.DurableEvent, error) {
				return client.Recv()
			},
			handle:               l.handleEvent,
			shouldReconnectOnEOF: l.shouldReconnectOnEOF,
			reconnectContext:     l.lifecycleContext(),
			labels: streamListenLabels{
				streamName:    "durable event listener",
				reconnectVerb: "relisten",
			},
			l: l.l,
		},
	)
}

func (l *DurableEventsListener) Close() error {
	return l.streamCore().Close()
}

func (l *DurableEventsListener) closeStream() error {
	return l.streamCore().closeStream()
}

func (l *DurableEventsListener) handleEvent(e *contracts.DurableEvent) error {
	t := listenTuple{
		taskId:    e.TaskId,
		signalKey: e.SignalKey,
	}

	// find all handlers for this workflow run
	handlers, ok := l.handlers.Load(t)

	if !ok {
		return nil
	}

	eg := errgroup.Group{}

	h := handlers.(*threadSafeDurableEventHandlers)

	h.mu.RLock()

	if h.deleted {
		h.mu.RUnlock()
		return nil
	}

	handlerSnapshots := make([]durableEventHandlerSnapshot, 0, len(h.handlers))
	for id, handler := range h.handlers {
		handlerSnapshots = append(handlerSnapshots, durableEventHandlerSnapshot{
			id:      id,
			handler: handler,
		})
	}

	h.mu.RUnlock()

	handledIDs := make([]uint64, 0, len(handlerSnapshots))

	for _, handlerSnapshot := range handlerSnapshots {
		handlerCp := handlerSnapshot.handler
		handledIDs = append(handledIDs, handlerSnapshot.id)

		eg.Go(func() error {
			return handlerCp(e)
		})
	}

	err := eg.Wait()

	if err == nil {
		l.removeDurableEventHandlersFromBucket(t, h, handledIDs...)
	}

	return err
}

func (l *DurableEventsListener) hasHandlers() bool {
	hasHandlers := false

	l.handlers.Range(func(key, value interface{}) bool {
		h := value.(*threadSafeDurableEventHandlers)

		h.mu.RLock()
		hasHandlers = !h.deleted && len(h.handlers) > 0
		h.mu.RUnlock()

		return !hasHandlers
	})

	return hasHandlers
}

func (l *DurableEventsListener) shouldReconnectOnEOF(ctx context.Context) bool {
	if ctx.Err() != nil || l.isClosed() {
		return false
	}

	return l.hasHandlers()
}
