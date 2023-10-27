package sqlmodel

import (
	"context"
	"fmt"
	"log"
	"slices"
	"strings"
)

type notifier struct {
	id         int64
	filterUids map[string]bool
	output     chan string
}

// RunNotifier will execute the sql notify listener and upon each change will broadcast this to each connected channel.
// If the connection dies or the context closes, this method will return an error.
func (s *SqlDataAccess) RunNotifier(ctx context.Context) error {
	conn, err := s.pool.Acquire(ctx)
	if err != nil {
		return fmt.Errorf("failed to acquire connection: %w", err)
	}
	defer conn.Release()

	if _, err := conn.Exec(ctx, `LISTEN `+NotificationChannel); err != nil {
		return fmt.Errorf("failed to start listening session: %w", err)
	}

	for {
		n, err := conn.Conn().WaitForNotification(ctx)
		if err != nil {
			return err
		}
		uid := strings.Split(n.Payload, ",")[0]
		func() {
			s.notifierStateLock.RLock()
			defer s.notifierStateLock.RUnlock()
			for _, nfier := range s.notifiers {
				if len(nfier.filterUids) == 0 || nfier.filterUids[uid] {
					select {
					case nfier.output <- n.Payload:
					default:
						log.Print("closing channel")
						close(nfier.output)
					}
				}
			}
		}()
	}
}

// ListenForWorkspaceUidChanges will register a listener until the surrounding context is closed or output channel closed.
func (s *SqlDataAccess) ListenForWorkspaceUidChanges(ctx context.Context, filterUids []string, output chan string) (error, func()) {
	// don't go further if the context is already closed
	if ctx.Err() != nil {
		return ctx.Err(), nil
	}
	// convert filter uids into a map for easier access
	filterUidsSet := make(map[string]bool, len(filterUids))
	for _, uid := range filterUids {
		filterUidsSet[uid] = true
	}

	// lock so that we can register/deregister and start a goroutine
	s.notifierStateLock.Lock()
	defer s.notifierStateLock.Unlock()

	// add the new notifier
	if s.notifiers == nil {
		s.notifiers = make([]notifier, 0, 1)
	}
	notifierId := s.lastNotifierId.Add(1)
	s.notifiers = append(s.notifiers, notifier{id: notifierId, output: output, filterUids: filterUidsSet})

	// return no error and a closing function that can be used to close and delete things
	return nil, func() {
		s.notifierStateLock.Lock()
		defer s.notifierStateLock.Unlock()
		slices.DeleteFunc(s.notifiers, func(n notifier) bool {
			return n.id == notifierId
		})
	}
}
