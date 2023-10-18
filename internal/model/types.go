package model

import (
	"context"
	"fmt"
)

//go:generate protoc --go_out=. --go_opt=paths=source_relative types.proto

type DataAccess interface {
	// GetWorkspace returns the workspace with the given uid, we assume permission checks have _already_ been done on
	// this uid. This method returns the workspace, an ErrNotExist, or some other error.
	GetWorkspace(ctx context.Context, id string) (*Workspace, error)
	// BulkGetWorkspace returns the details of the given workspaces by uid in bulk. This is because there is no higher
	// level container in which to list workspaces. We assume permission checks have already been done against this
	// list of workspaces. This method returns a map from uid to workspace, or an error. It may return both if some
	// workspaces returned an ErrNotExist error.
	BulkGetWorkspace(ctx context.Context, id []string) (map[string]*Workspace, error)
	// CreateWorkspace inserts a new workspace into the data access layer.
	CreateWorkspace(ctx context.Context, ws *Workspace) (*Workspace, error)
	// UpdateWorkspace updates a workspace in place. It will return an error if the workspace doesn't already exist at
	// the given revision number.
	UpdateWorkspace(ctx context.Context, ws *Workspace) (*Workspace, error)
	// DeleteWorkspace deletes the workspace permanently. It will return an error if the workspace doesn't already exist
	// at the given revision number.
	DeleteWorkspace(ctx context.Context, ws *Workspace) error
	// ListWorkspaceChanges returns a list of changes for all workspaces since the last cursor.
	ListWorkspaceChanges(ctx context.Context, cursor []byte, limit int) (page *WorkspaceChangePage, err error)
	// StreamWorkspaceChanges returns a channel that will be populated with workspace change items as updates are
	// detected. The channel will be closed if the system is shutting down or the client is not keeping up with the
	// rate of items being emitted.
	StreamWorkspaceChanges(ctx context.Context, cursor []byte) (stream <-chan *WorkspaceChangePage, err error)
}

type WorkspaceChangePage struct {
	Changes    []*WorkspaceChange
	NextCursor []byte
}

type ErrNotExist struct {
	Inner error
}

func (e ErrNotExist) Error() string {
	if e.Inner == nil {
		return "not found"
	}
	return fmt.Sprintf("not found: %v", e.Inner)
}

func (e ErrNotExist) String() string {
	return e.Error()
}

func (e ErrNotExist) Unwrap() error {
	return e.Inner
}

type ErrIncorrectRevision struct {
	Inner error
}

func (e ErrIncorrectRevision) Error() string {
	if e.Inner == nil {
		return "incorrect revision"
	}
	return fmt.Sprintf("incorrect revision: %v", e.Inner)
}

func (e ErrIncorrectRevision) String() string {
	return e.Error()
}

func (e ErrIncorrectRevision) Unwrap() error {
	return e.Inner
}
