package sqlmodel

import (
	"context"
	"log"
	"os"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/types/known/timestamppb"

	"github.com/astromechza/eventing/internal/model"
)

//goland:noinspection SqlWithoutWhere
func TestRealSql(t *testing.T) {
	dbUrl := os.Getenv("DB_URL")
	if dbUrl == "" {
		t.SkipNow()
	}

	rootContext, cancel := context.WithTimeout(context.Background(), time.Minute)
	defer cancel()

	da, err := New(rootContext, dbUrl)
	require.NoError(t, err)
	defer da.Close()
	_, err = da.pool.Exec(rootContext, `DELETE FROM workspaces`)
	require.NoError(t, err)
	_, err = da.pool.Exec(rootContext, `DELETE FROM workspace_changes`)
	require.NoError(t, err)

	// this bit gets set up by the server and runs forever in a loop
	subCtx, subCancel := context.WithCancel(rootContext)
	defer subCancel()
	go func() {
		log.Print("running notifier")
		assert.ErrorIs(t, da.RunNotifier(subCtx), context.Canceled)
		log.Print("finished notifier")
	}()

	// setup a buffered channel and begin listening
	listenChan := make(chan *model.WorkspaceChangeNotification, 2000)
	err, listenCloser := da.ListenForWorkspaceChanges(rootContext, nil, listenChan)
	require.NoError(t, err)
	defer listenCloser()

	var ws *model.Workspace
	t.Run("can create a workspace", func(t *testing.T) {
		ws = new(model.Workspace)
		ws.Reset()
		ws.DisplayName = "my workspace"
		ws.CreatedAt = timestamppb.Now()
		ws.Lifecycle = model.Workspace_LIFECYCLE_ACTIVE
		ws, err = da.CreateWorkspace(rootContext, ws)
		if assert.NoError(t, err) {
			t.Logf("created workspace with uid %s", ws.Uid)
			assert.NotEmpty(t, ws.GetUid())
			assert.Equal(t, int64(1), ws.Revision)
			assert.Equal(t, "my workspace", ws.DisplayName)
		}
	})

	t.Run("can get the workspace", func(t *testing.T) {
		t.Logf("looking up workspace by uid %s", ws.Uid)
		returned, err := da.GetWorkspace(rootContext, ws.Uid)
		if assert.NoError(t, err) {
			assert.Equal(t, ws.Uid, returned.Uid)
			assert.Equal(t, ws.Revision, returned.Revision)
			assert.Equal(t, "my workspace", returned.DisplayName)
		}
	})

	t.Run("cant get the workspace that doesn't exist", func(t *testing.T) {
		res, err := da.GetWorkspace(rootContext, "unknown")
		assert.Nil(t, res)
		var errAs model.ErrNotExist
		assert.ErrorAs(t, err, &errAs)
	})

	t.Run("can list the workspace", func(t *testing.T) {
		returned, err := da.BulkGetWorkspace(rootContext, []string{ws.Uid, "does-not-exist"})
		if assert.NoError(t, err) {
			assert.Len(t, returned, 1)
			if assert.Contains(t, returned, ws.Uid) {
				assert.Equal(t, ws.Uid, returned[ws.Uid].Uid)
				assert.Equal(t, ws.Revision, returned[ws.Uid].Revision)
				assert.Equal(t, "my workspace", returned[ws.Uid].DisplayName)
			}
		}
	})

	t.Run("can update the workspace", func(t *testing.T) {
		ws.DisplayName = ws.DisplayName + "2"
		ws, err = da.UpdateWorkspace(rootContext, ws)
		if assert.NoError(t, err) {
			assert.Equal(t, int64(2), ws.Revision)
			assert.Equal(t, "my workspace2", ws.DisplayName)
		}
	})

	t.Run("can delete the workspace", func(t *testing.T) {
		assert.NoError(t, da.DeleteWorkspace(rootContext, ws))
	})

	t.Run("confirm that workspace no longer exists", func(t *testing.T) {
		res, err := da.GetWorkspace(rootContext, ws.Uid)
		assert.Nil(t, res)
		var errAs model.ErrNotExist
		assert.ErrorAs(t, err, &errAs)
	})

	t.Run("the changes list contains the entries for this row", func(t *testing.T) {
		changes, err := da.ListWorkspaceChanges(rootContext, nil, 100, []string{ws.Uid})
		if assert.NoError(t, err) {
			filtered := changes.Changes
			if assert.Len(t, filtered, 3) {
				assert.Equal(t, 1, int(filtered[0].GetWorkspace().GetRevision()))
				assert.Equal(t, 2, int(filtered[1].GetWorkspace().GetRevision()))
				assert.Equal(t, 3, int(filtered[2].GetTombstone().GetRevision()))
				assert.Greater(t, filtered[1].Entry, filtered[0].Entry)
				assert.Greater(t, filtered[2].Entry, filtered[1].Entry)
			}
		}
	})

	t.Run("and no changes for an unknown row", func(t *testing.T) {
		changes, err := da.ListWorkspaceChanges(rootContext, nil, 100, []string{"unknown"})
		if assert.NoError(t, err) {
			filtered := changes.Changes
			assert.Len(t, filtered, 0)
		}
	})

	t.Run("add 1000", func(t *testing.T) {
		for i := 0; i < 1000; i++ {
			ws = new(model.Workspace)
			ws.Reset()
			ws.DisplayName = "my workspace"
			ws.CreatedAt = timestamppb.Now()
			ws.Lifecycle = model.Workspace_LIFECYCLE_ACTIVE
			_, err = da.CreateWorkspace(rootContext, ws)
			assert.NoError(t, err)
		}
	})

	t.Run("the changes list is limited to 100", func(t *testing.T) {
		changes, err := da.ListWorkspaceChanges(rootContext, nil, 100, nil)
		if assert.NoError(t, err) {
			assert.Len(t, changes.Changes, 100)
		}
	})

	t.Run("peek works", func(t *testing.T) {
		cursor, err := da.PeekLastWorkspaceChange(rootContext)
		require.NoError(t, err)
		page, err := da.ListWorkspaceChanges(rootContext, cursor, 100, nil)
		require.NoError(t, err)
		assert.Equal(t, len(page.Changes), 0)
		assert.Equal(t, cursor, page.NextCursor)

		ws = new(model.Workspace)
		ws.Reset()
		ws.DisplayName = "my workspace"
		ws.CreatedAt = timestamppb.Now()
		ws.Lifecycle = model.Workspace_LIFECYCLE_ACTIVE
		_, err = da.CreateWorkspace(rootContext, ws)
		assert.NoError(t, err)

		page, err = da.ListWorkspaceChanges(rootContext, cursor, 100, nil)
		require.NoError(t, err)
		if assert.Equal(t, len(page.Changes), 1) {
			assert.Equal(t, "my workspace", page.Changes[0].GetWorkspace().DisplayName)
		}
		assert.NotEqual(t, cursor, page.NextCursor)
	})

	t.Run("notifications were received", func(t *testing.T) {
		counter := 0
		for range listenChan {
			counter += 1
			if counter >= 1004 {
				log.Print(counter)
				break
			}
		}
		t.Logf("got %d notifications", counter)
		assert.Equal(t, 1004, counter)
	})
}
