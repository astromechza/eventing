-- +goose Up
CREATE TABLE workspaces (
      uid text NOT NULL PRIMARY KEY,
      revision bigint NOT NULL,
      raw bytea NOT NULL
);
CREATE TABLE workspace_changes (
    entry bigint NOT NULL PRIMARY KEY,
    uid text NOT NULL,
    revision bigint NOT NULL,
    at timestamp WITHOUT TIME ZONE NOT NULL,
    raw bytea NULL,
    tombstone bool NOT NULL DEFAULT false
);
CREATE INDEX workspace_changes_uid_idx ON workspace_changes(uid);
CREATE SEQUENCE workspace_change_entry AS bigint INCREMENT BY 1 MINVALUE 1 CACHE 1;

-- +goose Down
DROP TABLE IF EXISTS workspaces;
DROP TABLE IF EXISTS workspace_changes;
DROP SEQUENCE IF EXISTS workspace_change_entry;
