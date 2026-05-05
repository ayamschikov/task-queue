-- +goose Up
ALTER TABLE tasks ADD COLUMN picked_at TIMESTAMPTZ;

-- Used by the stuck-task sweep: only running rows matter, partial keeps it small.
CREATE INDEX idx_tasks_running_picked ON tasks (picked_at) WHERE status = 'running';

-- +goose Down
DROP INDEX idx_tasks_running_picked;
ALTER TABLE tasks DROP COLUMN picked_at;
