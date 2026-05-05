-- +goose Up
CREATE TABLE tasks (
    id          SERIAL PRIMARY KEY,
    type        TEXT        NOT NULL,              -- тип задачи: "email", "resize" и т.д.
    payload     JSONB       NOT NULL DEFAULT '{}', -- данные задачи
    status      TEXT        NOT NULL DEFAULT 'pending',
    priority    INT         NOT NULL DEFAULT 0,    -- чем выше, тем приоритетнее
    attempts    INT         NOT NULL DEFAULT 0,    -- сколько раз пытались выполнить
    max_retries INT         NOT NULL DEFAULT 3,
    last_error  TEXT,                              -- текст последней ошибки
    created_at  TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at  TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    scheduled_at TIMESTAMPTZ NOT NULL DEFAULT NOW() -- когда задачу можно брать (для отложенного retry)
);

-- Индекс для воркера: быстро найти pending задачи, отсортированные по приоритету.
-- Partial index — только pending, не тратим место на done/failed.
CREATE INDEX idx_tasks_pending ON tasks (priority DESC, scheduled_at)
    WHERE status = 'pending';

-- +goose Down
DROP TABLE tasks;
