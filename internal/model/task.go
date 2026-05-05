package model

import (
	"encoding/json"
	"time"
)

type TaskStatus string

const (
	TaskStatusPending TaskStatus = "pending"
	TaskStatusRunning TaskStatus = "running"
	TaskStatusDone    TaskStatus = "done"
	TaskStatusFailed  TaskStatus = "failed"
)

type Task struct {
	ID          int             `json:"id"`
	Type        string          `json:"type"`
	Payload     json.RawMessage `json:"payload"`
	Status      TaskStatus      `json:"status"`
	Priority    int             `json:"priority"`
	Attempts    int             `json:"attempts"`
	MaxRetries  int             `json:"max_retries"`
	LastError   *string         `json:"last_error,omitempty"`
	CreatedAt   time.Time       `json:"created_at"`
	UpdatedAt   time.Time       `json:"updated_at"`
	ScheduledAt time.Time       `json:"scheduled_at"`
	PickedAt    *time.Time      `json:"picked_at,omitempty"`
}
