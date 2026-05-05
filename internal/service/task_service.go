package service

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"

	"github.com/ayamschikov/task-queue/internal/model"
	"github.com/ayamschikov/task-queue/internal/repository"
)

const defaultMaxRetries = 3

var (
	ErrInvalidType = errors.New("type is required")
	ErrNotFound    = errors.New("task not found")
)

type TaskRepo interface {
	Enqueue(ctx context.Context, p repository.EnqueueParams) (*model.Task, error)
	FindByID(ctx context.Context, id int) (*model.Task, error)
}

type TaskService struct {
	repo TaskRepo
}

func NewTaskService(r TaskRepo) *TaskService {
	return &TaskService{repo: r}
}

type EnqueueInput struct {
	Type       string
	Payload    json.RawMessage
	Priority   int
	MaxRetries int
}

func (s *TaskService) Enqueue(ctx context.Context, in EnqueueInput) (*model.Task, error) {
	if in.Type == "" {
		return nil, ErrInvalidType
	}
	if in.MaxRetries <= 0 {
		in.MaxRetries = defaultMaxRetries
	}
	if len(in.Payload) == 0 {
		in.Payload = json.RawMessage(`{}`)
	}
	t, err := s.repo.Enqueue(ctx, repository.EnqueueParams{
		Type:       in.Type,
		Payload:    in.Payload,
		Priority:   in.Priority,
		MaxRetries: in.MaxRetries,
	})
	if err != nil {
		return nil, fmt.Errorf("service enqueue: %w", err)
	}
	return t, nil
}

func (s *TaskService) GetByID(ctx context.Context, id int) (*model.Task, error) {
	t, err := s.repo.FindByID(ctx, id)
	if err != nil {
		if errors.Is(err, repository.ErrNotFound) {
			return nil, ErrNotFound
		}
		return nil, fmt.Errorf("service get by id: %w", err)
	}
	return t, nil
}
