package service

import (
	"context"
	"encoding/json"
	"errors"
	"testing"

	"github.com/ayamschikov/task-queue/internal/model"
	"github.com/ayamschikov/task-queue/internal/repository"
)

type fakeRepo struct {
	enqueueParams repository.EnqueueParams
	enqueueResult *model.Task
	enqueueErr    error
	findID        int
	findResult    *model.Task
	findErr       error
}

func (f *fakeRepo) Enqueue(_ context.Context, p repository.EnqueueParams) (*model.Task, error) {
	f.enqueueParams = p
	return f.enqueueResult, f.enqueueErr
}

func (f *fakeRepo) FindByID(_ context.Context, id int) (*model.Task, error) {
	f.findID = id
	return f.findResult, f.findErr
}

func TestEnqueue_emptyType(t *testing.T) {
	svc := NewTaskService(&fakeRepo{})
	_, err := svc.Enqueue(context.Background(), EnqueueInput{Type: ""})
	if !errors.Is(err, ErrInvalidType) {
		t.Errorf("err: got %v want ErrInvalidType", err)
	}
}

func TestEnqueue_appliesDefaults(t *testing.T) {
	repo := &fakeRepo{enqueueResult: &model.Task{ID: 1}}
	svc := NewTaskService(repo)

	if _, err := svc.Enqueue(context.Background(), EnqueueInput{Type: "x"}); err != nil {
		t.Fatal(err)
	}

	if repo.enqueueParams.MaxRetries != defaultMaxRetries {
		t.Errorf("max_retries: got %d want %d", repo.enqueueParams.MaxRetries, defaultMaxRetries)
	}
	if string(repo.enqueueParams.Payload) != "{}" {
		t.Errorf("payload: got %q want '{}'", string(repo.enqueueParams.Payload))
	}
}

func TestEnqueue_propagatesValues(t *testing.T) {
	repo := &fakeRepo{enqueueResult: &model.Task{ID: 1}}
	svc := NewTaskService(repo)

	in := EnqueueInput{
		Type:       "send_email",
		Payload:    json.RawMessage(`{"to":"x"}`),
		Priority:   5,
		MaxRetries: 7,
	}
	if _, err := svc.Enqueue(context.Background(), in); err != nil {
		t.Fatal(err)
	}

	if repo.enqueueParams.Type != "send_email" {
		t.Errorf("type: got %q", repo.enqueueParams.Type)
	}
	if repo.enqueueParams.Priority != 5 {
		t.Errorf("priority: got %d", repo.enqueueParams.Priority)
	}
	if repo.enqueueParams.MaxRetries != 7 {
		t.Errorf("max_retries: got %d (default should not override explicit value)", repo.enqueueParams.MaxRetries)
	}
	if string(repo.enqueueParams.Payload) != `{"to":"x"}` {
		t.Errorf("payload: got %q", string(repo.enqueueParams.Payload))
	}
}

func TestEnqueue_wrapsRepoError(t *testing.T) {
	boom := errors.New("boom")
	svc := NewTaskService(&fakeRepo{enqueueErr: boom})

	_, err := svc.Enqueue(context.Background(), EnqueueInput{Type: "x"})
	if !errors.Is(err, boom) {
		t.Errorf("err: got %v want wrapped boom", err)
	}
}

func TestGetByID_mapsRepoNotFound(t *testing.T) {
	svc := NewTaskService(&fakeRepo{findErr: repository.ErrNotFound})

	_, err := svc.GetByID(context.Background(), 1)
	if !errors.Is(err, ErrNotFound) {
		t.Errorf("err: got %v want service.ErrNotFound", err)
	}
}

func TestGetByID_returnsTask(t *testing.T) {
	want := &model.Task{ID: 42}
	svc := NewTaskService(&fakeRepo{findResult: want})

	got, err := svc.GetByID(context.Background(), 42)
	if err != nil {
		t.Fatal(err)
	}
	if got.ID != 42 {
		t.Errorf("id: got %d want 42", got.ID)
	}
}

func TestGetByID_wrapsOtherError(t *testing.T) {
	boom := errors.New("boom")
	svc := NewTaskService(&fakeRepo{findErr: boom})

	_, err := svc.GetByID(context.Background(), 1)
	if !errors.Is(err, boom) {
		t.Errorf("err: got %v want wrapped boom", err)
	}
}
