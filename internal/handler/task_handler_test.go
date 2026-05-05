package handler

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/go-chi/chi/v5"

	"github.com/ayamschikov/task-queue/internal/model"
	"github.com/ayamschikov/task-queue/internal/service"
)

type fakeService struct {
	enqueueIn     service.EnqueueInput
	enqueueResult *model.Task
	enqueueErr    error
	getID         int
	getResult     *model.Task
	getErr        error
}

func (f *fakeService) Enqueue(_ context.Context, in service.EnqueueInput) (*model.Task, error) {
	f.enqueueIn = in
	return f.enqueueResult, f.enqueueErr
}

func (f *fakeService) GetByID(_ context.Context, id int) (*model.Task, error) {
	f.getID = id
	return f.getResult, f.getErr
}

func newRouter(svc TaskService) *chi.Mux {
	r := chi.NewRouter()
	NewTaskHandler(svc).Routes(r)
	return r
}

func TestCreate_success(t *testing.T) {
	svc := &fakeService{enqueueResult: &model.Task{ID: 42, Type: "send_email"}}
	body := strings.NewReader(`{"type":"send_email","priority":5}`)
	req := httptest.NewRequest("POST", "/tasks", body)
	rec := httptest.NewRecorder()

	newRouter(svc).ServeHTTP(rec, req)

	if rec.Code != http.StatusCreated {
		t.Fatalf("status: got %d want 201", rec.Code)
	}
	var got model.Task
	if err := json.Unmarshal(rec.Body.Bytes(), &got); err != nil {
		t.Fatal(err)
	}
	if got.ID != 42 {
		t.Errorf("response id: got %d want 42", got.ID)
	}
	if svc.enqueueIn.Type != "send_email" {
		t.Errorf("service input type: got %q", svc.enqueueIn.Type)
	}
	if svc.enqueueIn.Priority != 5 {
		t.Errorf("service input priority: got %d", svc.enqueueIn.Priority)
	}
}

func TestCreate_invalidJSON(t *testing.T) {
	svc := &fakeService{}
	req := httptest.NewRequest("POST", "/tasks", strings.NewReader(`{not json`))
	rec := httptest.NewRecorder()

	newRouter(svc).ServeHTTP(rec, req)

	if rec.Code != http.StatusBadRequest {
		t.Errorf("status: got %d want 400", rec.Code)
	}
}

func TestCreate_serviceInvalidType(t *testing.T) {
	svc := &fakeService{enqueueErr: service.ErrInvalidType}
	req := httptest.NewRequest("POST", "/tasks", strings.NewReader(`{}`))
	rec := httptest.NewRecorder()

	newRouter(svc).ServeHTTP(rec, req)

	if rec.Code != http.StatusBadRequest {
		t.Errorf("status: got %d want 400", rec.Code)
	}
}

func TestCreate_internalError(t *testing.T) {
	svc := &fakeService{enqueueErr: errors.New("boom")}
	req := httptest.NewRequest("POST", "/tasks", strings.NewReader(`{"type":"x"}`))
	rec := httptest.NewRecorder()

	newRouter(svc).ServeHTTP(rec, req)

	if rec.Code != http.StatusInternalServerError {
		t.Errorf("status: got %d want 500", rec.Code)
	}
}

func TestGet_success(t *testing.T) {
	svc := &fakeService{getResult: &model.Task{ID: 42}}
	req := httptest.NewRequest("GET", "/tasks/42", nil)
	rec := httptest.NewRecorder()

	newRouter(svc).ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("status: got %d want 200", rec.Code)
	}
	if svc.getID != 42 {
		t.Errorf("service id: got %d want 42", svc.getID)
	}
}

func TestGet_invalidID(t *testing.T) {
	svc := &fakeService{}
	req := httptest.NewRequest("GET", "/tasks/abc", nil)
	rec := httptest.NewRecorder()

	newRouter(svc).ServeHTTP(rec, req)

	if rec.Code != http.StatusBadRequest {
		t.Errorf("status: got %d want 400", rec.Code)
	}
}

func TestGet_notFound(t *testing.T) {
	svc := &fakeService{getErr: service.ErrNotFound}
	req := httptest.NewRequest("GET", "/tasks/999", nil)
	rec := httptest.NewRecorder()

	newRouter(svc).ServeHTTP(rec, req)

	if rec.Code != http.StatusNotFound {
		t.Errorf("status: got %d want 404", rec.Code)
	}
}
