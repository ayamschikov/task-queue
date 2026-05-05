package handler

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"strconv"

	"github.com/go-chi/chi/v5"

	"github.com/ayamschikov/task-queue/internal/model"
	"github.com/ayamschikov/task-queue/internal/service"
)

type TaskService interface {
	Enqueue(ctx context.Context, in service.EnqueueInput) (*model.Task, error)
	GetByID(ctx context.Context, id int) (*model.Task, error)
}

type TaskHandler struct {
	svc TaskService
}

func NewTaskHandler(s TaskService) *TaskHandler {
	return &TaskHandler{svc: s}
}

func (h *TaskHandler) Routes(r chi.Router) {
	r.Post("/tasks", h.create)
	r.Get("/tasks/{id}", h.get)
}

type createReq struct {
	Type       string          `json:"type"`
	Payload    json.RawMessage `json:"payload,omitempty"`
	Priority   int             `json:"priority,omitempty"`
	MaxRetries int             `json:"max_retries,omitempty"`
}

func (h *TaskHandler) create(w http.ResponseWriter, r *http.Request) {
	var req createReq
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		writeError(w, http.StatusBadRequest, "invalid json")
		return
	}
	t, err := h.svc.Enqueue(r.Context(), service.EnqueueInput{
		Type:       req.Type,
		Payload:    req.Payload,
		Priority:   req.Priority,
		MaxRetries: req.MaxRetries,
	})
	if err != nil {
		if errors.Is(err, service.ErrInvalidType) {
			writeError(w, http.StatusBadRequest, err.Error())
			return
		}
		writeError(w, http.StatusInternalServerError, "internal error")
		return
	}
	writeJSON(w, http.StatusCreated, t)
}

func (h *TaskHandler) get(w http.ResponseWriter, r *http.Request) {
	id, err := strconv.Atoi(chi.URLParam(r, "id"))
	if err != nil {
		writeError(w, http.StatusBadRequest, "invalid id")
		return
	}
	t, err := h.svc.GetByID(r.Context(), id)
	if err != nil {
		if errors.Is(err, service.ErrNotFound) {
			writeError(w, http.StatusNotFound, "task not found")
			return
		}
		writeError(w, http.StatusInternalServerError, "internal error")
		return
	}
	writeJSON(w, http.StatusOK, t)
}

func writeJSON(w http.ResponseWriter, status int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(v)
}

func writeError(w http.ResponseWriter, status int, msg string) {
	writeJSON(w, status, map[string]string{"error": msg})
}
