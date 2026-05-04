package api

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"os"
	"time"

	engine "github.com/OpenNSW/go-temporal-workflow"
	"github.com/OpenNSW/nsw-task-flow/internal/orchestrator"
)

type Server struct {
	manager *orchestrator.TaskManager
}

func NewServer(manager *orchestrator.TaskManager) *Server {
	return &Server{manager: manager}
}

func (s *Server) Start(addr string) {
	// Serve static files
	http.Handle("/", http.FileServer(http.Dir("./static")))

	// API Endpoints
	http.HandleFunc("/api/tasks", s.handleGetTasks)
	http.HandleFunc("/api/start", s.handleStartWorkflow)
	http.HandleFunc("/api/submit", s.handleSubmitTask)

	log.Printf("[API] Starting HTTP API on %s...", addr)
	go func() {
		if err := http.ListenAndServe(addr, nil); err != nil {
			log.Printf("[API] HTTP Server error: %v", err)
		}
	}()
}

func (s *Server) handleGetTasks(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(s.manager.GetDB().GetAllTasks())
}

func (s *Server) handleStartWorkflow(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "POST only", http.StatusMethodNotAllowed)
		return
	}

	fileBytes, err := os.ReadFile("workflow.json")
	if err != nil {
		http.Error(w, "Failed to read workflow.json", http.StatusInternalServerError)
		return
	}

	var def engine.WorkflowDefinition
	if err := json.Unmarshal(fileBytes, &def); err != nil {
		http.Error(w, "Failed to parse workflow.json", http.StatusInternalServerError)
		return
	}

	workflowID := "nsw-demo-wf-" + time.Now().Format("150405")
	log.Printf("[API] Submitting Workflow ID: %s to Layer 1", workflowID)

	err = s.manager.GetLayer1Manager().StartWorkflow(context.Background(), workflowID, def, map[string]any{})
	if err != nil {
		http.Error(w, fmt.Sprintf("Failed to start workflow: %v", err), http.StatusInternalServerError)
		return
	}

	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)
	w.Write([]byte(fmt.Sprintf(`{"status":"ok", "workflow_id":"%s"}`, workflowID)))
}

func (s *Server) handleSubmitTask(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "POST only", http.StatusMethodNotAllowed)
		return
	}

	var req struct {
		TaskID string         `json:"task_id"`
		Output map[string]any `json:"output"`
	}
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	db := s.manager.GetDB()
	record, exists := db.GetTask(req.TaskID)
	if !exists {
		http.Error(w, "Task not found", http.StatusNotFound)
		return
	}

	if record.IsCompleted {
		http.Error(w, "Task already completed", http.StatusConflict)
		return
	}

	// Merge original inputs with the new output from frontend
	finalOutput := make(map[string]any)
	for k, v := range record.Inputs {
		finalOutput[k] = v
	}
	for k, v := range req.Output {
		finalOutput[k] = v
	}

	// Update DB with the merged data but DO NOT mark as completed yet.
	// The Graph Engine (Child Workflow) will trigger completion when it hits END.
	record.Inputs = finalOutput
	db.SaveTask(req.TaskID, record)

	// Complete the parent task in Temporal using layer2Manager
	err := s.manager.GetLayer2Manager().TaskDone(context.Background(), record.ParentWorkflowID, record.RunID, record.NodeID, finalOutput)
	if err != nil {
		log.Printf("[API] Temporal completion failed: %v", err)
		http.Error(w, "Temporal completion failed", http.StatusInternalServerError)
		return
	}

	log.Printf("[API] Task %s marked as done from Frontend!", req.TaskID)
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)
	w.Write([]byte(`{"status":"ok"}`))
}
