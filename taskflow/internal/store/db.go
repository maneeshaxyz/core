package store

import (
	"encoding/json"
	"log"
	"os"
	"sync"
	"time"
)

const dbFilePath = "/tmp/nsw_task_db.json"

// TaskRecord is the single DB entry per task instance, as described in the architecture doc.
// It stores both Layer 1 (parent) and Layer 2 (active sub-process) coordinates separately,
// and holds Data as a namespaced map mirroring the JSONForms structure.
type TaskRecord struct {
	TaskID         string `json:"task_id"`
	TaskType       string `json:"task_type"`
	UserFormID     string `json:"user_form_id"`
	ReviewerFormID string `json:"reviewer_form_id"`
	// Status drives UI rendering ("PENDING_USER", "QUEUED_EXTERNALLY", "COMPLETED")
	Status string `json:"status"`

	// Layer 1 parent coordinates — used to wake Layer 1 when Layer 2 finishes
	Layer1WorkflowID string `json:"layer1_workflow_id"`
	Layer1RunID      string `json:"layer1_run_id"`
	Layer1NodeID     string `json:"layer1_node_id"`

	// Layer 2 active sub-process coordinates — used to wake Layer 2 via the API
	Layer2WorkflowID string `json:"layer2_workflow_id"`
	Layer2RunID      string `json:"layer2_run_id"`
	ActiveActivityID string `json:"active_activity_id"`

	// Data mirrors the namespaced JSONForms structure: {"userform": {...}, "reviewerform": {...}}
	Data map[string]any `json:"data"`

	CreatedAt time.Time `json:"created_at"`
}

// TaskDB is an in-memory, file-backed database for task records.
type TaskDB struct {
	mu    sync.RWMutex
	tasks map[string]TaskRecord // keyed by TaskID
}

func NewTaskDB() *TaskDB {
	db := &TaskDB{
		tasks: make(map[string]TaskRecord),
	}

	data, err := os.ReadFile(dbFilePath)
	if err == nil {
		if err := json.Unmarshal(data, &db.tasks); err != nil {
			log.Printf("[TaskDB] Failed to parse existing DB file: %v", err)
		} else {
			log.Printf("[TaskDB] Loaded %d tasks from %s", len(db.tasks), dbFilePath)
		}
	} else if !os.IsNotExist(err) {
		log.Printf("[TaskDB] Failed to read DB file: %v", err)
	}

	return db
}

func (db *TaskDB) saveToFile() {
	data, err := json.MarshalIndent(db.tasks, "", "  ")
	if err != nil {
		log.Printf("[TaskDB] Failed to marshal tasks: %v", err)
		return
	}
	if err := os.WriteFile(dbFilePath, data, 0644); err != nil {
		log.Printf("[TaskDB] Failed to write DB file: %v", err)
	}
}

func (db *TaskDB) SaveTask(record TaskRecord) {
	db.mu.Lock()
	defer db.mu.Unlock()
	db.tasks[record.TaskID] = record
	db.saveToFile()
}

func (db *TaskDB) GetTask(taskID string) (TaskRecord, bool) {
	db.mu.RLock()
	defer db.mu.RUnlock()
	record, exists := db.tasks[taskID]
	return record, exists
}

func (db *TaskDB) GetTaskByLayer2WorkflowID(layer2WorkflowID string) (TaskRecord, bool) {
	db.mu.RLock()
	defer db.mu.RUnlock()
	for _, record := range db.tasks {
		if record.Layer2WorkflowID == layer2WorkflowID {
			return record, true
		}
	}
	return TaskRecord{}, false
}

func (db *TaskDB) GetAllTasks() []TaskRecord {
	db.mu.RLock()
	defer db.mu.RUnlock()
	var list []TaskRecord
	for _, record := range db.tasks {
		list = append(list, record)
	}
	return list
}
