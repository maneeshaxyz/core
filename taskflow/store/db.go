package store

import "time"

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

// TaskStore is an interface that any persistent or in-memory database used by the TaskManager should implement.
type TaskStore interface {
	SaveTask(record TaskRecord)
	GetTask(taskID string) (TaskRecord, bool)
	GetTaskByLayer2WorkflowID(layer2WorkflowID string) (TaskRecord, bool)
	GetAllTasks() []TaskRecord
}
