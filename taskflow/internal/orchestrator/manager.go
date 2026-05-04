package orchestrator

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"os"
	"time"

	engine "github.com/OpenNSW/go-temporal-workflow"
	"github.com/OpenNSW/nsw-task-flow/internal/store"
)

// TaskManager orchestrates external task requests and mock completion
type TaskManager struct {
	db            *store.TaskDB
	layer1Manager engine.TemporalManager
	layer2Manager engine.TemporalManager
}

func NewTaskManager(db *store.TaskDB, layer1 engine.TemporalManager, layer2 engine.TemporalManager) *TaskManager {
	return &TaskManager{
		db:            db,
		layer1Manager: layer1,
		layer2Manager: layer2,
	}
}

// StartTask handles an incoming task request from the workflow engine (Layer 1).
// It starts a Layer 2 workflow based on task.json.
func (tm *TaskManager) StartTask(payload engine.TaskPayload) error {
	taskID := payload.NodeID // using NodeID as unique identifier for this run
	layer2WorkflowID := fmt.Sprintf("task-layer2-%s-%s", payload.WorkflowID, payload.NodeID)

	// 1. Write task information to DB
	record := store.TaskRecord{
		WorkflowID:       payload.WorkflowID,
		RunID:            payload.RunID,
		NodeID:           payload.NodeID,
		TaskTemplateID:   payload.TaskTemplateID,
		Layer2WorkflowID: layer2WorkflowID,
		Status:           "PENDING_L2",
		CreatedAt:        time.Now(),
	}
	tm.db.SaveTask(taskID, record)
	log.Printf("[TaskManager] Persisted new task to DB. TaskID: %s, L2 Workflow: %s", taskID, layer2WorkflowID)

	// 2. Load task.json
	fileBytes, err := os.ReadFile("task.json")
	if err != nil {
		return fmt.Errorf("failed to read task.json: %v", err)
	}

	var def engine.WorkflowDefinition
	if err := json.Unmarshal(fileBytes, &def); err != nil {
		return fmt.Errorf("failed to parse task.json: %v", err)
	}

	// 3. Start Layer 2 Workflow
	log.Printf("[TaskManager] starting layer 2 workflow with payload %v", payload)

	initialVars := map[string]any{
		"applicant_name":    "",
		"justification":     "",
		"reviewer_comments": "",
		"review_outcome":    "",
	}

	err = tm.layer2Manager.StartWorkflow(context.Background(), layer2WorkflowID, def, initialVars)
	if err != nil {
		return fmt.Errorf("failed to start layer 2 workflow: %v", err)
	}

	log.Printf("[TaskManager] Successfully started Layer 2 workflow: %s", layer2WorkflowID)

	return nil
}

// HandleLayer2Completion is called when ANY workflow completes.
// We check if it's a Layer 2 workflow, and if so, complete the corresponding Layer 1 task.
func (tm *TaskManager) HandleLayer2Completion(workflowID string, finalVariables map[string]any) error {
	record, exists := tm.db.GetTaskByLayer2WorkflowID(workflowID)
	if !exists {
		// Not a layer 2 workflow we launched, or DB lost it. Safe to ignore.
		return nil
	}

	log.Printf("[TaskManager] Detected completion of Layer 2 workflow %s for Task %s", workflowID, record.NodeID)

	// Update DB status and store the final outcome
	record.Status = "COMPLETED"
	record.Inputs = finalVariables
	tm.db.SaveTask(record.NodeID, record)

	// Complete the Layer 1 task in Temporal
	err := tm.layer1Manager.TaskDone(context.Background(), record.WorkflowID, record.RunID, record.NodeID, finalVariables)
	if err != nil {
		log.Printf("[TaskManager] Failed to complete Layer 1 task in Temporal: %v", err)
		return err
	}

	log.Printf("[TaskManager] Task %s marked as done in Layer 1!", record.NodeID)
	return nil
}

// StartLayer3Task persists a Layer 3 subtask and waits for frontend interaction.
func (tm *TaskManager) StartLayer3Task(payload engine.TaskPayload) error {
	taskID := payload.NodeID

	record := store.TaskRecord{
		WorkflowID:     payload.WorkflowID,
		RunID:          payload.RunID,
		NodeID:         payload.NodeID,
		TaskTemplateID: payload.TaskTemplateID,
		Status:         "PENDING_L3",
		Inputs:         payload.Inputs,
		CreatedAt:      time.Now(),
	}
	tm.db.SaveTask(taskID, record)
	log.Printf("[TaskManager] Persisted Layer 3 task to DB. TaskID: %s", taskID)

	return nil
}

func (tm *TaskManager) GetDB() *store.TaskDB {
	return tm.db
}

func (tm *TaskManager) GetLayer1Manager() engine.TemporalManager {
	return tm.layer1Manager
}

func (tm *TaskManager) GetLayer2Manager() engine.TemporalManager {
	return tm.layer2Manager
}
