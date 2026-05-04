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
	// Use a unique ID for this specific task instance (WorkflowID + NodeID)
	taskID := fmt.Sprintf("%s:%s", payload.WorkflowID, payload.NodeID)
	childWorkflowID := fmt.Sprintf("task-layer2-%s-%s", payload.WorkflowID, payload.NodeID)

	// 1. Write the base "Application" record to DB
	record := store.TaskRecord{
		ParentWorkflowID: payload.WorkflowID,
		RunID:            payload.RunID,
		NodeID:           payload.NodeID,
		TaskTemplateID:   payload.TaskTemplateID,
		ChildWorkflowID:  childWorkflowID,
		IsCompleted:      false,
		CreatedAt:        time.Now(),
		Inputs:           make(map[string]any),
	}
	tm.db.SaveTask(taskID, record)
	log.Printf("[TaskManager] Created Master Application Task: %s", taskID)

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
	log.Printf("[TaskManager] starting child workflow with payload %v", payload)
	
	err = tm.layer2Manager.StartWorkflow(context.Background(), childWorkflowID, def, map[string]any{})
	if err != nil {
		return fmt.Errorf("failed to start child workflow: %v", err)
	}

	log.Printf("[TaskManager] Successfully started Child workflow: %s", childWorkflowID)

	return nil
}

// HandleLayer2Completion is called when ANY workflow completes.
// We check if it's a child workflow we launched, and if so, complete the corresponding parent task.
func (tm *TaskManager) HandleLayer2Completion(workflowID string, finalVariables map[string]any) error {
	record, exists := tm.db.GetTaskByChildWorkflowID(workflowID)
	if !exists {
		// Not a layer 2 workflow we launched, or DB lost it. Safe to ignore.
		return nil
	}

	log.Printf("[TaskManager] Detected completion of Layer 2 workflow %s for Task %s", workflowID, record.NodeID)

	// Update DB status and store the final outcome
	record.IsCompleted = true
	record.Inputs = finalVariables
	tm.db.SaveTask(record.NodeID, record)

	// Complete the parent task in Temporal
	err := tm.layer1Manager.TaskDone(context.Background(), record.ParentWorkflowID, record.RunID, record.NodeID, finalVariables)
	if err != nil {
		log.Printf("[TaskManager] Failed to complete parent task in Temporal: %v", err)
		return err
	}

	log.Printf("[TaskManager] Task %s marked as done in Parent Graph!", record.NodeID)
	return nil
}

// StartSubTask updates the existing master record with the current step's details.
func (tm *TaskManager) StartSubTask(payload engine.TaskPayload) error {
	// Find the master application record associated with this Child Workflow
	record, exists := tm.db.GetTaskByChildWorkflowID(payload.WorkflowID)
	if !exists {
		return fmt.Errorf("no master task found for child workflow %s", payload.WorkflowID)
	}

	// Update the existing record with the new node's template and merged inputs
	record.TaskTemplateID = payload.TaskTemplateID
	
	// Merge inputs provided by the engine (e.g. from previous nodes)
	if record.Inputs == nil {
		record.Inputs = make(map[string]any)
	}
	for k, v := range payload.Inputs {
		record.Inputs[k] = v
	}

	// Important: We use the master task's ID (ParentWorkflowID:NodeID) for persistence
	masterTaskID := fmt.Sprintf("%s:%s", record.ParentWorkflowID, record.NodeID)
	
	// SIMULATION: If this is the Reviewer Step, simulate an OUTBOUND API CALL
	if payload.TaskTemplateID == "reviewer_submission_form" {
		log.Printf("[TaskManager] >>> INTEGRATION: Pushing task %s to EXTERNAL Reviewer System via API...", masterTaskID)
		record.IntegrationStatus = "QUEUED_EXTERNALLY"
	} else {
		record.IntegrationStatus = ""
	}

	tm.db.SaveTask(masterTaskID, record)
	
	log.Printf("[TaskManager] Updated Master Task %s to Step: %s", masterTaskID, payload.TaskTemplateID)
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
