package store

import (
	"testing"
	"time"
)

// testStore implements store.TaskStore to make sure the interface methods compile and work correctly
type testStore struct {
	tasks map[string]TaskRecord
}

func (t *testStore) SaveTask(record TaskRecord) {
	t.tasks[record.TaskID] = record
}

func (t *testStore) GetTask(taskID string) (TaskRecord, bool) {
	record, ok := t.tasks[taskID]
	return record, ok
}

func (t *testStore) GetTaskByLayer2WorkflowID(layer2WorkflowID string) (TaskRecord, bool) {
	for _, record := range t.tasks {
		if record.Layer2WorkflowID == layer2WorkflowID {
			return record, true
		}
	}
	return TaskRecord{}, false
}

func (t *testStore) GetAllTasks() []TaskRecord {
	var list []TaskRecord
	for _, record := range t.tasks {
		list = append(list, record)
	}
	return list
}

func TestTaskStoreInterface(t *testing.T) {
	var store TaskStore = &testStore{tasks: make(map[string]TaskRecord)}

	record := TaskRecord{
		TaskID:           "test-1",
		TaskType:         "TEST",
		Status:           "PENDING_USER",
		Layer1WorkflowID: "l1-wf-1",
		Layer1RunID:      "l1-run-1",
		Layer1NodeID:     "node-1",
		Layer2WorkflowID: "l2-wf-1",
		Layer2RunID:      "l2-run-1",
		ActiveActivityID: "activity-1",
		Data:             map[string]any{"userform": map[string]any{"name": "Alice"}},
		CreatedAt:        time.Now(),
	}

	store.SaveTask(record)

	fetched, ok := store.GetTask("test-1")
	if !ok {
		t.Fatal("Expected task to be fetched")
	}

	if fetched.TaskType != "TEST" {
		t.Errorf("Expected TaskType 'TEST', got %s", fetched.TaskType)
	}

	fetchedL2, ok := store.GetTaskByLayer2WorkflowID("l2-wf-1")
	if !ok {
		t.Fatal("Expected task to be fetched by Layer 2 workflow ID")
	}
	if fetchedL2.TaskID != "test-1" {
		t.Errorf("Expected TaskID 'test-1', got %s", fetchedL2.TaskID)
	}

	all := store.GetAllTasks()
	if len(all) != 1 {
		t.Errorf("Expected exactly 1 task record, got %d", len(all))
	}
}
