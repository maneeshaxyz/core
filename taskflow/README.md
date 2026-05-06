# NSW Task Flow Engine

[![Go Reference](https://pkg.go.dev/badge/github.com/OpenNSW/nsw-task-flow.svg)](https://pkg.go.dev/github.com/OpenNSW/nsw-task-flow)
[![Go Test](https://github.com/OpenNSW/nsw-task-flow/actions/workflows/go.yml/badge.svg)](https://github.com/OpenNSW/nsw-task-flow/actions)

The **National Single Window (NSW) Task Flow Engine** is a modular, domain-driven, and highly decoupled task orchestration package backed by the [Temporal Workflow Engine](https://temporal.io). 

It is designed as a reusable Go package that developers can import to easily build National Single Window portals or any enterprise system requiring resilient, long-running macro workflows and micro interaction lifecycles.

---

## 🏛️ Architecture & Domain Model

The engine is built on a clean, decoupled hierarchy to completely separate high-level business flows from granular user-facing interaction flows:

```
              [Parent Workflow]  (Macro Business Journey)
                     │
                     ▼ (StartTask)
              [TaskManager] ────► [Task Record Created in Database]
                     │
                     ▼ (StartTaskWorkflow)
              [Task Workflow]    (Micro Interactive Journey)
                     │
                     ▼ (StartSubTask)
              [SubTask Node]     (e.g., PENDING_USER Status)
                     │
                     ▼ (CompleteTaskStep)
           [Resume SubTask & Continue]
                     │
                     ▼ (TaskWorkflow Completed)
           [HandleTaskCompletion]
                     │
                     ▼ (Callback)
              [Resume Parent Workflow]
```

### 🔑 Key Concepts
1. **Parent Workflow (Macro Journey)**:
   The high-level orchestrating workflow. It describes the top-level process (e.g., importing goods, getting phytosanitary approval). It has no awareness of individual UI forms or low-level review states.
2. **Task Workflow (Micro Journey / Micro-Flow)**:
   A self-contained sub-process representing a concrete block of work (e.g., Application Form Submission, Inspection evaluation). Tasks run as independent Temporal workflows under the hood.
3. **SubTask (Interaction Step)**:
   Individual steps inside a Task's workflow definition. It may pause asynchronously waiting for human interactions (like a user filling a form) or external APIs (like a payment gateway or third-party agency review).
4. **Unified DB Record (`TaskRecord`)**:
   Correlates the parent coordinate IDs, active task execution coordinates, form configuration IDs, and the namespaced payload data into a single, transactional record.

---

## 📦 Project Structure

```bash
├── orchestrator/          # Core task orchestration engine (public package)
│   ├── manager.go         # TaskManager - starts tasks, manages sub-tasks, completes steps
│   ├── manager_test.go    # Comprehensive unit and lifecycle test coverage
│   └── registry.go        # Registry of task template definitions (schemas, task types)
├── store/                 # Storage abstraction & domain models (public package)
│   ├── db.go              # TaskStore interface and TaskRecord struct
│   └── db_test.go         # Store verification test suite
├── demo/                  # Self-contained executable demo (developer playground)
│   ├── main.go            # Wire-up, Temporal configuration, and worker boots
│   ├── server.go          # HTTP API server routing to TaskManager
│   ├── db.go              # Simple file-backed, in-memory implementation of TaskStore
│   ├── task.json          # Micro-flow workflow definition (User Form -> External Review)
│   ├── workflow.json      # Macro-flow workflow definition
│   └── static/            # Demo web UI (split-panel portal view & forms)
```

---

## 🚀 Getting Started with the Demo

The repository includes a ready-to-use developer demo showcasing the **Split-Panel Portal View**. It simulates an applicant filling out a phytosanitary certificate application on the left, and a reviewer approving/rejecting it on the right in real time.

### Prerequisites
- [Go](https://golang.org/doc/install) 1.20+
- [Temporal CLI](https://docs.temporal.io/cli/) (local development server)

### 1. Run the Local Temporal Server
Open a new terminal window and run:
```bash
temporal server start-dev
```
*This starts a local development cluster. You can view the Temporal Web UI at [http://localhost:8233](http://localhost:8233).*

### 2. Run the Demo Server
In a separate terminal window, start the demo:
```bash
go run ./demo
```
This command:
1. Registers the Task Templates.
2. Boots the Layer 1 (Parent) and Layer 2 (Task) Temporal workers.
3. Spins up a web server on [http://localhost:8080](http://localhost:8080).

### 3. Open the Demo UI
Go to [http://localhost:8080](http://localhost:8080) in your web browser. 
- Click **"Start Workflow"** to kick off the Macro Journey.
- Experience the real-time split-screen interaction (Form submission ➡️ Reviewer evaluation ➡️ Parent Completion)!

---

## 🛠️ Usage as a Go Package

Import the packages to incorporate the Task Flow engine into your own projects:

```go
import (
	"context"
	
	"github.com/OpenNSW/nsw-task-flow/orchestrator"
	"github.com/OpenNSW/nsw-task-flow/store"
	engine "github.com/OpenNSW/go-temporal-workflow"
)

// 1. Initialize your Database Store
var db store.TaskStore = myDBImpl

// 2. Define a Task Registry
registry := orchestrator.NewTaskTemplateRegistry()
registry.Register(orchestrator.TaskTemplateEntry{
	TemplateID:          "phytosanitary_task",
	TaskType:            "APPLICATION",
	WorkflowID:          "phyto_task_v1", // The Task Workflow JSON file ID
	UserJsonFormsID:     "phyto_form",
	ReviewerJsonFormsID: "reviewer_form",
})

// 3. Define the Callback to Wake up the Parent Workflow
onTaskCompleted := func(parentWorkflowID string, parentRunID string, parentNodeID string, finalVariables map[string]any) error {
	return parentWorkflowManager.TaskDone(context.Background(), parentWorkflowID, parentRunID, parentNodeID, finalVariables)
}

// 4. Initialize the TaskManager
tm := orchestrator.NewTaskManager(db, registry, taskWorkflowManager, onTaskCompleted).
	WithTaskDefPath("path/to/task.json")

// 5. Connect TaskManager in your Temporal handlers
// When parent workflow triggers a task node:
parentTaskHandler := func(payload engine.TaskPayload) error {
	return tm.StartTask(payload)
}

// When the task workflow registers interaction nodes (e.g. generic_user_input):
taskHandler := func(payload engine.TaskPayload) error {
	return tm.StartSubTask(payload)
}

// When the task workflow completes:
taskCompletionHandler := func(workflowID string, finalVariables map[string]any) error {
	return tm.HandleTaskCompletion(workflowID, finalVariables)
}
```

---

## 🧪 Running Unit Tests

The core packages are backed by an extensive unit and integration test suite asserting database state transitions, callback invocation, intermediate state mutations, and type checks.

To execute tests with race-detection, run:
```bash
go test -v -race ./...
```
