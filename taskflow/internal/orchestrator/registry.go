package orchestrator

// TaskTemplateEntry defines how the TaskManager should initialize a task:
// which Layer 2 workflow to run and which JSONForms schemas to use for rendering.
type TaskTemplateEntry struct {
	TemplateID         string `json:"template_id"`
	TaskType           string `json:"task_type"`     // e.g. "APPLICATION"
	WorkflowID         string `json:"workflow_id"`   // Layer 2 workflow definition ID (maps to task.json)
	UserJsonFormsID    string `json:"user_jsonforms_id"`
	ReviewerJsonFormsID string `json:"reviewer_jsonforms_id"`
}

// TaskTemplateRegistry is a simple in-process registry mapping template IDs to their config.
type TaskTemplateRegistry struct {
	entries map[string]TaskTemplateEntry
}

// NewTaskTemplateRegistry creates a registry pre-populated with known task templates.
func NewTaskTemplateRegistry() *TaskTemplateRegistry {
	r := &TaskTemplateRegistry{entries: make(map[string]TaskTemplateEntry)}

	// Register the Phyto Application task template as described in the architecture doc.
	r.Register(TaskTemplateEntry{
		TemplateID:          "phyto_application_task",
		TaskType:            "APPLICATION",
		WorkflowID:          "Phyto_Application_Flow_v1", // matches task.json "id"
		UserJsonFormsID:     "phyto_user_form_v1",
		ReviewerJsonFormsID: "phyto_reviewer_form_v1",
	})

	return r
}

func (r *TaskTemplateRegistry) Register(entry TaskTemplateEntry) {
	r.entries[entry.TemplateID] = entry
}

func (r *TaskTemplateRegistry) Get(templateID string) (TaskTemplateEntry, bool) {
	entry, ok := r.entries[templateID]
	return entry, ok
}
