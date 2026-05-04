package main

import (
	"log"
	"os"
	"os/signal"
	"syscall"

	engine "github.com/OpenNSW/go-temporal-workflow"
	"github.com/OpenNSW/nsw-task-flow/internal/api"
	"github.com/OpenNSW/nsw-task-flow/internal/orchestrator"
	"github.com/OpenNSW/nsw-task-flow/internal/store"
	"go.temporal.io/sdk/client"
)

func main() {
	// 1. Create the Temporal client
	c, err := client.Dial(client.Options{
		HostPort: client.DefaultHostPort,
	})
	if err != nil {
		log.Fatalln("Unable to create Temporal client", err)
	}
	defer c.Close()

	// 2. Initialize store
	db := store.NewTaskDB()

	// 3. Setup Temporal Managers
	var tm *orchestrator.TaskManager

	layer1TaskHandler := func(payload engine.TaskPayload) error {
		log.Printf("\n[Layer 1 Engine] Task Activated! Forwarding to TaskManager... NodeID: %s\n", payload.NodeID)
		if tm != nil {
			return tm.StartTask(payload)
		}
		return nil
	}

	layer1CompletionHandler := func(workflowID string, finalVariables map[string]any) error {
		log.Printf("\n[Layer 1 Engine] Workflow %s Completed! Final state: %v\n", workflowID, finalVariables)
		return nil
	}

	layer1Manager := engine.NewTemporalManager(
		c,
		"nsw-layer1-queue",
		layer1TaskHandler,
		layer1CompletionHandler,
	)

	layer2TaskHandler := func(payload engine.TaskPayload) error {
		log.Printf("\n[Layer 2 Engine] Task Activated! NodeID: %s, Template: %s\n", payload.NodeID, payload.TaskTemplateID)
		if tm != nil {
			return tm.StartSubTask(payload)
		}
		return nil
	}

	layer2CompletionHandler := func(workflowID string, finalVariables map[string]any) error {
		log.Printf("\n[Layer 2 Engine] Workflow %s Completed! Final state: %v\n", workflowID, finalVariables)
		if tm != nil {
			return tm.HandleLayer2Completion(workflowID, finalVariables)
		}
		return nil
	}

	layer2Manager := engine.NewTemporalManager(
		c,
		"nsw-layer2-queue",
		layer2TaskHandler,
		layer2CompletionHandler,
	)

	// 4. Initialize Orchestrator & API Server
	tm = orchestrator.NewTaskManager(db, layer1Manager, layer2Manager)

	apiServer := api.NewServer(tm)
	apiServer.Start(":8080")

	// 5. Start Workers
	log.Println("Starting Layer 1 Temporal Worker...")
	if err := layer1Manager.StartWorker(); err != nil {
		log.Fatalln("Unable to start layer 1 worker", err)
	}
	defer layer1Manager.StopWorker()

	log.Println("Starting Layer 2 Temporal Worker...")
	if err := layer2Manager.StartWorker(); err != nil {
		log.Fatalln("Unable to start layer 2 worker", err)
	}
	defer layer2Manager.StopWorker()

	// Wait for interrupts
	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, syscall.SIGINT, syscall.SIGTERM)
	<-sigChan
	log.Println("Shutting down gracefully...")
}
