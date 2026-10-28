package main

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"os"
	"text/tabwriter"
	"time"

	"github.com/google/uuid"
	"github.com/spf13/cobra"

	"github.com/Shreyas9468/cube/node"
	"github.com/Shreyas9468/cube/task"
)

var (
	managerAddr string
	taskFile    string
	taskName    string
	taskImage   string
	taskCPU     float64
	taskMemory  int64
	taskDisk    int64
	taskRestart string
)

func main() {
	rootCmd := &cobra.Command{
		Use:   "cubectl",
		Short: "cubectl is the command-line interface for the Cube container orchestrator",
	}

	rootCmd.PersistentFlags().StringVarP(&managerAddr, "manager", "m", "localhost:5556", "Cube manager API host:port")

	// run command
	runCmd := &cobra.Command{
		Use:   "run",
		Short: "Submit and start a new container task",
		RunE:  runTask,
	}
	runCmd.Flags().StringVarP(&taskFile, "file", "f", "", "Path to JSON file defining the task")
	runCmd.Flags().StringVarP(&taskName, "name", "n", "", "Task name")
	runCmd.Flags().StringVarP(&taskImage, "image", "i", "", "Container image to run")
	runCmd.Flags().Float64Var(&taskCPU, "cpu", 0.25, "CPU request (cores)")
	runCmd.Flags().Int64Var(&taskMemory, "memory", 256*1024*1024, "Memory request in bytes")
	runCmd.Flags().Int64Var(&taskDisk, "disk", 1024*1024*1024, "Disk request in bytes")
	runCmd.Flags().StringVar(&taskRestart, "restart", task.RestartPolicyOnFailure, "Restart policy (always, on-failure, never)")

	// status / list command
	statusCmd := &cobra.Command{
		Use:     "status",
		Aliases: []string{"list", "ps", "tasks"},
		Short:   "List all cluster tasks and their current states",
		RunE:    listTasks,
	}

	// stop command
	stopCmd := &cobra.Command{
		Use:   "stop <task-id>",
		Short: "Stop and remove a running container task",
		Args:  cobra.ExactArgs(1),
		RunE:  stopTask,
	}

	// nodes command
	nodesCmd := &cobra.Command{
		Use:     "nodes",
		Aliases: []string{"workers"},
		Short:   "List registered worker nodes and resource telemetry",
		RunE:    listNodes,
	}

	// inspect command
	inspectCmd := &cobra.Command{
		Use:   "inspect <task-id>",
		Short: "Display detailed task metadata and execution status",
		Args:  cobra.ExactArgs(1),
		RunE:  inspectTask,
	}

	// logs command
	logsCmd := &cobra.Command{
		Use:   "logs <task-id>",
		Short: "Fetch and display container logs for a task",
		Args:  cobra.ExactArgs(1),
		RunE:  getTaskLogs,
	}

	rootCmd.AddCommand(runCmd, statusCmd, stopCmd, nodesCmd, inspectCmd, logsCmd)

	if err := rootCmd.Execute(); err != nil {
		fmt.Fprintf(os.Stderr, "Error: %v\n", err)
		os.Exit(1)
	}
}

func runTask(cmd *cobra.Command, args []string) error {
	var t task.Task

	if taskFile != "" {
		data, err := os.ReadFile(taskFile)
		if err != nil {
			return fmt.Errorf("unable to read task file: %w", err)
		}
		if err := json.Unmarshal(data, &t); err != nil {
			return fmt.Errorf("unable to parse task JSON: %w", err)
		}
	} else {
		if taskName == "" || taskImage == "" {
			return fmt.Errorf("both --name and --image are required when --file is not provided")
		}
		t = task.Task{
			Name:          taskName,
			Image:         taskImage,
			CPU:           taskCPU,
			Memory:        taskMemory,
			Disk:          taskDisk,
			RestartPolicy: taskRestart,
		}
	}

	if t.ID == uuid.Nil {
		t.ID = uuid.New()
	}
	t.State = task.Scheduled

	event := task.TaskEvent{
		ID:        uuid.New(),
		State:     task.Scheduled,
		Timestamp: time.Now().UTC(),
		Task:      t,
	}

	body, err := json.Marshal(event)
	if err != nil {
		return fmt.Errorf("failed to encode task event: %w", err)
	}

	resp, err := http.Post(fmt.Sprintf("http://%s/tasks", managerAddr), "application/json", bytes.NewBuffer(body))
	if err != nil {
		return fmt.Errorf("failed to contact manager at %s: %w", managerAddr, err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusCreated && resp.StatusCode != http.StatusOK {
		respBody, _ := io.ReadAll(resp.Body)
		return fmt.Errorf("manager returned error (%d): %s", resp.StatusCode, string(respBody))
	}

	fmt.Printf("Task %s (%s) successfully submitted to cluster.\n", t.ID, t.Name)
	return nil
}

func listTasks(cmd *cobra.Command, args []string) error {
	resp, err := http.Get(fmt.Sprintf("http://%s/tasks", managerAddr))
	if err != nil {
		return fmt.Errorf("failed to contact manager at %s: %w", managerAddr, err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("unexpected status from manager: %d", resp.StatusCode)
	}

	var tasks []*task.Task
	if err := json.NewDecoder(resp.Body).Decode(&tasks); err != nil {
		return fmt.Errorf("failed to decode response: %w", err)
	}

	if len(tasks) == 0 {
		fmt.Println("No tasks currently scheduled in cluster.")
		return nil
	}

	w := tabwriter.NewWriter(os.Stdout, 0, 8, 2, ' ', 0)
	fmt.Fprintln(w, "TASK ID\tNAME\tIMAGE\tSTATE\tRESTARTS\tCONTAINER\tSTARTED")
	for _, t := range tasks {
		started := "-"
		if !t.StartTime.IsZero() {
			started = t.StartTime.Format("15:04:05")
		}
		cid := "-"
		if len(t.ContainerID) > 12 {
			cid = t.ContainerID[:12]
		} else if t.ContainerID != "" {
			cid = t.ContainerID
		}
		fmt.Fprintf(w, "%s\t%s\t%s\t%s\t%d\t%s\t%s\n",
			t.ID.String()[:8], t.Name, t.Image, stateToString(t.State), t.RestartCount, cid, started)
	}
	return w.Flush()
}

func stopTask(cmd *cobra.Command, args []string) error {
	taskID := args[0]
	req, err := http.NewRequest(http.MethodDelete, fmt.Sprintf("http://%s/tasks/%s", managerAddr, taskID), nil)
	if err != nil {
		return fmt.Errorf("failed to create delete request: %w", err)
	}

	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return fmt.Errorf("failed to contact manager at %s: %w", managerAddr, err)
	}
	defer resp.Body.Close()

	if resp.StatusCode == http.StatusNotFound {
		return fmt.Errorf("task %s not found in cluster", taskID)
	}
	if resp.StatusCode != http.StatusNoContent && resp.StatusCode != http.StatusOK {
		return fmt.Errorf("manager returned status %d", resp.StatusCode)
	}

	fmt.Printf("Stop signal successfully sent for task %s.\n", taskID)
	return nil
}

func listNodes(cmd *cobra.Command, args []string) error {
	resp, err := http.Get(fmt.Sprintf("http://%s/nodes", managerAddr))
	if err != nil {
		return fmt.Errorf("failed to contact manager at %s: %w", managerAddr, err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("unexpected status from manager: %d", resp.StatusCode)
	}

	var nodes []*node.Node
	if err := json.NewDecoder(resp.Body).Decode(&nodes); err != nil {
		return fmt.Errorf("failed to decode response: %w", err)
	}

	if len(nodes) == 0 {
		fmt.Println("No worker nodes currently registered in cluster.")
		return nil
	}

	w := tabwriter.NewWriter(os.Stdout, 0, 8, 2, ' ', 0)
	fmt.Fprintln(w, "NODE NAME\tROLE\tSTATUS\tTASKS\tMEMORY (TOTAL)\tDISK (TOTAL)")
	for _, n := range nodes {
		status := "Healthy"
		if !n.Healthy {
			status = "Unhealthy"
		}
		memStr := fmt.Sprintf("%d MB", n.Memory/(1024*1024))
		diskStr := fmt.Sprintf("%d GB", n.Disk/(1024*1024*1024))
		fmt.Fprintf(w, "%s\t%s\t%s\t%d\t%s\t%s\n",
			n.Name, n.Role, status, n.TaskCount, memStr, diskStr)
	}
	return w.Flush()
}

func stateToString(s task.State) string {
	switch s {
	case task.Pending:
		return "Pending"
	case task.Scheduled:
		return "Scheduled"
	case task.Running:
		return "Running"
	case task.Completed:
		return "Completed"
	case task.Failed:
		return "Failed"
	default:
		return "Unknown"
	}
}

func inspectTask(cmd *cobra.Command, args []string) error {
	taskID := args[0]
	resp, err := http.Get(fmt.Sprintf("http://%s/tasks/%s", managerAddr, taskID))
	if err != nil {
		return fmt.Errorf("failed to contact manager at %s: %w", managerAddr, err)
	}
	defer resp.Body.Close()

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return fmt.Errorf("failed to read response: %w", err)
	}

	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("manager returned %d: %s", resp.StatusCode, string(body))
	}

	var pretty bytes.Buffer
	if err := json.Indent(&pretty, body, "", "  "); err != nil {
		fmt.Println(string(body))
		return nil
	}
	fmt.Println(pretty.String())
	return nil
}

func getTaskLogs(cmd *cobra.Command, args []string) error {
	taskID := args[0]
	resp, err := http.Get(fmt.Sprintf("http://%s/tasks/%s/logs", managerAddr, taskID))
	if err != nil {
		return fmt.Errorf("failed to contact manager at %s: %w", managerAddr, err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(resp.Body)
		return fmt.Errorf("manager returned %d: %s", resp.StatusCode, string(body))
	}

	_, err = io.Copy(os.Stdout, resp.Body)
	return err
}
