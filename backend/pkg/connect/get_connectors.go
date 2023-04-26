// Copyright 2022 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file https://github.com/redpanda-data/redpanda/blob/dev/licenses/bsl.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

package connect

import (
	"context"
	"fmt"
	"net/http"
	"strings"

	"github.com/cloudhut/common/rest"
	con "github.com/cloudhut/connect-client"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"

	"github.com/redpanda-data/console/backend/pkg/config"
)

type connectorState = string

const (
	connectorStateUnassigned connectorState = "UNASSIGNED"
	connectorStateRunning    connectorState = "RUNNING"
	connectorStatePaused     connectorState = "PAUSED"
	connectorStateFailed     connectorState = "FAILED"
	connectorStateRestarting connectorState = "RESTARTING"
	connectorStateDestroyed  connectorState = "DESTROYED"
)

// connectorStatus is our holistic unified connector status that takes into account not just the
// connector instance state, but also state of all the tasks within the connector
type connectorStatus = string

const (
	connectorStatusHealthy    connectorStatus = "HEALTHY"
	connectorStatusUnhealthy  connectorStatus = "UNHEALTHY"
	connectorStatusDegraded   connectorStatus = "DEGRADED"
	connectorStatusPaused     connectorStatus = "PAUSED"
	connectorStatusRestarting connectorStatus = "RESTARTING"
	connectorStatusUnassigned connectorStatus = "UNASSIGNED"
	connectorStatusDestroyed  connectorStatus = "DESTROYED"
	connectorStatusUnknown    connectorStatus = "UNKNOWN"
)

// ClusterConnectors contains all available information about the deployed connectors
// in a single Kafka connect cluster.
type ClusterConnectors struct {
	ClusterName    string           `json:"clusterName"`
	ClusterAddress string           `json:"clusterAddress"`
	ClusterInfo    con.RootResource `json:"clusterInfo"`

	TotalConnectors   int                    `json:"totalConnectors"`
	RunningConnectors int                    `json:"runningConnectors"`
	Connectors        []ClusterConnectorInfo `json:"connectors"`
	Error             string                 `json:"error,omitempty"`

	// This is set at the HTTP handler level as this will be returned by the Hooks.
	AllowedActions []string `json:"allowedActions"`
}

// ClusterConnectorInfo contains all information we can retrieve about a single
// connector in a Kafka connect cluster.
type ClusterConnectorInfo struct {
	Name         string                      `json:"name"`
	Class        string                      `json:"class"`
	Config       map[string]string           `json:"config"`
	Type         string                      `json:"type"`  // Source or Sink
	Topic        string                      `json:"topic"` // Kafka Topic name
	State        connectorState              `json:"state"` // Running, ..
	Status       connectorStatus             `json:"status"`
	TotalTasks   int                         `json:"totalTasks"`
	RunningTasks int                         `json:"runningTasks"`
	Trace        string                      `json:"trace,omitempty"`
	Errors       []ClusterConnectorInfoError `json:"errors"`
	Tasks        []ClusterConnectorTaskInfo  `json:"tasks"`
}

type connectorErrorType = string

const (
	connectorErrorTypeError   = "ERROR"
	connectorErrorTypeWarning = "WARNING"
)

// ClusterConnectorInfoError provides nicer information about connector errors gathered from connector traces
type ClusterConnectorInfoError struct {
	Type    connectorErrorType `json:"type"`
	Title   string             `json:"title"`
	Content string             `json:"content"`
}

// ClusterConnectorTaskInfo provides information about a connector's task.
type ClusterConnectorTaskInfo struct {
	TaskID   int    `json:"taskId"`
	State    string `json:"state"`
	WorkerID string `json:"workerId"`
	Trace    string `json:"trace,omitempty"` // only set if the task is errored
}

// GetAllClusterConnectors returns the merged GET /connectors responses across all configured Connect clusters. Requests will be
// sent concurrently. Context timeout should be configured correctly in order to not await responses from offline clusters
// for too long.
func (s *Service) GetAllClusterConnectors(ctx context.Context) ([]*ClusterConnectors, error) {
	if !s.Cfg.Enabled {
		return nil, ErrKafkaConnectNotConfigured
	}

	ch := make(chan *ClusterConnectors, len(s.ClientsByCluster))
	for _, cluster := range s.ClientsByCluster {
		go func(cfg config.ConnectCluster, c *con.Client) {
			connectors, err := c.ListConnectorsExpanded(ctx)
			errMsg := ""
			if err != nil {
				s.Logger.Warn("failed to list connectors from Kafka connect cluster",
					zap.String("cluster_name", cfg.Name), zap.String("cluster_address", cfg.URL), zap.Error(err))
				errMsg = err.Error()

				ch <- &ClusterConnectors{
					ClusterName:    cfg.Name,
					ClusterAddress: cfg.URL,
					Connectors:     listConnectorsExpandedToClusterConnectorInfo(connectors),
					Error:          errMsg,
				}
				return
			}

			root, err := c.GetRoot(ctx)
			if err != nil {
				s.Logger.Warn("failed to list root resource from Kafka connect cluster",
					zap.String("cluster_name", cfg.Name), zap.String("cluster_address", cfg.URL), zap.Error(err))
				errMsg = err.Error()
			}

			totalConnectors := 0
			runningConnectors := 0
			for _, connector := range connectors {
				totalConnectors++
				if connector.Status.Connector.State == connectorStateRunning {
					runningConnectors++
				}
			}

			ch <- &ClusterConnectors{
				ClusterName:       cfg.Name,
				ClusterAddress:    cfg.URL,
				ClusterInfo:       root,
				TotalConnectors:   totalConnectors,
				RunningConnectors: runningConnectors,
				Connectors:        listConnectorsExpandedToClusterConnectorInfo(connectors),
				Error:             errMsg,
			}
		}(cluster.Cfg, cluster.Client)
	}

	// Consume all list connector responses and merge them into a single array.
	shards := make([]*ClusterConnectors, cap(ch))
	for i := 0; i < cap(ch); i++ {
		shards[i] = <-ch
	}
	return shards, nil
}

// GetClusterConnectors returns the GET /connectors response for a single connect cluster. A cluster can be referenced
// by it's name (as specified in the user config).
func (s *Service) GetClusterConnectors(ctx context.Context, clusterName string) (ClusterConnectors, *rest.Error) {
	c, restErr := s.getConnectClusterByName(clusterName)
	if restErr != nil {
		return ClusterConnectors{}, restErr
	}

	connectors, err := c.Client.ListConnectorsExpanded(ctx)
	errMsg := ""
	if err != nil {
		s.Logger.Warn("failed to list connectors from Kafka connect cluster",
			zap.String("cluster_name", c.Cfg.Name), zap.String("cluster_address", c.Cfg.URL), zap.Error(err))
		errMsg = err.Error()
	}

	return ClusterConnectors{
		ClusterName:    c.Cfg.Name,
		ClusterAddress: c.Cfg.URL,
		Connectors:     listConnectorsExpandedToClusterConnectorInfo(connectors),
		Error:          errMsg,
	}, nil
}

// GetConnector requests the connector info as well as the status info and merges both information together. If either
// request fails an error will be returned.
func (s *Service) GetConnector(ctx context.Context, clusterName string, connector string) (ClusterConnectorInfo, *rest.Error) {
	c, restErr := s.getConnectClusterByName(clusterName)
	if restErr != nil {
		return ClusterConnectorInfo{}, restErr
	}

	cInfo, err := c.Client.GetConnector(ctx, connector)
	if err != nil {
		return ClusterConnectorInfo{}, &rest.Error{
			Err:          err,
			Status:       http.StatusServiceUnavailable,
			Message:      fmt.Sprintf("Failed to get connector info: %v", err.Error()),
			InternalLogs: []zapcore.Field{zap.String("cluster_name", clusterName), zap.String("connector", connector)},
			IsSilent:     false,
		}
	}

	stateInfo, err := c.Client.GetConnectorStatus(ctx, connector)
	if err != nil {
		return ClusterConnectorInfo{}, &rest.Error{
			Err:          err,
			Status:       http.StatusServiceUnavailable,
			Message:      fmt.Sprintf("Failed to get connector state: %v", err.Error()),
			InternalLogs: []zapcore.Field{zap.String("cluster_name", clusterName), zap.String("connector", connector)},
			IsSilent:     false,
		}
	}

	tasks := make([]ClusterConnectorTaskInfo, len(stateInfo.Tasks))
	runningTasks := 0
	for i, task := range stateInfo.Tasks {
		tasks[i] = ClusterConnectorTaskInfo{
			TaskID:   task.ID,
			State:    task.State,
			WorkerID: task.WorkerID,
			Trace:    task.Trace,
		}
		if task.State == connectorStateRunning {
			runningTasks++
		}
	}

	return ClusterConnectorInfo{
		Name:         cInfo.Name,
		Class:        getMapValueOrString(cInfo.Config, "connector.class", "unknown"),
		Config:       cInfo.Config,
		Type:         cInfo.Type,
		State:        stateInfo.Connector.State,
		Topic:        getMapValueOrString(cInfo.Config, "kafka.topic", "unknown"),
		TotalTasks:   len(stateInfo.Tasks),
		RunningTasks: runningTasks,
		Tasks:        tasks,
	}, nil
}

func listConnectorsExpandedToClusterConnectorInfo(l map[string]con.ListConnectorsResponseExpanded) []ClusterConnectorInfo {
	if l == nil {
		return []ClusterConnectorInfo{}
	}

	connectorInfo := make([]ClusterConnectorInfo, 0, len(l))
	for _, c := range l {
		c := c
		cInfo := connectorsResponseToClusterConnectorInfo(&c)
		connectorInfo = append(connectorInfo, *cInfo)
	}

	return connectorInfo
}

//nolint:gocognit,cyclop,gocyclo // lots of inspection of state and tasks to determine status and errors
func connectorsResponseToClusterConnectorInfo(c *con.ListConnectorsResponseExpanded) *ClusterConnectorInfo {
	totalTasks := len(c.Status.Tasks)
	tasks := make([]ClusterConnectorTaskInfo, totalTasks)
	connectorTaskErrors := make([]ClusterConnectorInfoError, 0, totalTasks)

	runningTasks := 0
	failedTasks := 0
	pausedTasks := 0
	restartingTasks := 0
	for i, task := range c.Status.Tasks {
		tasks[i] = ClusterConnectorTaskInfo{
			TaskID:   task.ID,
			State:    task.State,
			WorkerID: task.WorkerID,
			Trace:    task.Trace,
		}

		switch task.State {
		case connectorStateRunning:
			runningTasks++
		case connectorStateFailed:
			failedTasks++

			errTitle := fmt.Sprintf("Connector %s Task %d is in failed state.", c.Info.Name, task.ID)
			connectorTaskErrors = append(connectorTaskErrors, ClusterConnectorInfoError{
				Type:    connectorErrorTypeError,
				Title:   errTitle,
				Content: traceToErrorContent(errTitle, task.Trace),
			})
		case connectorStatePaused:
			pausedTasks++
		case connectorStateRestarting:
			restartingTasks++
		}
	}

	// LOGIC:
	// HEALTHY: Connector is in running state, > 0 tasks, all of them in running state.
	// UNHEALTHY: Connector is failed state.
	//			Or Connector is in running state but has 0 tasks.
	// 			Or Connector is in running state, has > 0 tasks, and all tasks are in failed state.
	// DEGRADED: Connector is in running state, has > 0 tasks, but has at least one state in failed state, but not all tasks are failed.
	// PAUSED: Connector is in paused state, regardless of individual tasks' states.
	// RESTARTING: Connector is in restarting state, or at least one task is in restarting state.
	// UNASSIGNED: Connector is in unassigned state, regardless of any tasks.
	// DESTROYED: Connector is in destroyed state, regardless of any tasks.
	// UNKNOWN: Any other scenario.
	var connStatus connectorStatus
	var errDetailedContent string
	//nolint:gocritic // this if else is easier to read as they map to rules and logic specified above.
	if (c.Status.Connector.State == connectorStateRunning) &&
		totalTasks > 0 && runningTasks == totalTasks {
		connStatus = connectorStatusHealthy
	} else if (c.Status.Connector.State == connectorStateFailed) ||
		((c.Status.Connector.State == connectorStateRunning) && (totalTasks == 0 || totalTasks == failedTasks)) {
		connStatus = connectorStatusUnhealthy

		if c.Status.Connector.State == connectorStateFailed {
			errDetailedContent = "Connector " + c.Info.Name + " is in failed state."
		} else if totalTasks == 0 {
			errDetailedContent = "Connector " + c.Info.Name + " is in " + strings.ToLower(c.Status.Connector.State) + " state but has no tasks."
		} else if totalTasks == failedTasks {
			errDetailedContent = "Connector " + c.Info.Name + " is in " + strings.ToLower(c.Status.Connector.State) + " state. All tasks are in failed state."
		}
	} else if (c.Status.Connector.State == connectorStateRunning) && (totalTasks > 0 && failedTasks > 0 && failedTasks < totalTasks) {
		connStatus = connectorStatusDegraded
		errDetailedContent = fmt.Sprintf("Connector %s is in %s state but has %d / %d failed tasks.",
			c.Info.Name, strings.ToLower(c.Status.Connector.State), failedTasks, totalTasks)
	} else if c.Status.Connector.State == connectorStatePaused {
		connStatus = connectorStatusPaused
	} else if (c.Status.Connector.State == connectorStateRestarting) ||
		(totalTasks > 0 && restartingTasks > 0) {
		connStatus = connectorStatusRestarting
	} else if c.Status.Connector.State == connectorStateUnassigned {
		connStatus = connectorStatusUnassigned
	} else if c.Status.Connector.State == connectorStateDestroyed {
		connStatus = connectorStatusDestroyed
	} else {
		connStatus = connectorStatusUnknown
		errDetailedContent = fmt.Sprintf("Unknown connector status. Connector %s is in %s state.",
			c.Info.Name, strings.ToLower(c.Status.Connector.State))
	}

	connectorErrors := make([]ClusterConnectorInfoError, 0)
	if connStatus == connectorStatusUnhealthy ||
		connStatus == connectorStatusDegraded {
		stateStr := "unhealthy"
		if connStatus == connectorStatusDegraded {
			stateStr = "degraded"
		}

		errTitle := "Connector " + c.Info.Name + " is in " + stateStr + " state."

		defaultContent := errTitle
		if errDetailedContent != "" {
			defaultContent = errDetailedContent
		}

		connectorErrors = append(connectorErrors, ClusterConnectorInfoError{
			Type:    connectorErrorTypeError,
			Title:   errTitle,
			Content: traceToErrorContent(defaultContent, c.Status.Connector.Trace),
		})
	} else if len(c.Status.Connector.Trace) > 0 {
		errTitle := "Connector " + c.Info.Name + " has an error"
		connectorErrors = append(connectorErrors, ClusterConnectorInfoError{
			Type:    connectorErrorTypeError,
			Title:   errTitle,
			Content: traceToErrorContent(errTitle, c.Status.Connector.Trace),
		})
	}

	connectorErrors = append(connectorErrors, connectorTaskErrors...)

	return &ClusterConnectorInfo{
		Name:         c.Info.Name,
		Class:        getMapValueOrString(c.Info.Config, "connector.class", "unknown"),
		Topic:        getMapValueOrString(c.Info.Config, "kafka.topic", "unknown"),
		Config:       c.Info.Config,
		Type:         c.Info.Type,
		State:        c.Status.Connector.State,
		Status:       connStatus,
		Tasks:        tasks,
		Trace:        c.Status.Connector.Trace,
		Errors:       connectorErrors,
		TotalTasks:   len(c.Status.Tasks),
		RunningTasks: runningTasks,
	}
}

// traceToErrorContent takes two parameters: a defaultValue string and a trace string.
// It returns a error message optimized for humans that is the sanitized trace line or the
// provided default value if no trace line is found.
func traceToErrorContent(defaultValue, trace string) string {
	if trace == "" {
		return defaultValue
	}

	lines := strings.Split(trace, "\n")
	filtered := make([]string, 0, len(lines))
	for _, l := range lines {
		l := strings.Trim(l, "\t")
		if !strings.HasSuffix(l, "exception") && !strings.HasSuffix(l, ")") {
			filtered = append(filtered, l)
		}
	}

	// some lines have 'caused by' prefix which has description
	for _, l := range filtered {
		if strings.HasPrefix(strings.ToLower(l), "caused by") {
			return sanitizeTraceLine(l)
		}
	}

	if len(filtered) > 0 {
		return sanitizeTraceLine(filtered[0])
	}

	return defaultValue
}

func sanitizeTraceLine(l string) string {
	lower := strings.ToLower(l)
	if strings.HasPrefix(lower, "caused by:") {
		l = strings.TrimPrefix(l, "caused by:")
		l = strings.TrimPrefix(l, "Caused by:")
	}

	var content string
	if strings.Index(l, ":") > 0 {
		content = l[strings.Index(l, ":")+1:]
	}

	content = strings.TrimSpace(content)

	return content
}
