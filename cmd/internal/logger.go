package internal

import (
	"encoding/json"
	"fmt"
	"io"
	"os"
	"strings"
	"time"
)

type AirbyteLogger interface {
	Log(level, message string)
	Catalog(catalog Catalog)
	ConnectionStatus(status ConnectionStatus)
	Record(tableNamespace, tableName string, data map[string]interface{})
	Flush()
	StreamState(streamName, namespace string, shardStates ShardStates) // Stream state method
	GlobalState(sharedState map[string]interface{}, streamStates map[string]ShardStates) // Global state method
	StreamTrace(streamName, namespace, status string) // Stream status trace method
	Error(error string)
}

const MaxBatchSize = 1

func NewLogger(w io.Writer) AirbyteLogger {
	al := airbyteLogger{}
	al.writer = w
	al.recordEncoder = json.NewEncoder(w)
	al.records = make([]AirbyteMessage, 0, MaxBatchSize)
	return &al
}

type airbyteLogger struct {
	recordEncoder *json.Encoder
	writer        io.Writer
	records       []AirbyteMessage
}

func (a *airbyteLogger) Log(level, message string) {
	if err := a.recordEncoder.Encode(AirbyteMessage{
		Type: LOG,
		Log: &AirbyteLogMessage{
			Level:   level,
			Message: preamble() + message,
		},
	}); err != nil {
		fmt.Fprintf(os.Stderr, "%sFailed to write log message: %v", preamble(), err)
	}
}

func (a *airbyteLogger) Catalog(catalog Catalog) {
	if err := a.recordEncoder.Encode(AirbyteMessage{
		Type:    CATALOG,
		Catalog: &catalog,
	}); err != nil {
		a.Error(fmt.Sprintf("catalog encoding error: %v", err))
	}
}

func (a *airbyteLogger) Record(tableNamespace, tableName string, data map[string]interface{}) {
	now := time.Now()
	amsg := AirbyteMessage{
		Type: RECORD,
		Record: &AirbyteRecord{
			Namespace: tableNamespace,
			Stream:    tableName,
			Data:      data,
			EmittedAt: now.UnixMilli(),
		},
	}

	a.records = append(a.records, amsg)
	if len(a.records) == MaxBatchSize {
		a.Flush()
	}
}

func (a *airbyteLogger) Flush() {
	for _, record := range a.records {
		if err := a.recordEncoder.Encode(record); err != nil {
			a.Error(fmt.Sprintf("flush encoding error: %v", err))
		}
	}
	a.records = a.records[:0]
}

func (a *airbyteLogger) StreamState(streamName, namespace string, shardStates ShardStates) {
	// Create stream descriptor
	streamDescriptor := StreamDescriptor{
		Name: streamName,
	}
	if namespace != "" {
		streamDescriptor.Namespace = &namespace
	}
	
	// Create stream state
	streamState := &AirbyteStreamState{
		StreamDescriptor: streamDescriptor,
		StreamState:      &shardStates,
	}
	
	if err := a.recordEncoder.Encode(AirbyteMessage{
		Type:  STATE,
		State: &AirbyteState{StateType: STATE_TYPE_STREAM, Stream: streamState},
	}); err != nil {
		a.Error(fmt.Sprintf("stream state encoding error: %v", err))
	}
}

func (a *airbyteLogger) GlobalState(sharedState map[string]interface{}, streamStates map[string]ShardStates) {
	// Convert streamStates map to slice of AirbyteStreamState
	globalStreamStates := make([]AirbyteStreamState, 0, len(streamStates))
	
	for stateKey, shardStates := range streamStates {
		// Parse the state key to extract namespace and stream name
		parts := strings.SplitN(stateKey, ":", 2)
		var namespace, streamName string
		if len(parts) == 2 {
			namespace = parts[0]
			streamName = parts[1]
		} else {
			streamName = stateKey
		}
		
		// Create stream descriptor
		streamDescriptor := StreamDescriptor{
			Name: streamName,
		}
		if namespace != "" {
			streamDescriptor.Namespace = &namespace
		}
		
		// Add to global stream states
		globalStreamStates = append(globalStreamStates, AirbyteStreamState{
			StreamDescriptor: streamDescriptor,
			StreamState:      &shardStates,
		})
	}
	
	// Create global state
	globalState := &AirbyteGlobalState{
		SharedState:  sharedState,
		StreamStates: globalStreamStates,
	}
	
	if err := a.recordEncoder.Encode(AirbyteMessage{
		Type:  STATE,
		State: &AirbyteState{StateType: STATE_TYPE_GLOBAL, Global: globalState},
	}); err != nil {
		a.Error(fmt.Sprintf("global state encoding error: %v", err))
	}
}

func (a *airbyteLogger) StreamTrace(streamName, namespace, status string) {
	// Create stream descriptor
	streamDescriptor := StreamDescriptor{
		Name: streamName,
	}
	if namespace != "" {
		streamDescriptor.Namespace = &namespace
	}
	
	// Create stream status
	streamStatus := &AirbyteStreamStatus{
		StreamDescriptor: streamDescriptor,
		Status:           status,
	}
	
	if err := a.recordEncoder.Encode(AirbyteMessage{
		Type:  TRACE,
		Trace: &AirbyteTraceMessage{
			Type:      TRACE_TYPE_STREAM,
			EmittedAt: time.Now().UnixMilli(),
			Stream:    streamStatus,
		},
	}); err != nil {
		a.Error(fmt.Sprintf("stream trace encoding error: %v", err))
	}
}

func (a *airbyteLogger) Error(error string) {
	if err := a.recordEncoder.Encode(AirbyteMessage{
		Type: LOG,
		Log: &AirbyteLogMessage{
			Level:   LOGLEVEL_ERROR,
			Message: error,
		},
	}); err != nil {
		fmt.Fprintf(os.Stderr, "%sFailed to write error: %v", preamble(), err)
	}
}

func (a *airbyteLogger) ConnectionStatus(status ConnectionStatus) {
	if err := a.recordEncoder.Encode(AirbyteMessage{
		Type:             CONNECTION_STATUS,
		ConnectionStatus: &status,
	}); err != nil {
		a.Error(fmt.Sprintf("connection status encoding error: %v", err))
	}
}

func preamble() string {
	return "PlanetScale Source :: "
}
