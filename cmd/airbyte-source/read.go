package airbyte_source

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"strings"

	"github.com/planetscale/airbyte-source/cmd/internal"
	psdbconnectv1alpha1 "github.com/planetscale/airbyte-source/proto/psdbconnect/v1alpha1"
	"github.com/spf13/cobra"
)

var (
	readSourceConfigFilePath string
	readSourceCatalogPath    string
	stateFilePath            string
)

func init() {
	rootCmd.AddCommand(ReadCommand(DefaultHelper(os.Stdout)))
}

func ReadCommand(ch *Helper) *cobra.Command {
	readCmd := &cobra.Command{
		Use:   "read",
		Short: "Converts rows from a PlanetScale database into AirbyteRecordMessages",
		Run: func(cmd *cobra.Command, args []string) {
			ch.Logger = internal.NewLogger(cmd.OutOrStdout())
			defer ch.Logger.Flush()
			if readSourceConfigFilePath == "" {
				fmt.Fprintf(cmd.ErrOrStderr(), "Please pass path to a valid source config file via the [%v] argument", "config")
				os.Exit(1)
			}
			
			if readSourceCatalogPath == "" {
				fmt.Fprintf(cmd.OutOrStdout(), "Please pass path to a valid source catalog file via the [%v] argument", "config")
				os.Exit(1)
			}

			ch.Logger.Log(internal.LOGLEVEL_INFO, "Checking connection")

			psc, err := parseSource(ch.FileReader, readSourceConfigFilePath)
			if err != nil {
				fmt.Fprintln(cmd.OutOrStdout(), "Please provide path to a valid configuration file")
				return
			}

			if err := ch.EnsureDB(psc); err != nil {
				fmt.Fprintln(cmd.OutOrStdout(), "Unable to connect to PlanetScale Database")
				return
			}

			defer func() {
				if err := ch.Database.Close(); err != nil {
					fmt.Fprintf(cmd.OutOrStdout(), "Unable to close connection to PlanetScale Database, failed with %v", err)
				}
			}()

			cs, err := checkConnectionStatus(ch.Database, psc)
			if err != nil {
				ch.Logger.ConnectionStatus(cs)
				return
			}

			catalog, err := readCatalog(readSourceCatalogPath)
			if err != nil {
				ch.Logger.Error(fmt.Sprintf("Unable to read catalog: %+v", err))
				os.Exit(1)
			}

			if len(catalog.Streams) == 0 {
				ch.Logger.Log(internal.LOGLEVEL_ERROR, "Catalog has no streams")
				return
			}

			state := ""
			if stateFilePath != "" {
				ch.Logger.Log(internal.LOGLEVEL_INFO, fmt.Sprintf("State file detected, parsing provided file %s", stateFilePath))
				b, err := os.ReadFile(stateFilePath)
				if err != nil {
					ch.Logger.Error(fmt.Sprintf("Unable to read state : %v", err))
					os.Exit(1)
				}
				state = string(b)
				ch.Logger.Log(internal.LOGLEVEL_INFO, fmt.Sprintf("State value: %s", state))
			}
			shards, err := ch.Database.ListShards(context.Background(), psc)
			if err != nil {
				ch.Logger.Error(fmt.Sprintf("Unable to list shards : %v", err))
				os.Exit(1)
			}

			syncState, err := readState(state, psc, catalog.Streams, shards, ch.Logger)
			if err != nil {
				ch.Logger.Error(fmt.Sprintf("Unable to read state : %v", err))
				os.Exit(1)
			}

			// Log initial sync state configuration
			ch.Logger.Log(internal.LOGLEVEL_INFO, "=== SYNC STATE CONFIGURATION ===")
			ch.Logger.Log(internal.LOGLEVEL_INFO, fmt.Sprintf("Configured state type: %s", psc.GetStateType()))
			ch.Logger.Log(internal.LOGLEVEL_INFO, fmt.Sprintf("Number of streams to sync: %d", len(catalog.Streams)))
			ch.Logger.Log(internal.LOGLEVEL_INFO, fmt.Sprintf("Initial sync state contains %d stream(s)", len(syncState.Streams)))
			for streamKey, streamState := range syncState.Streams {
				ch.Logger.Log(internal.LOGLEVEL_INFO, fmt.Sprintf("Stream %s has %d shard(s)", streamKey, len(streamState.Shards)))
			}

			for _, table := range catalog.Streams {
				keyspaceOrDatabase := table.Stream.Namespace
				if keyspaceOrDatabase == "" {
					keyspaceOrDatabase = psc.Database
				}
				streamStateKey := keyspaceOrDatabase + ":" + table.Stream.Name
				streamState, ok := syncState.Streams[streamStateKey]
				if !ok {
					ch.Logger.Error(fmt.Sprintf("Unable to read state for stream %v", streamStateKey))
					os.Exit(1)
				}

				// Emit STARTED trace message
				ch.Logger.StreamTrace(table.Stream.Name, keyspaceOrDatabase, internal.STREAM_STATUS_STARTED)

				for shardName, shardState := range streamState.Shards {
					var tc *psdbconnectv1alpha1.TableCursor

					tc, err = shardState.SerializedCursorToTableCursor(table)
					ch.Logger.Log(internal.LOGLEVEL_INFO, fmt.Sprintf("Using serialized cursor for stream %s", streamStateKey))
					if err != nil {
						ch.Logger.Error(fmt.Sprintf("Invalid serialized cursor for stream %v, failed with [%v]", streamStateKey, err))
						os.Exit(1)
					}

					// Emit RUNNING trace message before processing
					ch.Logger.StreamTrace(table.Stream.Name, keyspaceOrDatabase, internal.STREAM_STATUS_RUNNING)

					sc, err := ch.Database.Read(context.Background(), cmd.OutOrStdout(), psc, table, tc)
					if err != nil {
						ch.Logger.Error(err.Error())
						os.Exit(1)
					}

					if sc != nil {
						// if we get any new state, we assign it here.
						// otherwise, the older state is round-tripped back to Airbyte.
						ch.Logger.Log(internal.LOGLEVEL_INFO, fmt.Sprintf("Updating state for stream %s, shard %s with new cursor", streamStateKey, shardName))
						syncState.Streams[streamStateKey].Shards[shardName] = sc
						cursorPreview := sc.Cursor
						if len(cursorPreview) > 50 {
							cursorPreview = cursorPreview[:50] + "..."
						}
						ch.Logger.Log(internal.LOGLEVEL_INFO, fmt.Sprintf("New cursor for stream %s, shard %s: %s", streamStateKey, shardName, cursorPreview))
					} else {
						ch.Logger.Log(internal.LOGLEVEL_INFO, fmt.Sprintf("No new state returned for stream %s, shard %s - keeping existing state", streamStateKey, shardName))
					}
				}
				
				// Emit COMPLETE trace message after processing all shards for this stream
				ch.Logger.StreamTrace(table.Stream.Name, keyspaceOrDatabase, internal.STREAM_STATUS_COMPLETE)
				
				// Flush any pending records before emitting state
				ch.Logger.Flush()
				
				// Emit state based on configured state type
				stateType := psc.GetStateType()
				ch.Logger.Log(internal.LOGLEVEL_INFO, fmt.Sprintf("State type configured as: %s", stateType))
				if stateType == internal.STATE_TYPE_STREAM {
					// Emit per-stream state after processing all shards for this stream
					ch.Logger.Log(internal.LOGLEVEL_INFO, fmt.Sprintf("Emitting STREAM state for stream: %s, namespace: %s", table.Stream.Name, keyspaceOrDatabase))
					ch.Logger.Log(internal.LOGLEVEL_INFO, fmt.Sprintf("Stream state contains %d shards", len(syncState.Streams[streamStateKey].Shards)))
					for shardName, shardState := range syncState.Streams[streamStateKey].Shards {
						cursorPreview := shardState.Cursor
						if len(cursorPreview) > 50 {
							cursorPreview = cursorPreview[:50] + "..."
						}
						ch.Logger.Log(internal.LOGLEVEL_INFO, fmt.Sprintf("Shard %s cursor: %s", shardName, cursorPreview))
					}
					ch.Logger.StreamState(table.Stream.Name, keyspaceOrDatabase, syncState.Streams[streamStateKey])
					ch.Logger.Log(internal.LOGLEVEL_INFO, fmt.Sprintf("Successfully emitted STREAM state for stream: %s", table.Stream.Name))
				}
			}
			
			// For GLOBAL state type, emit a single global state message after processing all streams
			stateType := psc.GetStateType()
			ch.Logger.Log(internal.LOGLEVEL_INFO, fmt.Sprintf("Final state emission - State type: %s", stateType))
			if stateType == internal.STATE_TYPE_GLOBAL {
				// Flush any pending records before emitting global state
				ch.Logger.Flush()
				ch.Logger.Log(internal.LOGLEVEL_INFO, fmt.Sprintf("Emitting GLOBAL state with %d streams", len(syncState.Streams)))
				for streamKey, streamState := range syncState.Streams {
					ch.Logger.Log(internal.LOGLEVEL_INFO, fmt.Sprintf("Global state - Stream %s has %d shards", streamKey, len(streamState.Shards)))
					for shardName, shardState := range streamState.Shards {
						cursorPreview := shardState.Cursor
						if len(cursorPreview) > 50 {
							cursorPreview = cursorPreview[:50] + "..."
						}
						ch.Logger.Log(internal.LOGLEVEL_INFO, fmt.Sprintf("Global state - Stream %s, Shard %s cursor: %s", streamKey, shardName, cursorPreview))
					}
				}
				// Emit global state with all stream states
				ch.Logger.GlobalState(nil, syncState.Streams) // No shared state for this connector
				ch.Logger.Log(internal.LOGLEVEL_INFO, "Successfully emitted GLOBAL state message")
			} else {
				ch.Logger.Log(internal.LOGLEVEL_INFO, "No global state emission - using STREAM state type")
			}
			
			ch.Logger.Log(internal.LOGLEVEL_INFO, "=== SYNC COMPLETED ===")
			ch.Logger.Log(internal.LOGLEVEL_INFO, fmt.Sprintf("Total streams processed: %d", len(catalog.Streams)))
			ch.Logger.Log(internal.LOGLEVEL_INFO, fmt.Sprintf("State emission mode: %s", psc.GetStateType()))
		},
	}
	readCmd.Flags().StringVar(&readSourceCatalogPath, "catalog", "", "Path to the PlanetScale catalog configuration")
	readCmd.Flags().StringVar(&readSourceConfigFilePath, "config", "", "Path to the PlanetScale catalog configuration")
	readCmd.Flags().StringVar(&stateFilePath, "state", "", "Path to the PlanetScale state information")
	return readCmd
}

type State struct {
	Shards map[string]map[string]interface{} `json:"shards"`
}

func readState(state string, psc internal.PlanetScaleSource, streams []internal.ConfiguredStream, shards []string, logger internal.AirbyteLogger) (internal.SyncState, error) {
	syncState := internal.SyncState{
		Streams: map[string]internal.ShardStates{},
	}
	
	if state != "" {
		logger.Log(internal.LOGLEVEL_INFO, fmt.Sprintf("Parsing state file with length: %d", len(state)))
		
		// Try to parse the new Airbyte state format first (array of stream states)
		err := parseNewStateFormat(state, &syncState)
		if err != nil {
			// Fall back to old format parsing
			logger.Log(internal.LOGLEVEL_INFO, fmt.Sprintf("Failed to parse new state format, trying old format: %v", err))
			err = parseStreamStateMessages(state, &syncState)
			if err != nil {
				return syncState, fmt.Errorf("failed to parse both new and old state formats: %v", err)
			}
		} else {
			logger.Log(internal.LOGLEVEL_INFO, fmt.Sprintf("Successfully parsed new state format with %d streams", len(syncState.Streams)))
			for stateKey := range syncState.Streams {
				logger.Log(internal.LOGLEVEL_INFO, fmt.Sprintf("Found state for stream key: %s", stateKey))
			}
		}
	}

	for _, s := range streams {
		keyspaceOrDatabase := s.Stream.Namespace
		if keyspaceOrDatabase == "" {
			keyspaceOrDatabase = psc.Database
		}
		stateKey := keyspaceOrDatabase + ":" + s.Stream.Name
		logger.Log(internal.LOGLEVEL_INFO, fmt.Sprintf("Syncing stream %s with sync mode %s", s.Stream.Name, s.SyncMode))
		logger.Log(internal.LOGLEVEL_INFO, fmt.Sprintf("Looking for state with key: %s", stateKey))
		ignoreCurrentCursor := !s.IncrementalSyncRequested()

		// if no table cursor was found in the state, or we want to ignore the current cursor,
		// Send along an empty cursor for each shard.
		if _, ok := syncState.Streams[stateKey]; !ok || ignoreCurrentCursor {
			if !ok {
				logger.Log(internal.LOGLEVEL_INFO, fmt.Sprintf("No cursor found for key %s (state exists: %t)", stateKey, ok))
			}
			if ignoreCurrentCursor {
				logger.Log(internal.LOGLEVEL_INFO, fmt.Sprintf("Ignoring cursor for key %s because incremental sync not requested", stateKey))
			}
			logger.Log(internal.LOGLEVEL_INFO, fmt.Sprintf("Ignoring current cursor since incremental sync is disabled, or no cursor was found for key %s", stateKey))
			initialState, err := psc.GetInitialState(keyspaceOrDatabase, shards)
			if err != nil {
				return syncState, err
			}
			syncState.Streams[stateKey] = initialState
		} else {
			logger.Log(internal.LOGLEVEL_INFO, fmt.Sprintf("Using existing state for key %s", stateKey))
		}
	}

	return syncState, nil
}

// parseNewStateFormat parses the new Airbyte state format (array of stream state messages)
func parseNewStateFormat(state string, syncState *internal.SyncState) error {
	// Define a structure that matches the actual JSON format
	type StreamStateMessage struct {
		Type   string `json:"type"`
		Stream struct {
			StreamDescriptor struct {
				Name      string  `json:"name"`
				Namespace *string `json:"namespace"`
			} `json:"stream_descriptor"`
			StreamState internal.ShardStates `json:"stream_state"`
		} `json:"stream"`
	}
	
	var messages []StreamStateMessage
	
	// Try to parse as array of stream state messages
	err := json.Unmarshal([]byte(state), &messages)
	if err != nil {
		return fmt.Errorf("failed to parse as new state format: %v", err)
	}
	
	// Convert to our internal format
	for _, message := range messages {
		if message.Type == "STREAM" {
			namespace := ""
			if message.Stream.StreamDescriptor.Namespace != nil {
				namespace = *message.Stream.StreamDescriptor.Namespace
			}
			
			stateKey := namespace + ":" + message.Stream.StreamDescriptor.Name
			syncState.Streams[stateKey] = message.Stream.StreamState
		}
	}
	
	return nil
}

// parseStreamStateMessages parses a sequence of AirbyteMessage state messages
func parseStreamStateMessages(state string, syncState *internal.SyncState) error {
	lines := strings.Split(strings.TrimSpace(state), "\n")
	
	for _, line := range lines {
		if strings.TrimSpace(line) == "" {
			continue
		}
		
		var message internal.AirbyteMessage
		err := json.Unmarshal([]byte(line), &message)
		if err != nil {
			continue // Skip invalid JSON lines
		}
		
		if message.Type == internal.STATE && message.State != nil {
			switch message.State.StateType {
			case internal.STATE_TYPE_STREAM:
				// Handle STREAM state message
				if message.State.Stream != nil {
					streamDesc := message.State.Stream.StreamDescriptor
					namespace := ""
					if streamDesc.Namespace != nil {
						namespace = *streamDesc.Namespace
					}
					
					stateKey := namespace + ":" + streamDesc.Name
					if message.State.Stream.StreamState != nil {
						syncState.Streams[stateKey] = *message.State.Stream.StreamState
					}
				}
			case internal.STATE_TYPE_GLOBAL:
				// Handle GLOBAL state message
				if message.State.Global != nil {
					// Process each stream state in the global state
					for _, streamState := range message.State.Global.StreamStates {
						streamDesc := streamState.StreamDescriptor
						namespace := ""
						if streamDesc.Namespace != nil {
							namespace = *streamDesc.Namespace
						}
						
						stateKey := namespace + ":" + streamDesc.Name
						if streamState.StreamState != nil {
							syncState.Streams[stateKey] = *streamState.StreamState
						}
					}
				}
			case "":
				// Legacy state format (backwards compatibility)
				// This handles old state messages that don't have state_type set
				// Treat as stream state if it has stream field
				if message.State.Stream != nil {
					streamDesc := message.State.Stream.StreamDescriptor
					namespace := ""
					if streamDesc.Namespace != nil {
						namespace = *streamDesc.Namespace
					}
					
					stateKey := namespace + ":" + streamDesc.Name
					if message.State.Stream.StreamState != nil {
						syncState.Streams[stateKey] = *message.State.Stream.StreamState
					}
				}
			}
		}
	}
	
	return nil
}

func readCatalog(path string) (c internal.ConfiguredCatalog, err error) {
	b, err := os.ReadFile(path)
	if err != nil {
		return c, err
	}
	err = json.Unmarshal(b, &c)
	return c, err
}
