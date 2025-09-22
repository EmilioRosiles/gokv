package command

import (
	"sync"
)

type CommandLevel int

const (
	Node CommandLevel = iota
	Replica
	Cluster
)

type Command struct {
	Run   func(key string, args ...[]byte) (any, error)
	Level CommandLevel
	Aggr  func(results ...any) (any, error)
}

// CommandRegistry stores and manages command handlers.
type CommandRegistry struct {
	mu       sync.RWMutex
	commands map[string]Command
}

// NewCommandRegistry creates a new CommandRegistry.
func NewCommandRegistry() *CommandRegistry {
	return &CommandRegistry{
		commands: make(map[string]Command),
	}
}

// Register a command handler.
func (r *CommandRegistry) Register(name string, cmd Command) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.commands[name] = cmd
}

// Get a command handler.
func (r *CommandRegistry) Get(name string) (Command, bool) {
	r.mu.RLock()
	defer r.mu.RUnlock()
	cmd, ok := r.commands[name]
	return cmd, ok
}
