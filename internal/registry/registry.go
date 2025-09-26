package registry

import (
	"sync"

	"gokv/internal/hashring"
	"gokv/proto/commonpb"
)

type Command struct {
	Run             func(key string, args ...[]byte) (*commonpb.CommandResponse, error)
	Replicate       bool
	ResponsibleFunc func(req *commonpb.CommandRequest, hr *hashring.HashRing) (string, error)
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
