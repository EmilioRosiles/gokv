package command

import (
	"sync"
)

// CommandFunc is the type for command handlers.
type CommandFunc func(key string, args ...[]byte) ([]byte, error)

// CommandRegistry stores and manages command handlers.
type CommandRegistry struct {
	mu       sync.RWMutex
	commands map[string]CommandFunc
}

// NewCommandRegistry creates a new CommandRegistry.
func NewCommandRegistry() *CommandRegistry {
	return &CommandRegistry{
		commands: make(map[string]CommandFunc),
	}
}

// Register a command handler.
func (r *CommandRegistry) Register(name string, fn CommandFunc) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.commands[name] = fn
}

// Get a command handler.
func (r *CommandRegistry) Get(name string) (CommandFunc, bool) {
	r.mu.RLock()
	defer r.mu.RUnlock()
	fn, ok := r.commands[name]
	return fn, ok
}
