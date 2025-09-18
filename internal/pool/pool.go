package pool

import (
	"sync"

	"google.golang.org/grpc"
)

type GrpcConnectionPool struct {
	mu      sync.Mutex
	conns   map[string]*grpc.ClientConn
	factory func(address string) (*grpc.ClientConn, error)
}

func NewGrpcConnectionPool(factory func(address string) (*grpc.ClientConn, error)) *GrpcConnectionPool {
	return &GrpcConnectionPool{
		conns:   make(map[string]*grpc.ClientConn),
		factory: factory,
	}
}

func (p *GrpcConnectionPool) Get(address string) (*grpc.ClientConn, error) {
	p.mu.Lock()
	defer p.mu.Unlock()

	if conn, ok := p.conns[address]; ok {
		return conn, nil
	}

	conn, err := p.factory(address)
	if err != nil {
		return nil, err
	}

	p.conns[address] = conn
	return conn, nil
}

func (p *GrpcConnectionPool) Close(address string) {
	p.mu.Lock()
	defer p.mu.Unlock()

	if conn, ok := p.conns[address]; ok {
		conn.Close()
		delete(p.conns, address)
	}
}
