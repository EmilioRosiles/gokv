package peer

import (
	clusterpb "gokv/proto"
	"time"
)

type Peer struct {
	NodeID   string
	NodeAddr string
	Alive    bool
	LastSeen time.Time
}

func ToProto(entry Peer) *clusterpb.Node {
	return &clusterpb.Node{
		NodeId:   entry.NodeID,
		NodeAddr: entry.NodeAddr,
		Alive:    entry.Alive,
		LastSeen: entry.LastSeen.Unix(),
	}
}

func FromProto(p *clusterpb.Node) Peer {
	return Peer{
		NodeID:   p.NodeId,
		NodeAddr: p.NodeAddr,
		Alive:    p.Alive,
		LastSeen: time.Unix(p.LastSeen, 0),
	}
}

func ToProtoList(entries []Peer) []*clusterpb.Node {
	protoEntries := make([]*clusterpb.Node, len(entries))
	for i, e := range entries {
		protoEntries[i] = ToProto(e)
	}
	return protoEntries
}

func FromProtoList(pl []*clusterpb.Node) []Peer {
	entries := make([]Peer, len(pl))
	for i, p := range pl {
		entries[i] = FromProto(p)
	}
	return entries
}
