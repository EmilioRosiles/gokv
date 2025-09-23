package peer

import (
	"gokv/proto/commonpb"
	"time"
)

type Peer struct {
	NodeID   string
	NodeAddr string
	Alive    bool
	LastSeen time.Time
}

func ToProto(entry Peer) *commonpb.Node {
	return &commonpb.Node{
		NodeId:   entry.NodeID,
		NodeAddr: entry.NodeAddr,
		Alive:    entry.Alive,
		LastSeen: entry.LastSeen.Unix(),
	}
}

func FromProto(p *commonpb.Node) Peer {
	return Peer{
		NodeID:   p.NodeId,
		NodeAddr: p.NodeAddr,
		Alive:    p.Alive,
		LastSeen: time.Unix(p.LastSeen, 0),
	}
}

func ToProtoList(entries []Peer) []*commonpb.Node {
	protoEntries := make([]*commonpb.Node, len(entries))
	for i, e := range entries {
		protoEntries[i] = ToProto(e)
	}
	return protoEntries
}

func FromProtoList(pl []*commonpb.Node) []Peer {
	entries := make([]Peer, len(pl))
	for i, p := range pl {
		entries[i] = FromProto(p)
	}
	return entries
}
