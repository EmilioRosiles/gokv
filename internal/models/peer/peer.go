package peer

import (
	"time"

	"gokv/proto/externalpb"
	"gokv/proto/internalpb"
)

type Peer struct {
	NodeID           string
	NodeInternalAddr string
	NodeExternalAddr string
	Alive            bool
	LastSeen         time.Time
}

func ToHeartbeatNodeProto(entry Peer) *internalpb.HeartbeatNode {
	return &internalpb.HeartbeatNode{
		NodeId:           entry.NodeID,
		NodeInternalAddr: entry.NodeInternalAddr,
		NodeExternalAddr: entry.NodeExternalAddr,
		Alive:            entry.Alive,
		LastSeen:         entry.LastSeen.Unix(),
	}
}

func FromHeartbeatNodeProto(p *internalpb.HeartbeatNode) Peer {
	return Peer{
		NodeID:           p.NodeId,
		NodeInternalAddr: p.NodeInternalAddr,
		NodeExternalAddr: p.NodeExternalAddr,
		Alive:            p.Alive,
		LastSeen:         time.Unix(p.LastSeen, 0),
	}
}

func ToHeartbeatNodeProtoList(entries []Peer) []*internalpb.HeartbeatNode {
	protoEntries := make([]*internalpb.HeartbeatNode, len(entries))
	for i, e := range entries {
		protoEntries[i] = ToHeartbeatNodeProto(e)
	}
	return protoEntries
}

func FromHeartbeatNodeProtoList(pl []*internalpb.HeartbeatNode) []Peer {
	entries := make([]Peer, len(pl))
	for i, p := range pl {
		entries[i] = FromHeartbeatNodeProto(p)
	}
	return entries
}

func ToHealthcheckNodeProto(entry Peer) *externalpb.HealthcheckNode {
	return &externalpb.HealthcheckNode{
		NodeId:   entry.NodeID,
		NodeAddr: entry.NodeExternalAddr,
		Alive:    entry.Alive,
	}
}

func ToHealthcheckNodeProtoList(entries []Peer) []*externalpb.HealthcheckNode {
	protoEntries := make([]*externalpb.HealthcheckNode, len(entries))
	for i, e := range entries {
		protoEntries[i] = ToHealthcheckNodeProto(e)
	}
	return protoEntries
}
