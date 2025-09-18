package position

import clusterpb "gokv/proto"

type PositionEntry struct {
	ID        string  `json:"id"`
	Lat       float64 `json:"lat"`
	Lon       float64 `json:"lon"`
	Timestamp int64   `json:"timestamp"`
}

func ToProto(entry PositionEntry) *clusterpb.PositionEntry {
	return &clusterpb.PositionEntry{
		Id:        entry.ID,
		Lat:       entry.Lat,
		Lon:       entry.Lon,
		Timestamp: entry.Timestamp,
	}
}

func FromProto(p *clusterpb.PositionEntry) PositionEntry {
	return PositionEntry{
		ID:        p.Id,
		Lat:       p.Lat,
		Lon:       p.Lon,
		Timestamp: p.Timestamp,
	}
}

func ToProtoList(entries []PositionEntry) []*clusterpb.PositionEntry {
	protoEntries := make([]*clusterpb.PositionEntry, len(entries))
	for i, e := range entries {
		protoEntries[i] = ToProto(e)
	}
	return protoEntries
}

func FromProtoList(pl []*clusterpb.PositionEntry) []PositionEntry {
	entries := make([]PositionEntry, len(pl))
	for i, p := range pl {
		entries[i] = FromProto(p)
	}
	return entries
}
