package response

import (
	"errors"
	"fmt"

	"gokv/proto/clusterpb"
)

// Marshal converts a command handler's response into a protobuf CommandResponse.
func Marshal(data any) (*clusterpb.CommandResponse, error) {
	resp := &clusterpb.CommandResponse{}
	switch v := data.(type) {
	case []byte:
		resp.Response = &clusterpb.CommandResponse_Value{Value: v}
	case bool:
		resp.Response = &clusterpb.CommandResponse_Success{Success: v}
	case int64:
		resp.Response = &clusterpb.CommandResponse_Count{Count: v}
	case *clusterpb.KeyValueList:
		resp.Response = &clusterpb.CommandResponse_List{List: v}
	case *clusterpb.KeyValueMap:
		resp.Response = &clusterpb.CommandResponse_Map{Map: v}
	default:
		return nil, fmt.Errorf("unknown response type to marshal: %T", v)
	}
	return resp, nil
}

// Unmarshal converts a protobuf CommandResponse into a generic interface{}.
func Unmarshal(resp *clusterpb.CommandResponse) (any, error) {
	if resp.Error != "" {
		return nil, errors.New(resp.Error)
	}
	switch v := resp.Response.(type) {
	case *clusterpb.CommandResponse_Value:
		return v.Value, nil
	case *clusterpb.CommandResponse_Success:
		return v.Success, nil
	case *clusterpb.CommandResponse_Count:
		return v.Count, nil
	case *clusterpb.CommandResponse_List:
		return v.List, nil
	case *clusterpb.CommandResponse_Map:
		return v.Map, nil
	default:
		return nil, fmt.Errorf("unknown response type to unmarshal: %T", v)
	}
}
