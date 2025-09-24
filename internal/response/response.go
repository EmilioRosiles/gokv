package response

import (
	"errors"
	"fmt"

	"gokv/proto/commonpb"
)

// Marshal converts a command handler's response into a protobuf CommandResponse.
func Marshal(data any) (*commonpb.CommandResponse, error) {
	resp := &commonpb.CommandResponse{}
	switch v := data.(type) {
	case []byte:
		resp.Response = &commonpb.CommandResponse_Value{Value: v}
	case bool:
		resp.Response = &commonpb.CommandResponse_Success{Success: v}
	case int64:
		resp.Response = &commonpb.CommandResponse_Count{Count: v}
	case *commonpb.KeyValueList:
		resp.Response = &commonpb.CommandResponse_List{List: v}
	case *commonpb.KeyValueMap:
		resp.Response = &commonpb.CommandResponse_Map{Map: v}
	default:
		return nil, fmt.Errorf("unknown response type to marshal: %T", v)
	}
	return resp, nil
}

// Unmarshal converts a protobuf CommandResponse into a generic interface{}.
func Unmarshal(resp *commonpb.CommandResponse) (any, error) {
	if resp.Error != "" {
		return nil, errors.New(resp.Error)
	}
	switch v := resp.Response.(type) {
	case *commonpb.CommandResponse_Value:
		return v.Value, nil
	case *commonpb.CommandResponse_Success:
		return v.Success, nil
	case *commonpb.CommandResponse_Count:
		return v.Count, nil
	case *commonpb.CommandResponse_List:
		return v.List, nil
	case *commonpb.CommandResponse_Map:
		return v.Map, nil
	default:
		return nil, fmt.Errorf("unknown response type to unmarshal: %T", v)
	}
}
