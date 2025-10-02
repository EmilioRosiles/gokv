package commonpb

import "fmt"

func NewString(s string) *Value {
	return &Value{Kind: &Value_Str{Str: s}}
}

func NewInt(i int64) *Value {
	return &Value{Kind: &Value_Int{Int: i}}
}

func NewBool(b bool) *Value {
	return &Value{Kind: &Value_Bool{Bool: b}}
}

func NewBytes(b []byte) *Value {
	return &Value{Kind: &Value_Bytes{Bytes: b}}
}

func NewNil() *Value {
	return &Value{Kind: &Value_Nil{Nil: true}}
}

func NewList(vals ...*Value) *Value {
	return &Value{
		Kind: &Value_List{
			List: &ListValue{Values: vals},
		},
	}
}

func NewMap(m map[string]*Value) *Value {
	return &Value{
		Kind: &Value_Map{
			Map: &MapValue{Values: m},
		},
	}
}

func NewCursor(cursor uint64, count int, data *Value) *Value {
	payload := make(map[string]*Value, 2)
	payload["cursor"] = NewInt(int64(cursor))
	payload["data"] = data
	payload["count"] = NewInt(int64(count))
	return NewMap(payload)
}

func (v *Value) AsString() (string, bool) {
	if x, ok := v.Kind.(*Value_Str); ok {
		return x.Str, true
	}
	return "", false
}

func (v *Value) AsInt() (int64, bool) {
	if x, ok := v.Kind.(*Value_Int); ok {
		return x.Int, true
	}
	return 0, false
}

func (v *Value) AsBool() (bool, bool) {
	if x, ok := v.Kind.(*Value_Bool); ok {
		return x.Bool, true
	}
	return false, false
}

func (v *Value) AsBytes() ([]byte, bool) {
	if x, ok := v.Kind.(*Value_Bytes); ok {
		return x.Bytes, true
	}
	return nil, false
}

func (v *Value) AsNil() bool {
	_, ok := v.Kind.(*Value_Nil)
	return ok
}

func (v *Value) AsList() (*ListValue, bool) {
	if x, ok := v.Kind.(*Value_List); ok {
		return x.List, true
	}
	return nil, false
}

func (v *Value) AsMap() (*MapValue, bool) {
	if x, ok := v.Kind.(*Value_Map); ok {
		return x.Map, true
	}
	return nil, false
}

func ValueToInterface(v *Value) (any, error) {
	switch x := v.Kind.(type) {
	case *Value_Str:
		return x.Str, nil

	case *Value_Int:
		return x.Int, nil

	case *Value_Bool:
		return x.Bool, nil

	case *Value_Nil:
		return nil, nil

	case *Value_Bytes:
		return string(x.Bytes), nil

	case *Value_List:
		list := make([]any, len(x.List.Values))
		for i, elem := range x.List.Values {
			var err error
			list[i], err = ValueToInterface(elem)
			if err != nil {
				return nil, err
			}
		}
		return list, nil

	case *Value_Map:
		dict := make(map[string]any)
		for k, elem := range x.Map.Values {
			var err error
			dict[k], err = ValueToInterface(elem)
			if err != nil {
				return nil, err
			}
		}
		return dict, nil

	default:
		return nil, fmt.Errorf("<unknown>")
	}
}
