package bundler

import (
	"encoding/json"
	"fmt"
	"github.com/golang/protobuf/proto"
)

type Encoder func(proto.Message) ([]byte, error)

func JSONLEncode(message proto.Message) ([]byte, error) {
	buf := []byte{}
	data, err := json.Marshal(message)
	if err != nil {
		return nil, fmt.Errorf("json marshal: %w", err)
	}
	buf = append(buf, data...)
	buf = append(buf, byte('\n'))
	return buf, nil
}
