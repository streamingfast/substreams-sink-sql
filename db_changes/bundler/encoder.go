package bundler

import (
	"encoding/json"
	"fmt"
	"sort"
	"strings"

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

func CSVEncode(message map[string]string) ([]byte, error) {
	keys := make([]string, 0, len(message))
	for k := range message {
		keys = append(keys, k)
	}
	sort.Strings(keys)

	var row []string
	for _, key := range keys {
		row = append(row, message[key])
	}
	str := strings.Join(row, ",")
	data := []byte(str)
	data = append(data, byte('\n'))
	return data, nil
}
