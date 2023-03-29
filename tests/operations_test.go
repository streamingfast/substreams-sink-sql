package db

import (
	"fmt"
	"github.com/stretchr/testify/assert"
	"testing"
)

func TestEscapeString(t *testing.T) {
	type args struct {
		valueToEscape string
		escapeType    string
	}
	tests := []struct {
		name    string
		args    args
		want    string
		wantErr assert.ErrorAssertionFunc
	}{
		// TODO: Add test cases.
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := EscapeString(tt.args.valueToEscape, tt.args.escapeType)
			if !tt.wantErr(t, err, fmt.Sprintf("EscapeString(%v, %v)", tt.args.valueToEscape, tt.args.escapeType)) {
				return
			}
			assert.Equalf(t, tt.want, got, "EscapeString(%v, %v)", tt.args.valueToEscape, tt.args.escapeType)
		})
	}
}
