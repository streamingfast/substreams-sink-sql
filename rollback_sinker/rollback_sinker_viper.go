package rollback_sinker

import (
	"github.com/spf13/cobra"
	"github.com/streamingfast/cli/sflags"
	"github.com/streamingfast/substreams-sink-postgres/sinker"
)

const (
	FlagRollbackUrl      = "rollback-url"
	FlagRollbackDBSchema = "rollback-db-schema"
)

func NewFromViper(cmd *cobra.Command, postgresSinker *sinker.PostgresSinker) (*RollbackSinker, error) {
	rollbackUrl, _ := sflags.GetString(cmd, FlagRollbackUrl)
	rollbackDBSchema, _ := sflags.GetString(cmd, FlagRollbackDBSchema)
	return New(postgresSinker, rollbackUrl, rollbackDBSchema)
}
