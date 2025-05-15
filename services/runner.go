package services

import (
	"time"

	pbsql "github.com/streamingfast/substreams-sink-sql/pb/sf/substreams/sink/sql/services/v1"
	"go.uber.org/zap"
)

func Run(service *pbsql.Service, logger *zap.Logger) error {
	if service.HasuraFrontend != nil {
		panic("Hasura front end not supported yet")
	}
	if service.PostgraphileFrontend != nil {
		panic("Postgraphile front end not supported yet")
	}
	if service.RestFrontend != nil {
		panic("Rest front end not supported yet")
	}

	if service.DbtConfig != nil && service.DbtConfig.Enabled {
		go func() {
			for {
				err := runDBT(service.DbtConfig, logger)
				if err != nil {
					logger.Error("running dbt", zap.Error(err))
					time.Sleep(30 * time.Second)
				}
			}
		}()
	}

	return nil
}
