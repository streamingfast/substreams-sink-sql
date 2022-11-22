package main

import (
	"fmt"
	"os"
	"strconv"
	"strings"

	"github.com/spf13/viper"

	pbsubstreams "github.com/streamingfast/substreams/pb/sf/substreams/v1"
)

func readBlockRange(module *pbsubstreams.Module, input string) (start int64, stop uint64, err error) {
	if input == "" {
		input = "-1"
	}

	before, after, found := strings.Cut(input, ":")

	beforeRelative := strings.HasPrefix(before, "+")
	beforeInt64, err := strconv.ParseInt(strings.TrimPrefix(before, "+"), 0, 64)
	if err != nil {
		return 0, 0, fmt.Errorf("invalid block number value %q: %w", before, err)
	}

	afterRelative := false
	afterInt64 := int64(0)
	if found {
		afterRelative = strings.HasPrefix(after, "+")
		afterInt64, err = strconv.ParseInt(after, 0, 64)
		if err != nil {
			return 0, 0, fmt.Errorf("invalid block number value %q: %w", after, err)
		}
	}

	// If there is no `:` we assume it's a stop block value right away
	if !found {
		start = int64(module.InitialBlock)
		stop = uint64(resolveBlockNumber(beforeInt64, 0, beforeRelative, uint64(start)))
	} else {
		start = resolveBlockNumber(beforeInt64, int64(module.InitialBlock), beforeRelative, module.InitialBlock)
		stop = uint64(resolveBlockNumber(afterInt64, 0, afterRelative, uint64(start)))
	}

	return
}

func resolveBlockNumber(value int64, ifMinus1 int64, relative bool, against uint64) int64 {
	if !relative {
		if value < 0 {
			return ifMinus1
		}

		return value
	}

	return int64(against) + value
}

func readAPIToken() string {
	apiToken := viper.GetString("api-token")
	if apiToken != "" {
		return apiToken
	}

	apiToken = os.Getenv("SUBSTREAMS_API_TOKEN")
	if apiToken != "" {
		return apiToken
	}

	return os.Getenv("SF_API_TOKEN")
}
