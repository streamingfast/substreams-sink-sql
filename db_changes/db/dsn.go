package db

import (
	"fmt"
	"net/url"
	"os"
	"sort"
	"strconv"
	"strings"

	"github.com/drone/envsubst"
)

type DSN struct {
	driver   string
	original string

	host     string
	port     int64
	username string
	password string
	database string
	schema   string
	options  []string
}

var driverMap = map[string]string{
	"psql":       "postgres",
	"postgres":   "postgres",
	"clickhouse": "clickhouse",
}

func ParseDSN(dsn string) (*DSN, error) {
	expanded, err := envsubst.Eval(dsn, os.Getenv)
	if err != nil {
		return nil, fmt.Errorf("variables expansion failed: %w", err)
	}

	dsnURL, err := url.Parse(expanded)
	if err != nil {
		return nil, fmt.Errorf("invalid url: %w", err)
	}

	driver, ok := driverMap[dsnURL.Scheme]
	if !ok {
		keys := make([]string, len(driverMap))
		i := 0
		for k := range driverMap {
			keys[i] = k
			i++
		}

		return nil, fmt.Errorf("invalid scheme %s, allowed schemes: [%s]", dsnURL.Scheme, strings.Join(keys, ","))
	}

	host := dsnURL.Hostname()

	port := int64(5432)
	if strings.Contains(dsnURL.Host, ":") {
		port, _ = strconv.ParseInt(dsnURL.Port(), 10, 32)
	}

	username := dsnURL.User.Username()
	password, _ := dsnURL.User.Password()
	database := strings.TrimPrefix(dsnURL.EscapedPath(), "/")

	query := dsnURL.Query()
	keys := make([]string, 0, len(query))
	for key := range dsnURL.Query() {
		keys = append(keys, key)
	}
	sort.Strings(keys)

	d := &DSN{
		original: dsn,
		driver:   driver,
		host:     host,
		port:     port,
		username: username,
		password: password,
		database: database,
		schema:   "public",
	}

	if driver == "clickhouse" {
		d.schema = database
	}

	options := make([]string, len(query))
	for i, key := range keys {
		if key == "schemaName" {
			d.schema = query[key][0]
			continue
		}

		options[i] = fmt.Sprintf("%s=%s", key, strings.Join(query[key], ","))
	}
	d.options = options
	return d, nil
}

func (c *DSN) Driver() string {
	return c.driver
}

func (c *DSN) ConnString() string {
	if c.driver == "clickhouse" {
		return c.original
	}
	out := fmt.Sprintf("host=%s port=%d dbname=%s %s", c.host, c.port, c.database, strings.Join(c.options, " "))
	if c.username != "" {
		out = out + " user=" + c.username
	}
	if c.password != "" {
		out = out + " password=" + c.password
	}
	return out
}

func (c *DSN) Schema() string {
	return c.schema
}
