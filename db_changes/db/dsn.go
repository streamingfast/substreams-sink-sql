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

	Host     string
	Port     int64
	Username string
	Password string
	Database string
	schema   string
	Options  []string
}

var driverMap = map[string]string{
	"psql":       "postgres",
	"postgres":   "postgres",
	"clickhouse": "clickhouse",
	"risingwave": "risingwave",
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
		Host:     host,
		Port:     port,
		Username: username,
		Password: password,
		Database: database,
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
	d.Options = options
	return d, nil
}

func (c *DSN) Driver() string {
	return c.driver
}

// SqlDriver returns the SQL driver name that should be used with sql.Open()
// For RisingWave, this returns "postgres" since RisingWave uses the PostgreSQL wire protocol
func (c *DSN) SqlDriver() string {
	if c.driver == "risingwave" {
		return "postgres"
	}
	return c.driver
}

func (c *DSN) ConnString() string {
	if c.driver == "clickhouse" {
		for _, option := range c.Options {
			if c.Host == "localhost" {
				c.Host = "127.0.0.1"
				scheme := "http"
				if option == "secure=true" {
					scheme = "https"
				}
				return strings.Replace(c.original, "clickhouse://", scheme+"://", 1)
			}
		}
		return c.original
	}
	out := fmt.Sprintf("host=%s port=%d dbname=%s %s", c.Host, c.Port, c.Database, strings.Join(c.Options, " "))
	if c.Username != "" {
		out = out + " user=" + c.Username
	}
	if c.Password != "" {
		out = out + " password=" + c.Password
	}
	return out
}

func (c *DSN) Schema() string {
	return c.schema
}
