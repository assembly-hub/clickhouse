package clickhouse

import (
	"github.com/ClickHouse/clickhouse-go/v2"
	"github.com/assembly-hub/db"
)

type Config struct {
	clickhouse.Options
}

type Client struct {
	cfg *Config
}

func NewClient(cfg *Config) *Client {
	c := new(Client)
	c.cfg = cfg
	return c
}

func (c *Client) Connect() (db.Executor, error) {
	conn, err := clickhouse.Open(&c.cfg.Options)
	if err != nil {
		return nil, err
	}
	return NewDB(conn), err
}
