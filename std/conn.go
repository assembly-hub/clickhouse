package std

import (
	"database/sql"

	_ "github.com/ClickHouse/clickhouse-go/v2"
	"github.com/assembly-hub/db"
	"github.com/assembly-hub/impl-db-sql"
)

type Config struct {
	DSN string
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
	conn, err := sql.Open("clickhouse", c.cfg.DSN)
	if err != nil {
		return nil, err
	}
	return impl.NewDB(conn), err
}
