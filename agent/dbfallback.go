package agent

import (
	"database/sql"
	"fmt"
	"net/url"

	// used mssql driver
	_ "github.com/denisenkom/go-mssqldb"
	"github.com/hashicorp/consul/agent/structs"
)

var db *sql.DB

func (s *HTTPServer) IsAuditorOpen() bool {
	return db != nil
}

// GetKV will get the latest value and regex for a
func (s *HTTPServer) GetKV(key, datacenter string) (structs.DirEntry, error) {
	entry := structs.DirEntry{}
	rows, err := db.Query("SELECT kvkey, kvvalue, regex FROM kv WHERE kvkey = ? AND version = ( SELECT MAX(version) FROM kv WHERE kvkey = ?) AND datacenter = ? AND deleted = 0 ORDER BY timestamp DESC", key, key, datacenter)
	if err != nil {
		return entry, err
	}

	var regex sql.NullString

	for rows.Next() {
		err = rows.Scan(&entry.Key, &entry.Value, &regex)
		entry.RegEx = regex.String
		if err != nil {
			return entry, err
		}
	}

	return entry, nil
}

// createConnectionString returns a connection string based on the configuration
func createConnectionString(c *DB) string {
	query := url.Values{}
	query.Add("connection timeout", fmt.Sprintf("%d", 30))
	query.Add("database", c.Database)

	u := &url.URL{
		Scheme:   "sqlserver",
		User:     url.UserPassword(c.Username, c.Password),
		Host:     fmt.Sprintf("%s:%d", c.Host, c.Port),
		Path:     c.Server,
		RawQuery: query.Encode(),
	}
	return u.String()
}

// OpenAuditor initiates a db connection
func (s *HTTPServer) OpenAuditor() error {
	connString := createConnectionString(&s.agent.config.DB)
	s.agent.logger.Printf("[DEBUG] consul: connection string to MSSQL DB: %s", connString)
	var err error
	db, err = sql.Open("mssql", connString)
	if err != nil {
		return err
	}
	return nil
}

// CloseAuditor closes the db connection
func (s *HTTPServer) CloseAuditor() error {
	s.agent.logger.Printf("[DEBUG] consul: close connection to MSSQL DB")
	if err := db.Close(); err != nil {
		return err
	}
	return nil
}
