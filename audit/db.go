package audit

import (
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	// what is the right comment here?
	_ "github.com/denisenkom/go-mssqldb"
)

type config struct {
	Username string
	Password string
	Port     int
	Host     string
	Server   string
	Database string
}

// type autidor interface {
// 	log() bool
// 	Create() bool
// 	debug() string
// }

var db *sql.DB

// LogKVChange logs a KV change and returns the version of this KV and an error
func LogKVChange(key, value, acl string) (int, error) {

	// Get last version of KV
	var version int
	err := db.QueryRow("select max(version) from kv_history where kvkey = ?", key).Scan(&version)
	if err != nil {
		return -1, err
	}

	prep, err := db.Prepare("INSERT INTO kv_history (timestamp, kvkey, kvvalue, acl, version) VALUES (?,?,?,?,?)")
	if err != nil {
		return -1, err
	}

	// increase the version and write the new value
	version++
	res, err := prep.Exec(time.Now(), key, value, acl, version)
	if err != nil {
		return -1, err
	}

	// it should only be one row effected, if not something is wrong
	rowseffected, err := res.RowsAffected()
	if err != nil {
		return -1, err
	}
	if rowseffected != 1 {
		return -1, errors.New("Affected rows should be 1 but is " + string(rowseffected))
	}

	return version, nil
}

// Open initiates a db connection
func Open(c config) error {
	connString := fmt.Sprintf("server=%s;user id=%s;password=%s;port=%d;database=%s", c.Server, c.Username, c.Password, c.Port, c.Database)
	var err error
	db, _ = sql.Open("mssql", connString)
	if err != nil {
		return err
	}

	return nil
}

// Close the db connection
func Close() error {
	if err := db.Close(); err != nil {
		return err
	}
	return nil
}

func debug(c config) (string, error) {
	out, err := json.Marshal(c)
	if err != nil {
		return "", err
	}
	return string(out), nil
}
