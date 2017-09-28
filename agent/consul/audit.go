package consul

import (
	"database/sql"
	"errors"
	"fmt"
	"net/url"
	"strings"
	"time"

	// used mssql driver
	_ "github.com/denisenkom/go-mssqldb"

	"github.com/hashicorp/consul/agent/structs"
	"github.com/hashicorp/consul/api"
)

var db *sql.DB

// IsAuditorEnabled returns true if autiding is enabled
func (s *Server) IsAuditorEnabled() bool {
	return s.config.DBConfig.Database != ""
}

// Get latest version from key in this datacenter
func getVersion(key, datacenter string) (int, error) {
	version := 0

	rows, err := db.Query("select max(version) from kv where kvkey = ? and datacenter = ? AND deleted = 0", key, datacenter)
	if err != nil {
		return -1, err
	}
	for rows.Next() {
		err = rows.Scan(&version)
		if (err != nil) && (!strings.Contains(err.Error(), "converting driver.Value type")) {
			// if err != nil {
			return -1, err
		}
	}
	version++

	return version, nil
}

func deleteKVTree(args *structs.KVSRequest, s *Server) error {
	// Get last version of KV
	res, err := db.Exec("UPDATE kv SET deleted = 1 WHERE kvkey like '"+args.DirEnt.Key+"%' AND datacenter = ?", args.Datacenter)
	if err != nil {
		return err
	}
	if effected, _ := res.LastInsertId(); effected < 1 {
		s.logger.Printf("[WARNING] consul: Auditor: deleteKV could not find entry for key %s)", args.DirEnt.Key)
	}
	return nil
}

func deleteKV(args *structs.KVSRequest, s *Server) error {
	// Get last version of KV
	res, err := db.Exec("UPDATE kv SET deleted = 1 WHERE kvkey = ? AND datacenter = ?", args.DirEnt.Key, args.Datacenter)
	if err != nil {
		return err
	}
	if effected, _ := res.LastInsertId(); effected < 1 {
		s.logger.Printf("[WARNING] consul: Auditor: deleteKV could not find entry for key %s)", args.DirEnt.Key)
	}
	return nil
}

func changeKV(args *structs.KVSRequest) (int, error) {
	// Get last version of KV
	version, err := getVersion(args.DirEnt.Key, args.Datacenter)
	if err != nil {
		return -1, err
	}

	insert, err := db.Prepare("insert into kv (timestamp, createIndex, flags, kvkey, lockindex, modifyindex, session, kvvalue, version, datacenter, acl) values (?,?,?,?,?,?,?,?,?,?,?)")
	if err != nil {
		return -1, err
	}
	res, err := insert.Exec(
		time.Now(),
		args.DirEnt.CreateIndex,
		args.DirEnt.Flags,
		args.DirEnt.Key,
		args.DirEnt.LockIndex,
		args.DirEnt.ModifyIndex,
		args.DirEnt.Session,
		args.DirEnt.Value,
		version,
		args.Datacenter,
		args.Token)

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

// LogKVChange logs a KV change and returns the version of this KV and an error
func (s *Server) LogKVChange(args *structs.KVSRequest) error {

	switch args.Op {
	case api.KVDeleteTree:
		err := deleteKVTree(args, s)
		if err != nil {
			s.logger.Printf("[ERR] consul: Auditor: deleteKVTree failed (for key %s): %v", args.DirEnt.Key, err)
			return err
		}
		s.logger.Printf("[DEBUG] consul: logged deleteKVTree for key: %s", args.DirEnt.Key)
	case api.KVDelete:
		err := deleteKV(args, s)
		if err != nil {
			s.logger.Printf("[ERR] consul: Auditor: deleteKV failed (for key %s): %v", args.DirEnt.Key, err)
			return err
		}
		s.logger.Printf("[DEBUG] consul: logged deleteKV for key: %s", args.DirEnt.Key)
	case api.KVSet:
		version, err := changeKV(args)
		if err != nil {
			s.logger.Printf("[ERR] consul: Auditor: changeKV failed (for key %s): %v", args.DirEnt.Key, err)
			return err
		}
		s.logger.Printf("[DEBUG] consul: logged KV change for key: %s (version: %d)", args.DirEnt.Key, version)
	}

	return nil
}

// createConnectionString returns a connection string based on the configuration
func createConnectionString(c *structs.DBConfig) string {
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
func (s *Server) OpenAuditor(c *structs.DBConfig) error {

	connString := createConnectionString(c)
	s.logger.Printf("[DEBUG] consul: connection string to MSSQL DB: %s", connString)
	var err error
	db, err = sql.Open("mssql", connString)
	if err != nil {
		return err
	}
	return nil
}

// CloseAuditor closes the db connection
func (s *Server) CloseAuditor() error {
	if err := db.Close(); err != nil {
		return err
	}
	return nil
}
