package tidb

import (
	"crypto/tls"
	"database/sql"
	"fmt"
	"time"

	"github.com/go-sql-driver/mysql" // Import registers the driver and provides mysql package

	"github.com/siddon/web3insights/internal/config"
)

// OpenSQL opens a sql.DB connection to TiDB Cloud using MySQL protocol with TLS.
func OpenSQL(cfg *config.Config) (*sql.DB, error) {
	// Register a TLS config that skips certificate verification for TiDB Cloud
	// TiDB Cloud uses self-signed certificates, so we need to skip verification
	err := mysql.RegisterTLSConfig("tidb-cloud", &tls.Config{
		InsecureSkipVerify: true,
	})
	if err != nil {
		return nil, fmt.Errorf("failed to register TLS config: %w", err)
	}

	// TiDB Cloud requires TLS connections
	dsn := fmt.Sprintf("%s:%s@tcp(%s:%d)/%s?charset=utf8mb4&parseTime=true&loc=UTC&tls=tidb-cloud",
		cfg.TiDBSQLUser,
		cfg.TiDBSQLPassword,
		cfg.TiDBSQLHost,
		cfg.TiDBSQLPort,
		cfg.TiDBDatabase,
	)
	db, err := sql.Open("mysql", dsn)
	if err != nil {
		return nil, err
	}
	db.SetConnMaxLifetime(5 * time.Minute)
	db.SetMaxIdleConns(5)
	db.SetMaxOpenConns(20)
	return db, nil
}
