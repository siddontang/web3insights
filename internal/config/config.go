package config

import (
	"bufio"
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"time"
)

// Config holds all runtime configuration loaded from environment variables.
// This is intentionally minimal for the BTC MVP and can be extended later.
type Config struct {
	// General
	Env        string
	LogLevel   string
	DryRun     bool
	Chain      string
	MaxFiles   int
	MaxRetries int
	OutDir     string

	// Batch sizes for database inserts
	TransactionBatchSize int
	BlockBatchSize       int
	InputBatchSize       int
	OutputBatchSize      int

	// AWS Public Blockchain dataset
	AWSRegion      string
	AWSS3Bucket    string
	AWSS3BTCPrefix string

	// TiDB Cloud OpenAPI
	TiDBDatabase    string
	TiDBSQLHost     string
	TiDBSQLPort     int
	TiDBSQLUser     string
	TiDBSQLPassword string
}

// Load reads configuration from an optional INI-style .config file and then
// applies environment variables on top (env wins over file).
func Load() (*Config, error) {
	// 1) Load from .config file if present.
	path := os.Getenv("WEB3INSIGHTS_CONFIG")
	if path == "" {
		path = ".config"
	}
	return LoadFromPath(path)
}

// LoadFromPath reads configuration from a specific config file path and then
// applies environment variables on top (env wins over file).
func LoadFromPath(configPath string) (*Config, error) {
	cfg := &Config{}

	// 1) Load from config file if present.
	if err := loadFromINI(configPath, cfg); err != nil {
		return nil, err
	}

	// 2) Apply environment variables on top (env overrides file/defaults).
	if v := getEnv("WEB3INSIGHTS_ENV", ""); v != "" {
		cfg.Env = v
	}
	if v := getEnv("WEB3INSIGHTS_LOG_LEVEL", ""); v != "" {
		cfg.LogLevel = v
	}
	if isSet("WEB3INSIGHTS_DRY_RUN") {
		cfg.DryRun = getEnvBool("WEB3INSIGHTS_DRY_RUN", cfg.DryRun)
	}
	if v := getEnv("WEB3INSIGHTS_CHAIN", ""); v != "" {
		cfg.Chain = v
	}
	if isSet("WEB3INSIGHTS_MAX_FILES") {
		cfg.MaxFiles = getEnvInt("WEB3INSIGHTS_MAX_FILES", cfg.MaxFiles)
	}
	if isSet("WEB3INSIGHTS_MAX_RETRIES") {
		cfg.MaxRetries = getEnvInt("WEB3INSIGHTS_MAX_RETRIES", cfg.MaxRetries)
	}
	if v := getEnv("WEB3INSIGHTS_OUT_DIR", ""); v != "" {
		cfg.OutDir = v
	}

	if isSet("WEB3INSIGHTS_TRANSACTION_BATCH_SIZE") {
		cfg.TransactionBatchSize = getEnvInt("WEB3INSIGHTS_TRANSACTION_BATCH_SIZE", cfg.TransactionBatchSize)
	}
	if isSet("WEB3INSIGHTS_BLOCK_BATCH_SIZE") {
		cfg.BlockBatchSize = getEnvInt("WEB3INSIGHTS_BLOCK_BATCH_SIZE", cfg.BlockBatchSize)
	}
	if isSet("WEB3INSIGHTS_INPUT_BATCH_SIZE") {
		cfg.InputBatchSize = getEnvInt("WEB3INSIGHTS_INPUT_BATCH_SIZE", cfg.InputBatchSize)
	}
	if isSet("WEB3INSIGHTS_OUTPUT_BATCH_SIZE") {
		cfg.OutputBatchSize = getEnvInt("WEB3INSIGHTS_OUTPUT_BATCH_SIZE", cfg.OutputBatchSize)
	}

	if v := getEnv("WEB3INSIGHTS_AWS_REGION", ""); v != "" {
		cfg.AWSRegion = v
	}
	if v := getEnv("WEB3INSIGHTS_AWS_BUCKET", ""); v != "" {
		cfg.AWSS3Bucket = v
	}
	if v := getEnv("WEB3INSIGHTS_AWS_BTC_PREFIX", ""); v != "" {
		cfg.AWSS3BTCPrefix = v
	}

	if v := getEnv("TIDB_DATABASE", ""); v != "" {
		cfg.TiDBDatabase = v
	}
	if v := os.Getenv("TIDB_SQL_HOST"); v != "" {
		cfg.TiDBSQLHost = v
	}
	if isSet("TIDB_SQL_PORT") {
		cfg.TiDBSQLPort = getEnvInt("TIDB_SQL_PORT", cfg.TiDBSQLPort)
	}
	if v := os.Getenv("TIDB_SQL_USER"); v != "" {
		cfg.TiDBSQLUser = v
	}
	if v := os.Getenv("TIDB_SQL_PASSWORD"); v != "" {
		cfg.TiDBSQLPassword = v
	}

	// 3) Apply sane defaults for any unset fields.
	if cfg.Env == "" {
		cfg.Env = "dev"
	}
	if cfg.LogLevel == "" {
		cfg.LogLevel = "info"
	}
	if cfg.Chain == "" {
		cfg.Chain = "bitcoin"
	}
	if cfg.MaxFiles == 0 {
		cfg.MaxFiles = 10
	}
	if cfg.MaxRetries == 0 {
		cfg.MaxRetries = 3
	}
	if cfg.OutDir == "" {
		cfg.OutDir = "out"
	}
	if cfg.AWSRegion == "" {
		cfg.AWSRegion = "us-east-2"
	}
	if cfg.AWSS3Bucket == "" {
		cfg.AWSS3Bucket = "aws-public-blockchain"
	}
	if cfg.AWSS3BTCPrefix == "" {
		cfg.AWSS3BTCPrefix = "v1.0/btc/"
	}
	if cfg.TiDBDatabase == "" {
		cfg.TiDBDatabase = "web3insights"
	}
	if cfg.TiDBSQLPort == 0 {
		cfg.TiDBSQLPort = 4000
	}
	if cfg.TransactionBatchSize == 0 {
		cfg.TransactionBatchSize = 50
	}
	if cfg.BlockBatchSize == 0 {
		cfg.BlockBatchSize = 20
	}
	if cfg.InputBatchSize == 0 {
		cfg.InputBatchSize = 50
	}
	if cfg.OutputBatchSize == 0 {
		cfg.OutputBatchSize = 50
	}

	if cfg.TiDBSQLHost == "" || cfg.TiDBSQLUser == "" {
		// SQL connectivity is only required for DDL and fallback path,
		// but we enforce it up front to keep behaviour predictable.
		return nil, fmt.Errorf("missing TiDB SQL connection info (TIDB_SQL_HOST, TIDB_SQL_USER)")
	}

	return cfg, nil
}

func getEnv(key, def string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return def
}

func getEnvInt(key string, def int) int {
	if v := os.Getenv(key); v != "" {
		if i, err := strconv.Atoi(v); err == nil {
			return i
		}
	}
	return def
}

func getEnvBool(key string, def bool) bool {
	if v := os.Getenv(key); v != "" {
		switch v {
		case "1", "true", "TRUE", "True", "yes", "YES":
			return true
		case "0", "false", "FALSE", "False", "no", "NO":
			return false
		}
	}
	return def
}

func isSet(key string) bool {
	_, ok := os.LookupEnv(key)
	return ok
}

// loadFromINI parses a very small INI-like format:
//   - lines starting with # or ; are comments
//   - [section] headers are ignored (we treat keys as global)
//   - key = value pairs
//
// It is safe to call this with a non-existent path; it will just leave cfg
// unchanged in that case.
func loadFromINI(path string, cfg *Config) error {
	if path == "" {
		return nil
	}
	// Resolve relative to working directory.
	if !filepath.IsAbs(path) {
		if _, err := os.Stat(path); os.IsNotExist(err) {
			return nil
		}
	}

	f, err := os.Open(path)
	if err != nil {
		// If the file does not exist, silently ignore; otherwise return the error.
		if os.IsNotExist(err) {
			return nil
		}
		return err
	}
	defer f.Close()

	scanner := bufio.NewScanner(f)
	for scanner.Scan() {
		line := strings.TrimSpace(scanner.Text())
		if line == "" || strings.HasPrefix(line, "#") || strings.HasPrefix(line, ";") {
			continue
		}
		if strings.HasPrefix(line, "[") && strings.HasSuffix(line, "]") {
			// section header – ignored for now
			continue
		}
		parts := strings.SplitN(line, "=", 2)
		if len(parts) != 2 {
			continue
		}
		key := strings.TrimSpace(parts[0])
		value := strings.TrimSpace(parts[1])
		applyKeyValue(cfg, key, value)
	}
	return scanner.Err()
}

// applyKeyValue maps INI keys into Config fields.
func applyKeyValue(cfg *Config, key, value string) {
	switch key {
	case "env":
		cfg.Env = value
	case "log_level":
		cfg.LogLevel = value
	case "dry_run":
		cfg.DryRun = parseBool(value, cfg.DryRun)
	case "chain":
		cfg.Chain = value
	case "max_files":
		cfg.MaxFiles = parseInt(value, cfg.MaxFiles)
	case "max_retries":
		cfg.MaxRetries = parseInt(value, cfg.MaxRetries)
	case "out_dir":
		cfg.OutDir = value

	case "transaction_batch_size":
		cfg.TransactionBatchSize = parseInt(value, cfg.TransactionBatchSize)
	case "block_batch_size":
		cfg.BlockBatchSize = parseInt(value, cfg.BlockBatchSize)
	case "input_batch_size":
		cfg.InputBatchSize = parseInt(value, cfg.InputBatchSize)
	case "output_batch_size":
		cfg.OutputBatchSize = parseInt(value, cfg.OutputBatchSize)

	case "aws_region":
		cfg.AWSRegion = value
	case "aws_bucket":
		cfg.AWSS3Bucket = value
	case "aws_btc_prefix":
		cfg.AWSS3BTCPrefix = value

	case "tidb_database":
		cfg.TiDBDatabase = value
	case "tidb_sql_host":
		cfg.TiDBSQLHost = value
	case "tidb_sql_port":
		cfg.TiDBSQLPort = parseInt(value, cfg.TiDBSQLPort)
	case "tidb_sql_user":
		cfg.TiDBSQLUser = value
	case "tidb_sql_password":
		cfg.TiDBSQLPassword = value
	}
}

func parseInt(v string, def int) int {
	if v == "" {
		return def
	}
	i, err := strconv.Atoi(v)
	if err != nil {
		return def
	}
	return i
}

func parseBool(v string, def bool) bool {
	switch strings.ToLower(v) {
	case "1", "true", "yes", "y", "on":
		return true
	case "0", "false", "no", "n", "off":
		return false
	default:
		return def
	}
}

// DurationFromEnv reads a duration string (e.g. "5s", "1m") or returns a default.
func DurationFromEnv(key string, def time.Duration) time.Duration {
	if v := os.Getenv(key); v != "" {
		if d, err := time.ParseDuration(v); err == nil {
			return d
		}
	}
	return def
}
