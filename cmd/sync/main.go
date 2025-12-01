package main

import (
	"context"
	"flag"
	"fmt"
	"os"
	"path/filepath"
	"time"

	"github.com/siddon/web3insights/internal/awsdata"
	"github.com/siddon/web3insights/internal/config"
	"github.com/siddon/web3insights/internal/tidb"
)

func main() {
	var (
		configFile = flag.String("config", "", "Path to config file (default: .config or value from WEB3INSIGHTS_CONFIG env var)")
		date       = flag.String("date", "", "Date to sync (YYYY-MM-DD format, e.g., 2009-01-03)")
		startDate  = flag.String("start", "", "Start date for date range (YYYY-MM-DD format)")
		endDate    = flag.String("end", "", "End date for date range (YYYY-MM-DD format, inclusive)")
	)
	flag.Parse()

	// Load configuration
	var cfg *config.Config
	var err error
	if *configFile != "" {
		cfg, err = config.LoadFromPath(*configFile)
	} else {
		cfg, err = config.Load()
	}
	if err != nil {
		fmt.Fprintf(os.Stderr, "Error loading config: %v\n", err)
		os.Exit(1)
	}

	// Validate flags
	if *date != "" && (*startDate != "" || *endDate != "") {
		fmt.Fprintf(os.Stderr, "Error: cannot specify both -date and -start/-end\n")
		os.Exit(1)
	}
	if (*startDate != "" && *endDate == "") || (*startDate == "" && *endDate != "") {
		fmt.Fprintf(os.Stderr, "Error: both -start and -end must be specified for date range\n")
		os.Exit(1)
	}

	// Open database connection
	db, err := tidb.OpenSQL(cfg)
	if err != nil {
		fmt.Fprintf(os.Stderr, "Error connecting to TiDB: %v\n", err)
		os.Exit(1)
	}
	defer db.Close()

	ctx := context.Background()

	// Handle date or date range
	if *date != "" || (*startDate != "" && *endDate != "") {
		var dates []string
		if *date != "" {
			if err := validateDate(*date); err != nil {
				fmt.Fprintf(os.Stderr, "Error: %v\n", err)
				os.Exit(1)
			}
			dates = []string{*date}
		} else {
			if err := validateDate(*startDate); err != nil {
				fmt.Fprintf(os.Stderr, "Error: invalid start date: %v\n", err)
				os.Exit(1)
			}
			if err := validateDate(*endDate); err != nil {
				fmt.Fprintf(os.Stderr, "Error: invalid end date: %v\n", err)
				os.Exit(1)
			}

			startTime, err := time.Parse("2006-01-02", *startDate)
			if err != nil {
				fmt.Fprintf(os.Stderr, "Error: failed to parse start date: %v\n", err)
				os.Exit(1)
			}
			endTime, err := time.Parse("2006-01-02", *endDate)
			if err != nil {
				fmt.Fprintf(os.Stderr, "Error: failed to parse end date: %v\n", err)
				os.Exit(1)
			}
			if endTime.Before(startTime) {
				fmt.Fprintf(os.Stderr, "Error: end date must be after or equal to start date\n")
				os.Exit(1)
			}

			current := startTime
			for !current.After(endTime) {
				dates = append(dates, current.Format("2006-01-02"))
				current = current.AddDate(0, 0, 1)
			}
		}

		// Process each date: download if needed, then load
		for _, dateStr := range dates {
			fmt.Printf("\n--- Processing date: %s ---\n", dateStr)

			// Download files if needed (DownloadBTC checks if files exist)
			if err := awsdata.DownloadBTC(ctx, cfg, dateStr); err != nil {
				fmt.Fprintf(os.Stderr, "Error downloading data for date %s: %v\n", dateStr, err)
				os.Exit(1)
			}

			// Load all block files for this date
			blocksDir := filepath.Join(cfg.OutDir, "btc", "blocks", dateStr)
			fmt.Printf("Loading blocks for date %s...\n", dateStr)
			err = filepath.Walk(blocksDir, func(path string, info os.FileInfo, err error) error {
				if err != nil {
					return err
				}
				if info.IsDir() {
					return nil
				}
				if filepath.Ext(path) != ".parquet" {
					return nil
				}
				fmt.Printf("Loading block file: %s\n", path)
				return tidb.LoadBtcBlocks(db, path, cfg)
			})
			if err != nil {
				fmt.Fprintf(os.Stderr, "Error loading blocks for date %s: %v\n", dateStr, err)
				os.Exit(1)
			}

			// Load all transaction files for this date
			transactionsDir := filepath.Join(cfg.OutDir, "btc", "transactions", dateStr)
			fmt.Printf("Loading transactions for date %s...\n", dateStr)
			err = filepath.Walk(transactionsDir, func(path string, info os.FileInfo, err error) error {
				if err != nil {
					return err
				}
				if info.IsDir() {
					return nil
				}
				if filepath.Ext(path) != ".parquet" {
					return nil
				}
				fmt.Printf("Loading transaction file: %s\n", path)
				return tidb.LoadBtcTransactions(db, path, cfg)
			})
			if err != nil {
				fmt.Fprintf(os.Stderr, "Error loading transactions for date %s: %v\n", dateStr, err)
				os.Exit(1)
			}
		}

		fmt.Println("\nSuccessfully synced all dates to TiDB")
		return
	}

	// If no date specified, process all files from config directories
	blocksDir := filepath.Join(cfg.OutDir, "btc", "blocks")
	transactionsDir := filepath.Join(cfg.OutDir, "btc", "transactions")

	fmt.Printf("Loading all parquet files (block batch: %d, tx batch: %d, input batch: %d, output batch: %d)...\n",
		cfg.BlockBatchSize, cfg.TransactionBatchSize, cfg.InputBatchSize, cfg.OutputBatchSize)

	// Load all block files
	fmt.Println("Loading blocks from parquet files...")
	err = filepath.Walk(blocksDir, func(path string, info os.FileInfo, err error) error {
		if err != nil {
			return err
		}
		if info.IsDir() {
			return nil
		}
		if filepath.Ext(path) != ".parquet" {
			return nil
		}
		fmt.Printf("Loading block file: %s\n", path)
		return tidb.LoadBtcBlocks(db, path, cfg)
	})
	if err != nil {
		fmt.Fprintf(os.Stderr, "Error loading blocks: %v\n", err)
		os.Exit(1)
	}

	// Load all transaction files
	fmt.Println("Loading transactions from parquet files...")
	err = filepath.Walk(transactionsDir, func(path string, info os.FileInfo, err error) error {
		if err != nil {
			return err
		}
		if info.IsDir() {
			return nil
		}
		if filepath.Ext(path) != ".parquet" {
			return nil
		}
		fmt.Printf("Loading transaction file: %s\n", path)
		return tidb.LoadBtcTransactions(db, path, cfg)
	})
	if err != nil {
		fmt.Fprintf(os.Stderr, "Error loading transactions: %v\n", err)
		os.Exit(1)
	}

	fmt.Println("Successfully loaded all parquet files to TiDB")
}

// validateDate validates the date format (YYYY-MM-DD)
func validateDate(date string) error {
	if len(date) != 10 {
		return fmt.Errorf("date must be in YYYY-MM-DD format, got: %s", date)
	}
	if date[4] != '-' || date[7] != '-' {
		return fmt.Errorf("date must be in YYYY-MM-DD format, got: %s", date)
	}
	_, err := time.Parse("2006-01-02", date)
	if err != nil {
		return fmt.Errorf("invalid date: %w", err)
	}
	return nil
}
