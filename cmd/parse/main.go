package main

import (
	"flag"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"time"

	"github.com/parquet-go/parquet-go"
	"github.com/siddon/web3insights/internal/chain"
	"github.com/siddon/web3insights/internal/config"
)

func main() {
	var (
		configFile = flag.String("config", "", "Path to config file (default: .config or value from WEB3INSIGHTS_CONFIG env var)")
		date       = flag.String("date", "", "Date to parse (YYYY-MM-DD format, e.g., 2009-01-03)")
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
	if *date == "" && *startDate == "" && *endDate == "" {
		fmt.Fprintf(os.Stderr, "Error: must specify either -date or -start/-end\n")
		os.Exit(1)
	}

	// Handle date or date range
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

	// Process each date
	for _, dateStr := range dates {
		fmt.Printf("\n=== Processing date: %s ===\n\n", dateStr)

		// Parse blocks
		blocksDir := filepath.Join(cfg.OutDir, "btc", "blocks", dateStr)
		if err := parseBlocks(blocksDir); err != nil {
			fmt.Fprintf(os.Stderr, "Error parsing blocks for date %s: %v\n", dateStr, err)
			// Continue to transactions even if blocks fail
		}

		// Parse transactions
		transactionsDir := filepath.Join(cfg.OutDir, "btc", "transactions", dateStr)
		if err := parseTransactions(transactionsDir); err != nil {
			fmt.Fprintf(os.Stderr, "Error parsing transactions for date %s: %v\n", dateStr, err)
			// Continue to next date even if transactions fail
		}
	}
}

func parseBlocks(blocksDir string) error {
	// Check if directory exists
	if _, err := os.Stat(blocksDir); os.IsNotExist(err) {
		fmt.Printf("Blocks directory does not exist: %s\n", blocksDir)
		return nil
	}

	return filepath.Walk(blocksDir, func(path string, info os.FileInfo, err error) error {
		if err != nil {
			return err
		}
		if info.IsDir() {
			return nil
		}
		if filepath.Ext(path) != ".parquet" {
			return nil
		}

		// Check file size
		if info.Size() == 0 {
			fmt.Printf("Skipping empty file: %s\n", path)
			return nil
		}

		fmt.Printf("--- Parsing block file: %s ---\n", path)

		// Use a recover to catch panics from parquet library
		var readErr error
		func() {
			file, err := os.Open(path)
			if err != nil {
				fmt.Fprintf(os.Stderr, "Failed to open file %s: %v\n", path, err)
				readErr = err
				return
			}
			defer file.Close()

			// Get file info for size
			fileInfo, err := file.Stat()
			if err != nil {
				fmt.Fprintf(os.Stderr, "Failed to get file info: %v\n", err)
				readErr = err
				return
			}

			// Build reader using SchemaOf
			schema := parquet.SchemaOf(chain.BtcBlock{})
			parquetFile, err := parquet.OpenFile(file, fileInfo.Size())
			if err != nil {
				fmt.Fprintf(os.Stderr, "Failed to open parquet file: %v\n", err)
				readErr = err
				return
			}
			reader := parquet.NewGenericReader[chain.BtcBlock](parquetFile, schema)
			defer reader.Close()

			// Read and print all blocks
			rowCount := 0
			for {
				rows := make([]chain.BtcBlock, 100) // Read in batches
				n, err := reader.Read(rows)

				if err != nil && err != io.EOF {
					fmt.Fprintf(os.Stderr, "Failed to read parquet file %s: %v\n", path, err)
					readErr = err
					return
				}

				if n == 0 {
					break
				}

				// Print each block
				for i := 0; i < n; i++ {
					fmt.Println(rows[i].String())
					fmt.Println()
					rowCount++
				}
			}

			if rowCount == 0 {
				fmt.Printf("File %s contains no rows\n", path)
			} else {
				fmt.Printf("Successfully parsed %d blocks from %s\n", rowCount, path)
			}
		}()

		if readErr != nil {
			// Error already printed, just continue
			return nil
		}

		return nil
	})
}

func parseTransactions(transactionsDir string) error {
	// Check if directory exists
	if _, err := os.Stat(transactionsDir); os.IsNotExist(err) {
		fmt.Printf("Transactions directory does not exist: %s\n", transactionsDir)
		return nil
	}

	return filepath.Walk(transactionsDir, func(path string, info os.FileInfo, err error) error {
		if err != nil {
			return err
		}
		if info.IsDir() {
			return nil
		}
		if filepath.Ext(path) != ".parquet" {
			return nil
		}

		// Check file size
		if info.Size() == 0 {
			fmt.Printf("Skipping empty file: %s\n", path)
			return nil
		}

		fmt.Printf("--- Parsing transaction file: %s ---\n", path)

		// Use a recover to catch panics from parquet library
		var readErr error
		func() {
			file, err := os.Open(path)
			if err != nil {
				fmt.Fprintf(os.Stderr, "Failed to open file %s: %v\n", path, err)
				readErr = err
				return
			}
			defer file.Close()

			// Get file info for size
			fileInfo, err := file.Stat()
			if err != nil {
				fmt.Fprintf(os.Stderr, "Failed to get file info: %v\n", err)
				readErr = err
				return
			}

			// Build reader using SchemaOf
			schema := parquet.SchemaOf(chain.BtcTransaction{})
			parquetFile, err := parquet.OpenFile(file, fileInfo.Size())
			if err != nil {
				fmt.Fprintf(os.Stderr, "Failed to open parquet file: %v\n", err)
				readErr = err
				return
			}

			fmt.Printf("Schema: %s\n", schema.String())
			reader := parquet.NewGenericReader[chain.BtcTransaction](parquetFile, schema)
			defer reader.Close()

			// Read and print all transactions
			rowCount := 0
			for {
				rows := make([]chain.BtcTransaction, 100) // Read in batches
				n, err := reader.Read(rows)

				if err != nil && err != io.EOF {
					fmt.Fprintf(os.Stderr, "Failed to read parquet file %s: %v\n", path, err)
					readErr = err
					return
				}

				if n == 0 {
					break
				}

				// Print each transaction
				for i := 0; i < n; i++ {
					fmt.Println(rows[i].String())
					fmt.Println()
					rowCount++
				}
			}

			if rowCount == 0 {
				fmt.Printf("File %s contains no rows\n", path)
			} else {
				fmt.Printf("Successfully parsed %d transactions from %s\n", rowCount, path)
			}
		}()

		if readErr != nil {
			// Error already printed, just continue
			return nil
		}

		return nil
	})
}

func validateDate(dateStr string) error {
	_, err := time.Parse("2006-01-02", dateStr)
	if err != nil {
		return fmt.Errorf("invalid date format: %s (expected YYYY-MM-DD)", dateStr)
	}
	return nil
}
