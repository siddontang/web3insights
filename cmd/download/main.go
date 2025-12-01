package main

import (
	"context"
	"flag"
	"fmt"
	"os"
	"time"

	"github.com/siddon/web3insights/internal/awsdata"
	"github.com/siddon/web3insights/internal/config"
)

func main() {
	var (
		configFile = flag.String("config", "", "Path to config file (default: .config or value from WEB3INSIGHTS_CONFIG env var)")
		date       = flag.String("date", "", "Download data for a specific date (YYYY-MM-DD format, e.g., 2019-01-01)")
		startDate  = flag.String("start", "", "Start date for date range (YYYY-MM-DD format)")
		endDate    = flag.String("end", "", "End date for date range (YYYY-MM-DD format, inclusive)")
		chain      = flag.String("chain", "", "Blockchain to download (default: from config, currently supports: btc)")
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

	// Override chain from command line if provided
	if *chain != "" {
		cfg.Chain = *chain
	}

	// Validate that we have at least one date option
	if *date == "" && (*startDate == "" || *endDate == "") {
		fmt.Fprintf(os.Stderr, "Error: must specify either -date or both -start and -end\n")
		flag.Usage()
		os.Exit(1)
	}

	// Validate that date and date range are not both specified
	if *date != "" && (*startDate != "" || *endDate != "") {
		fmt.Fprintf(os.Stderr, "Error: cannot specify both -date and -start/-end\n")
		flag.Usage()
		os.Exit(1)
	}

	ctx := context.Background()

	// Handle single date
	if *date != "" {
		if err := downloadForDate(ctx, cfg, *date); err != nil {
			fmt.Fprintf(os.Stderr, "Error downloading for date %s: %v\n", *date, err)
			os.Exit(1)
		}
		return
	}

	// Handle date range
	if err := downloadForDateRange(ctx, cfg, *startDate, *endDate); err != nil {
		fmt.Fprintf(os.Stderr, "Error downloading date range: %v\n", err)
		os.Exit(1)
	}
}

// downloadForDate downloads data for a single date
func downloadForDate(ctx context.Context, cfg *config.Config, date string) error {
	if err := validateDate(date); err != nil {
		return fmt.Errorf("invalid date format: %w", err)
	}

	fmt.Printf("Downloading %s data for date: %s\n", cfg.Chain, date)

	switch cfg.Chain {
	case "bitcoin", "btc":
		return awsdata.DownloadBTC(ctx, cfg, date)
	default:
		return fmt.Errorf("unsupported chain: %s (currently only 'btc' or 'bitcoin' is supported)", cfg.Chain)
	}
}

// downloadForDateRange downloads data for a range of dates (inclusive)
func downloadForDateRange(ctx context.Context, cfg *config.Config, start, end string) error {
	if err := validateDate(start); err != nil {
		return fmt.Errorf("invalid start date format: %w", err)
	}
	if err := validateDate(end); err != nil {
		return fmt.Errorf("invalid end date format: %w", err)
	}

	startTime, err := time.Parse("2006-01-02", start)
	if err != nil {
		return fmt.Errorf("failed to parse start date: %w", err)
	}

	endTime, err := time.Parse("2006-01-02", end)
	if err != nil {
		return fmt.Errorf("failed to parse end date: %w", err)
	}

	if endTime.Before(startTime) {
		return fmt.Errorf("end date must be after or equal to start date")
	}

	fmt.Printf("Downloading %s data from %s to %s (inclusive)\n", cfg.Chain, start, end)

	// Iterate through each date in the range
	current := startTime
	for !current.After(endTime) {
		dateStr := current.Format("2006-01-02")
		fmt.Printf("\n--- Processing date: %s ---\n", dateStr)

		if err := downloadForDate(ctx, cfg, dateStr); err != nil {
			return fmt.Errorf("failed to download date %s: %w", dateStr, err)
		}

		// Move to next day
		current = current.AddDate(0, 0, 1)
	}

	fmt.Printf("\nSuccessfully downloaded data for all dates from %s to %s\n", start, end)
	return nil
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
