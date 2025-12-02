package awsdata

import (
	"context"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"

	"github.com/aws/aws-sdk-go-v2/aws"
	awsconfig "github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/siddon/web3insights/internal/config"
)

// DownloadBTC downloads Bitcoin parquet files from AWS S3 for a given date.
// It downloads both blocks and transactions files to the configured output directory.
// The date should be in YYYY-MM-DD format (e.g., "2019-01-01").
// Uses unsigned requests for public bucket access (equivalent to --no-sign-request).
// It will always check S3 for new files and only download ones that don't exist locally.
func DownloadBTC(ctx context.Context, cfg *config.Config, date string) error {
	// Validate date format
	if len(date) != 10 || date[4] != '-' || date[7] != '-' {
		return fmt.Errorf("invalid date format, expected YYYY-MM-DD, got: %s", date)
	}

	// Load AWS config with anonymous credentials for public bucket access
	// Use AnonymousCredentials to allow unsigned requests for public buckets
	awsCfg, err := awsconfig.LoadDefaultConfig(ctx,
		awsconfig.WithRegion(cfg.AWSRegion),
		awsconfig.WithCredentialsProvider(
			aws.NewCredentialsCache(aws.AnonymousCredentials{}),
		),
	)
	if err != nil {
		return fmt.Errorf("failed to load AWS config: %w", err)
	}

	// Create S3 client
	// Note: For public buckets, the SDK will still attempt to sign requests
	// but with placeholder credentials. This may work if the bucket allows unsigned requests.
	// If it doesn't work, we may need to use a custom HTTP client approach.
	s3Client := s3.NewFromConfig(awsCfg)

	// Download blocks (idempotent: skips files that already exist locally)
	blocksPrefix := fmt.Sprintf("%sblocks/date=%s/", cfg.AWSS3BTCPrefix, date)
	if err := downloadBTCFiles(ctx, s3Client, cfg, blocksPrefix, "blocks", date); err != nil {
		return fmt.Errorf("failed to download blocks: %w", err)
	}

	// Download transactions (idempotent: skips files that already exist locally)
	transactionsPrefix := fmt.Sprintf("%stransactions/date=%s/", cfg.AWSS3BTCPrefix, date)
	if err := downloadBTCFiles(ctx, s3Client, cfg, transactionsPrefix, "transactions", date); err != nil {
		return fmt.Errorf("failed to download transactions: %w", err)
	}

	return nil
}

// checkFilesExist checks if a directory exists and contains at least one parquet file
func checkFilesExist(dir string) bool {
	if _, err := os.Stat(dir); os.IsNotExist(err) {
		return false
	}

	entries, err := os.ReadDir(dir)
	if err != nil || len(entries) == 0 {
		return false
	}

	// Check if at least one parquet file exists
	for _, entry := range entries {
		if !entry.IsDir() && strings.HasSuffix(entry.Name(), ".parquet") {
			return true
		}
	}

	return false
}

// downloadBTCFiles lists and downloads all Bitcoin parquet files from the given S3 prefix.
func downloadBTCFiles(ctx context.Context, s3Client *s3.Client, cfg *config.Config, s3Prefix, dataType, date string) error {
	// List objects in S3
	listInput := &s3.ListObjectsV2Input{
		Bucket: aws.String(cfg.AWSS3Bucket),
		Prefix: aws.String(s3Prefix),
	}

	// Create local directory
	localDir := filepath.Join(cfg.OutDir, "btc", dataType, date)
	if err := os.MkdirAll(localDir, 0755); err != nil {
		return fmt.Errorf("failed to create directory %s: %w", localDir, err)
	}

	var downloadedCount int
	paginator := s3.NewListObjectsV2Paginator(s3Client, listInput)

	for paginator.HasMorePages() {
		page, err := paginator.NextPage(ctx)
		if err != nil {
			return fmt.Errorf("failed to list objects: %w", err)
		}

		for _, obj := range page.Contents {
			// Skip if not a parquet file
			if !strings.HasSuffix(*obj.Key, ".snappy.parquet") {
				continue
			}

			// Extract filename from S3 key
			filename := filepath.Base(*obj.Key)
			localPath := filepath.Join(localDir, filename)

			// Skip if file already exists
			if _, err := os.Stat(localPath); err == nil {
				fmt.Printf("Skipping existing file: %s\n", localPath)
				continue
			}

			if cfg.DryRun {
				fmt.Printf("[DRY RUN] Would download: %s -> %s\n", *obj.Key, localPath)
				downloadedCount++
				continue
			}

			// Download file
			if err := downloadFile(ctx, s3Client, cfg, *obj.Key, localPath); err != nil {
				return fmt.Errorf("failed to download %s: %w", *obj.Key, err)
			}

			downloadedCount++
			fmt.Printf("Downloaded: %s\n", localPath)
		}
	}

	if cfg.DryRun {
		fmt.Printf("[DRY RUN] Would download %d files for %s/%s\n", downloadedCount, dataType, date)
	} else {
		fmt.Printf("Downloaded %d files for %s/%s\n", downloadedCount, dataType, date)
	}

	return nil
}

// downloadFile downloads a single file from S3.
func downloadFile(ctx context.Context, s3Client *s3.Client, cfg *config.Config, s3Key, localPath string) error {
	// Get object from S3
	getInput := &s3.GetObjectInput{
		Bucket: aws.String(cfg.AWSS3Bucket),
		Key:    aws.String(s3Key),
	}

	result, err := s3Client.GetObject(ctx, getInput)
	if err != nil {
		return fmt.Errorf("failed to get object from S3: %w", err)
	}
	defer result.Body.Close()

	// Create temporary file in the same directory as target
	targetDir := filepath.Dir(localPath)
	tmpFile, err := os.CreateTemp(targetDir, filepath.Base(localPath)+".tmp.*")
	if err != nil {
		return fmt.Errorf("failed to create temporary file: %w", err)
	}
	tmpPath := tmpFile.Name()

	// Ensure temp file is removed on failure
	defer func() {
		if _, err := os.Stat(tmpPath); err == nil {
			os.Remove(tmpPath)
		}
	}()

	// Copy content to temporary file
	_, err = io.Copy(tmpFile, result.Body)
	if err != nil {
		tmpFile.Close()
		return fmt.Errorf("failed to write to temporary file: %w", err)
	}

	// Close the temporary file
	if err := tmpFile.Close(); err != nil {
		return fmt.Errorf("failed to close temporary file: %w", err)
	}

	// Move temporary file to target location (atomic operation)
	if err := os.Rename(tmpPath, localPath); err != nil {
		return fmt.Errorf("failed to move temporary file to target: %w", err)
	}

	// Temp file has been moved, so it no longer exists at tmpPath
	// The defer cleanup will check and skip removal since the file is gone
	return nil
}
