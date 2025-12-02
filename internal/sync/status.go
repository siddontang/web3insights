package sync

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"time"
)

// Status represents the sync status for a single parquet file
type Status struct {
	NumRows   int64     `json:"num_rows"`   // Total number of rows in the parquet file
	LastRow   int64     `json:"last_row"`   // Row number processed in this file
	UpdatedAt time.Time `json:"updated_at"` // When status was last updated
}

// IsComplete returns true if the file has been fully processed
func (s *Status) IsComplete() bool {
	return s.NumRows > 0 && s.LastRow >= s.NumRows
}

// LoadStatus loads sync status from file
func LoadStatus(statusPath string) (*Status, error) {
	data, err := os.ReadFile(statusPath)
	if err != nil {
		if os.IsNotExist(err) {
			// No status file exists, return empty status
			return &Status{}, nil
		}
		return nil, fmt.Errorf("failed to read status file: %w", err)
	}

	var status Status
	if err := json.Unmarshal(data, &status); err != nil {
		return nil, fmt.Errorf("failed to parse status file: %w", err)
	}

	return &status, nil
}

// SaveStatus saves sync status to file
func SaveStatus(statusPath string, status *Status) error {
	status.UpdatedAt = time.Now()

	// Ensure directory exists
	dir := filepath.Dir(statusPath)
	if err := os.MkdirAll(dir, 0755); err != nil {
		return fmt.Errorf("failed to create status directory: %w", err)
	}

	data, err := json.MarshalIndent(status, "", "  ")
	if err != nil {
		return fmt.Errorf("failed to marshal status: %w", err)
	}

	// Write to temp file first, then rename (atomic write)
	tmpPath := statusPath + ".tmp"
	if err := os.WriteFile(tmpPath, data, 0644); err != nil {
		return fmt.Errorf("failed to write status file: %w", err)
	}

	if err := os.Rename(tmpPath, statusPath); err != nil {
		return fmt.Errorf("failed to rename status file: %w", err)
	}

	return nil
}

// GetStatusPathForFile returns the status file path for a given parquet file
// Status file is saved in the same directory as the parquet file
func GetStatusPathForFile(parquetFilePath string) string {
	return parquetFilePath + ".status.json"
}
