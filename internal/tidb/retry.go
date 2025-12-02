package tidb

import (
	"fmt"
	"time"
)

const (
	maxRetries = 3
	retryDelay = time.Second
)

// retryWithBackoff executes a function with retry logic
func retryWithBackoff[T any](fn func() (T, error), operation string) (T, error) {
	var result T
	var lastErr error

	for attempt := 1; attempt <= maxRetries; attempt++ {
		result, lastErr = fn()
		if lastErr == nil {
			return result, nil
		}

		if attempt < maxRetries {
			delay := retryDelay * time.Duration(attempt)
			fmt.Printf("Retrying %s (attempt %d/%d) after %v: %v\n", operation, attempt, maxRetries, delay, lastErr)
			time.Sleep(delay)
		}
	}

	return result, fmt.Errorf("%s failed after %d attempts: %w", operation, maxRetries, lastErr)
}

// retryWithBackoffNoReturn executes a function with retry logic (no return value)
func retryWithBackoffNoReturn(fn func() error, operation string) error {
	var lastErr error

	for attempt := 1; attempt <= maxRetries; attempt++ {
		lastErr = fn()
		if lastErr == nil {
			return nil
		}

		if attempt < maxRetries {
			delay := retryDelay * time.Duration(attempt)
			fmt.Printf("Retrying %s (attempt %d/%d) after %v: %v\n", operation, attempt, maxRetries, delay, lastErr)
			time.Sleep(delay)
		}
	}

	return fmt.Errorf("%s failed after %d attempts: %w", operation, maxRetries, lastErr)
}
