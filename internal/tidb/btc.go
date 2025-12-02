package tidb

import (
	"database/sql"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/parquet-go/parquet-go"
	"github.com/siddon/web3insights/internal/chain"
	"github.com/siddon/web3insights/internal/config"
)

// inputRow represents a row to insert into btc_transaction_inputs
type inputRow struct {
	recordDate       time.Time
	transactionHash  string
	inputIndex       int64
	spentTxHash      string
	spentOutputIndex int64
	scriptAsm        string
	scriptHex        string
	sequence         int64
	requiredSigs     int64
	inputType        string
	address          string
	spentValue       float64
}

// outputRow represents a row to insert into btc_transaction_outputs
type outputRow struct {
	recordDate      time.Time
	transactionHash string
	outputIndex     int64
	scriptAsm       string
	scriptHex       string
	requiredSigs    int64
	outputType      string
	address         string
	outputAmount    float64
}

// buildValuesSQL builds a VALUES clause with the specified number of rows and placeholders per row
func buildValuesSQL(rowCount, placeholderCount int) string {
	if rowCount == 0 {
		return ""
	}

	// Build placeholders: "?, ?, ?, ?"
	placeholderSlice := make([]string, placeholderCount)
	for i := range placeholderSlice {
		placeholderSlice[i] = "?"
	}
	placeholders := strings.Join(placeholderSlice, ", ")
	rowPlaceholder := "(" + placeholders + ")"

	// Build rows: "(?, ?), (?, ?), (?, ?)"
	rowSlice := make([]string, rowCount)
	for i := range rowSlice {
		rowSlice[i] = rowPlaceholder
	}
	return strings.Join(rowSlice, ", ")
}

// extractArgsFunc is a function type that extracts SQL arguments from an item
type extractArgsFunc[T any] func(T) []interface{}

// batchInsertWithStmt executes a batch insert using a prepared statement with retry
func batchInsertWithStmt[T any](stmt *sql.Stmt, items []T, extractArgs extractArgsFunc[T]) error {
	if len(items) == 0 {
		return nil
	}

	args := make([]interface{}, 0, len(items)*20) // Estimate 20 args per item
	for _, item := range items {
		args = append(args, extractArgs(item)...)
	}

	return retryWithBackoffNoReturn(func() error {
		_, err := stmt.Exec(args...)
		if err != nil {
			return fmt.Errorf("failed to execute batch insert: %w", err)
		}
		return nil
	}, "batch insert")
}

// directInsert executes a direct SQL insert (not using prepared statement) with retry
func directInsert[T any](db *sql.DB, baseSQL string, items []T, extractArgs extractArgsFunc[T], placeholderCount int) error {
	if len(items) == 0 {
		return nil
	}

	valuesSQL := buildValuesSQL(len(items), placeholderCount)
	sql := baseSQL + valuesSQL

	args := make([]interface{}, 0, len(items)*placeholderCount)
	for _, item := range items {
		args = append(args, extractArgs(item)...)
	}

	return retryWithBackoffNoReturn(func() error {
		_, err := db.Exec(sql, args...)
		if err != nil {
			return fmt.Errorf("failed to execute direct insert: %w", err)
		}
		return nil
	}, "direct insert")
}

// ProgressCallback is called periodically during file processing to allow status updates
// filePath: path to the file being processed
// row: number of rows processed so far in this file
// numRows: total number of rows in the file
type ProgressCallback func(filePath string, row int64, numRows int64) error

// LoadBtcBlocks reads a block parquet file and inserts into btc_blocks table
func LoadBtcBlocks(db *sql.DB, filePath string, cfg *config.Config) error {
	return LoadBtcBlocksWithProgress(db, filePath, cfg, nil)
}

// LoadBtcBlocksWithProgress reads a block parquet file and inserts into btc_blocks table with progress callback
func LoadBtcBlocksWithProgress(db *sql.DB, filePath string, cfg *config.Config, onProgress ProgressCallback) error {
	return LoadBtcBlocksWithProgressAndRow(db, filePath, cfg, onProgress, 0)
}

// LoadBtcBlocksWithProgressAndRow reads a block parquet file and inserts with row
func LoadBtcBlocksWithProgressAndRow(db *sql.DB, filePath string, cfg *config.Config, onProgress ProgressCallback, startRow int64) error {
	return insertBlocksFromFile(db, filePath, cfg.BlockBatchSize, onProgress, startRow)
}

// LoadBtcTransactions reads a transaction parquet file and inserts into btc_transactions, btc_transaction_inputs, and btc_transaction_outputs tables
func LoadBtcTransactions(db *sql.DB, filePath string, cfg *config.Config) error {
	return LoadBtcTransactionsWithProgress(db, filePath, cfg, nil)
}

// LoadBtcTransactionsWithProgress reads a transaction parquet file and inserts with progress callback
func LoadBtcTransactionsWithProgress(db *sql.DB, filePath string, cfg *config.Config, onProgress ProgressCallback) error {
	return LoadBtcTransactionsWithProgressAndRow(db, filePath, cfg, onProgress, 0)
}

// LoadBtcTransactionsWithProgressAndRow reads a transaction parquet file and inserts with row
func LoadBtcTransactionsWithProgressAndRow(db *sql.DB, filePath string, cfg *config.Config, onProgress ProgressCallback, startRow int64) error {
	return insertTransactionsFromFile(db, filePath, cfg.TransactionBatchSize, cfg.InputBatchSize, cfg.OutputBatchSize, onProgress, startRow)
}

// extractBlockArgs extracts SQL arguments from a BtcBlock
func extractBlockArgs(block chain.BtcBlock) []interface{} {
	// Parse date string to time.Time
	date, err := time.Parse("2006-01-02", block.Date)
	if err != nil {
		// If parsing fails, use zero time (will be handled by database)
		date = time.Time{}
	}

	// Convert Int96Timestamp to time.Time for database (zero means NULL)
	var timestamp, mediantime interface{}
	timestampTime := block.Timestamp.Time()
	if timestampTime.IsZero() {
		timestamp = nil
	} else {
		timestamp = timestampTime
	}
	mediantimeTime := block.Mediantime.Time()
	if mediantimeTime.IsZero() {
		mediantime = nil
	} else {
		mediantime = mediantimeTime
	}

	return []interface{}{
		date,
		block.Hash,
		block.Size,
		block.StrippedSize,
		block.Weight,
		block.Number,
		block.Version,
		block.MerkleRoot,
		timestamp,
		block.Nonce,
		block.Bits,
		block.CoinbaseParam,
		block.TransactionCount,
		mediantime,
		block.Difficulty,
		block.Chainwork,
		block.Previousblockhash,
	}
}

// extractTransactionArgs extracts SQL arguments from a BtcTransaction
func extractTransactionArgs(tx chain.BtcTransaction) []interface{} {
	// Parse date string to time.Time
	date, err := time.Parse("2006-01-02", tx.Date)
	if err != nil {
		// If parsing fails, use zero time (will be handled by database)
		date = time.Time{}
	}

	// Convert Int96Timestamp to time.Time for database (zero means NULL)
	var blockTimestamp interface{}
	blockTimestampTime := tx.BlockTimestamp.Time()
	if blockTimestampTime.IsZero() {
		blockTimestamp = nil
	} else {
		blockTimestamp = blockTimestampTime
	}

	return []interface{}{
		date,
		tx.Hash,
		tx.Size,
		tx.VirtualSize,
		tx.Version,
		tx.LockTime,
		tx.BlockHash,
		tx.BlockNumber,
		blockTimestamp,
		tx.Index,
		tx.InputCount,
		tx.OutputCount,
		tx.InputValue,
		tx.OutputValue,
		tx.IsCoinbase,
		tx.Fee,
	}
}

// extractInputArgs extracts SQL arguments from an inputRow
func extractInputArgs(input inputRow) []interface{} {
	return []interface{}{
		input.recordDate,
		input.transactionHash,
		input.inputIndex,
		input.spentTxHash,
		input.spentOutputIndex,
		input.scriptAsm,
		input.scriptHex,
		input.sequence,
		input.requiredSigs,
		input.inputType,
		input.address,
		input.spentValue,
	}
}

// extractOutputArgs extracts SQL arguments from an outputRow
func extractOutputArgs(output outputRow) []interface{} {
	return []interface{}{
		output.recordDate,
		output.transactionHash,
		output.outputIndex,
		output.scriptAsm,
		output.scriptHex,
		output.requiredSigs,
		output.outputType,
		output.address,
		output.outputAmount,
	}
}

// insertBlocksFromFile reads a block parquet file and inserts into btc_blocks table
func insertBlocksFromFile(db *sql.DB, filePath string, batchSize int, onProgress ProgressCallback, startRow int64) error {
	file, err := os.Open(filePath)
	if err != nil {
		return fmt.Errorf("failed to open file %s: %w", filePath, err)
	}
	defer file.Close()

	fileInfo, err := file.Stat()
	if err != nil {
		return fmt.Errorf("failed to get file info: %w", err)
	}

	parquetFile, err := parquet.OpenFile(file, fileInfo.Size())
	if err != nil {
		return fmt.Errorf("failed to open parquet file: %w", err)
	}

	schema := parquet.SchemaOf(chain.BtcBlock{})
	reader := parquet.NewGenericReader[chain.BtcBlock](parquetFile, schema)
	defer reader.Close()

	// Get total number of rows in the file
	numRows := parquetFile.NumRows()

	// Seek to start row if resuming
	if startRow > 0 {
		if err := reader.SeekToRow(startRow); err != nil {
			return fmt.Errorf("failed to seek to row %d: %w", startRow, err)
		}
		fmt.Printf("Resuming from row %d/%d in %s\n", startRow, numRows, filepath.Base(filePath))
	}

	baseSQL := "INSERT IGNORE INTO btc_blocks (" +
		"record_date, hash, size, stripped_size, weight, number, version, merkle_root," +
		"block_timestamp, nonce, bits, coinbase_param, transaction_count, mediantime," +
		"difficulty, chainwork, previousblockhash" +
		") VALUES "

	// Prepare statement once for reuse
	valuesSQL := buildValuesSQL(batchSize, 17)
	batchSQL := baseSQL + valuesSQL

	stmt, err := retryWithBackoff(func() (*sql.Stmt, error) {
		return db.Prepare(batchSQL)
	}, "prepare block statement")
	if err != nil {
		return fmt.Errorf("failed to prepare statement: %w", err)
	}
	defer stmt.Close()

	pendingBlocks := make([]chain.BtcBlock, 0, batchSize)

	var totalRows int64 = startRow

	for {
		n, err := reader.Read(pendingBlocks[:batchSize])

		if err != nil && err != io.EOF {
			return fmt.Errorf("failed to read parquet file: %w", err)
		}

		pendingBlocks = pendingBlocks[:n]

		if n < batchSize || err == io.EOF {
			break
		}

		batch := pendingBlocks[:batchSize]

		if err := batchInsertWithStmt(stmt, batch, extractBlockArgs); err != nil {
			return fmt.Errorf("failed to insert block batch: %w", err)
		}

		totalRows += int64(len(batch))

		fmt.Printf("Inserted %d blocks from %s (total: %d/%d)\n", len(batch), filepath.Base(filePath), totalRows, numRows)

		// Call progress callback after each batch
		if onProgress != nil {
			if err := onProgress(filePath, totalRows, numRows); err != nil {
				fmt.Fprintf(os.Stderr, "Warning: progress callback failed: %v\n", err)
			}
		}
	}

	// Process remaining blocks with direct SQL
	if len(pendingBlocks) > 0 {
		if err := directInsert(db, baseSQL, pendingBlocks, extractBlockArgs, 17); err != nil {
			return fmt.Errorf("failed to insert remaining blocks: %w", err)
		}
		totalRows += int64(len(pendingBlocks))
		fmt.Printf("Inserted %d remaining blocks from %s (total: %d/%d)\n", len(pendingBlocks), filepath.Base(filePath), totalRows, numRows)

		// Call progress callback after remaining blocks (always save at end)
		if onProgress != nil {
			if err := onProgress(filePath, totalRows, numRows); err != nil {
				fmt.Fprintf(os.Stderr, "Warning: progress callback failed: %v\n", err)
			}
		}
	}

	return nil
}

// insertTransactionsFromFile reads a transaction parquet file and inserts into btc_transactions, btc_transaction_inputs, and btc_transaction_outputs tables
func insertTransactionsFromFile(db *sql.DB, filePath string, batchSize, inputBatchSize, outputBatchSize int, onProgress ProgressCallback, startRow int64) error {
	file, err := os.Open(filePath)
	if err != nil {
		return fmt.Errorf("failed to open file %s: %w", filePath, err)
	}
	defer file.Close()

	fileInfo, err := file.Stat()
	if err != nil {
		return fmt.Errorf("failed to get file info: %w", err)
	}

	parquetFile, err := parquet.OpenFile(file, fileInfo.Size())
	if err != nil {
		return fmt.Errorf("failed to open parquet file: %w", err)
	}

	schema := parquet.SchemaOf(chain.BtcTransaction{})
	reader := parquet.NewGenericReader[chain.BtcTransaction](parquetFile, schema)
	defer reader.Close()

	// Get total number of rows in the file
	numRows := parquetFile.NumRows()

	// Seek to start row if resuming
	if startRow > 0 {
		if err := reader.SeekToRow(startRow); err != nil {
			return fmt.Errorf("failed to seek to row %d: %w", startRow, err)
		}
		fmt.Printf("Resuming from row %d/%d in %s\n", startRow, numRows, filepath.Base(filePath))
	}

	// Prepare transaction statement once for reuse
	txBaseSQL := "INSERT IGNORE INTO btc_transactions (" +
		"record_date, hash, size, virtual_size, version, lock_time, block_hash, block_number," +
		"block_timestamp, tx_index, input_count, output_count, input_value, output_value," +
		"is_coinbase, fee" +
		") VALUES "

	txValuesSQL := buildValuesSQL(batchSize, 16)
	txBatchSQL := txBaseSQL + txValuesSQL

	txStmt, err := retryWithBackoff(func() (*sql.Stmt, error) {
		return db.Prepare(txBatchSQL)
	}, "prepare transaction statement")
	if err != nil {
		return fmt.Errorf("failed to prepare transaction statement: %w", err)
	}
	defer txStmt.Close()

	// Prepare input and output statements once for reuse
	inputBaseSQL := "INSERT IGNORE INTO btc_transaction_inputs (" +
		"record_date, transaction_hash, input_index, spent_transaction_hash, spent_output_index," +
		"script_asm, script_hex, sequence, required_signatures, input_type, address, spent_value" +
		") VALUES "

	outputBaseSQL := "INSERT IGNORE INTO btc_transaction_outputs (" +
		"record_date, transaction_hash, output_index, script_asm, script_hex, required_signatures," +
		"output_type, address, output_amount" +
		") VALUES "

	// Prepare statements for input/output batch sizes
	inputValuesSQL := buildValuesSQL(inputBatchSize, 12)
	inputBatchSQL := inputBaseSQL + inputValuesSQL
	inputStmt, err := retryWithBackoff(func() (*sql.Stmt, error) {
		return db.Prepare(inputBatchSQL)
	}, "prepare input statement")
	if err != nil {
		return fmt.Errorf("failed to prepare input statement: %w", err)
	}
	defer inputStmt.Close()

	outputValuesSQL := buildValuesSQL(outputBatchSize, 9)
	outputBatchSQL := outputBaseSQL + outputValuesSQL
	outputStmt, err := retryWithBackoff(func() (*sql.Stmt, error) {
		return db.Prepare(outputBatchSQL)
	}, "prepare output statement")
	if err != nil {
		return fmt.Errorf("failed to prepare output statement: %w", err)
	}
	defer outputStmt.Close()

	pendingTxs := make([]chain.BtcTransaction, batchSize)

	pendingInputs := make([]inputRow, 0, inputBatchSize)
	pendingOutputs := make([]outputRow, 0, outputBatchSize)

	var totalRows int64 = startRow

	for {
		n, err := reader.Read(pendingTxs[:batchSize])
		if err != nil && err != io.EOF {
			return fmt.Errorf("failed to read parquet file: %w", err)
		}

		pendingTxs = pendingTxs[:n]
		pendingInputs, pendingOutputs = collectTransactionData(pendingTxs[:n], pendingInputs, pendingOutputs)

		if len(pendingTxs) == batchSize {
			batch := pendingTxs[:batchSize]
			if err := batchInsertWithStmt(txStmt, batch, extractTransactionArgs); err != nil {
				return fmt.Errorf("failed to insert transaction batch: %w", err)
			}
		}

		inputNum := 0
		for len(pendingInputs) >= inputBatchSize {
			batch := pendingInputs[:inputBatchSize]
			pendingInputs = pendingInputs[inputBatchSize:]
			inputNum += inputBatchSize
			if err := batchInsertWithStmt(inputStmt, batch, extractInputArgs); err != nil {
				return fmt.Errorf("failed to insert input batch: %w", err)
			}
		}
		outputNum := 0
		for len(pendingOutputs) >= outputBatchSize {
			batch := pendingOutputs[:outputBatchSize]
			pendingOutputs = pendingOutputs[outputBatchSize:]
			outputNum += outputBatchSize
			if err := batchInsertWithStmt(outputStmt, batch, extractOutputArgs); err != nil {
				return fmt.Errorf("failed to insert output batch: %w", err)
			}
		}

		totalRows += int64(n)

		fmt.Printf("Inserted %d transactions, %d inputs, %d outputs from %s (total rows: %d/%d)\n", batchSize, inputNum, outputNum, filepath.Base(filePath), totalRows, numRows)

		// Call progress callback after each batch
		if onProgress != nil {
			if err := onProgress(filePath, totalRows, numRows); err != nil {
				fmt.Fprintf(os.Stderr, "Warning: progress callback failed: %v\n", err)
			}
		}

		if n < batchSize || err == io.EOF {
			break
		}
	}

	// Process remaining transactions - try batch first, then direct for remaining
	if len(pendingTxs) > 0 {
		if err := directInsert(db, txBaseSQL, pendingTxs, extractTransactionArgs, 16); err != nil {
			return fmt.Errorf("failed to insert remaining transactions: %w", err)
		}
		totalRows += int64(len(pendingTxs))
	}
	if len(pendingInputs) > 0 {
		if err := directInsert(db, inputBaseSQL, pendingInputs, extractInputArgs, 12); err != nil {
			return fmt.Errorf("failed to insert remaining inputs: %w", err)
		}
	}
	if len(pendingOutputs) > 0 {
		if err := directInsert(db, outputBaseSQL, pendingOutputs, extractOutputArgs, 9); err != nil {
			return fmt.Errorf("failed to insert remaining outputs: %w", err)
		}
	}
	if len(pendingTxs) > 0 || len(pendingInputs) > 0 || len(pendingOutputs) > 0 {
		fmt.Printf("Inserted remaining %d transactions, %d inputs, %d outputs from %s (total rows: %d/%d)\n", len(pendingTxs), len(pendingInputs), len(pendingOutputs), filepath.Base(filePath), totalRows, numRows)
	}

	// Call progress callback after remaining items (always save at end)
	if onProgress != nil {
		if err := onProgress(filePath, totalRows, numRows); err != nil {
			fmt.Fprintf(os.Stderr, "Warning: progress callback failed: %v\n", err)
		}
	}

	return nil
}

// collectTransactionData collects inputs and outputs from transactions
func collectTransactionData(transactions []chain.BtcTransaction, allInputs []inputRow, allOutputs []outputRow) ([]inputRow, []outputRow) {
	for _, tx := range transactions {
		// Parse date string to time.Time
		date, err := time.Parse("2006-01-02", tx.Date)
		if err != nil {
			// If parsing fails, use zero time (will be handled by database)
			date = time.Time{}
		}
		for i, input := range tx.Inputs {
			allInputs = append(allInputs, inputRow{
				recordDate:       date,
				transactionHash:  tx.Hash,
				inputIndex:       int64(i),
				spentTxHash:      input.SpentTransactionHash,
				spentOutputIndex: input.SpentOutputIndex,
				scriptAsm:        input.ScriptAsm,
				scriptHex:        input.ScriptHex,
				sequence:         input.Sequence,
				requiredSigs:     input.RequiredSignatures,
				inputType:        input.Type,
				address:          input.Address,
				spentValue:       input.Value,
			})
		}
		for i, output := range tx.Outputs {
			allOutputs = append(allOutputs, outputRow{
				recordDate:      date,
				transactionHash: tx.Hash,
				outputIndex:     int64(i),
				scriptAsm:       output.ScriptAsm,
				scriptHex:       output.ScriptHex,
				requiredSigs:    output.RequiredSignatures,
				outputType:      output.Type,
				address:         output.Address,
				outputAmount:    output.Value,
			})
		}
	}

	return allInputs, allOutputs
}
