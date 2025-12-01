package chain

import (
	"encoding/binary"
	"fmt"
	"time"

	"github.com/parquet-go/parquet-go"
)

// Int96Timestamp represents an int96 timestamp from parquet as [12]byte
// Reference: https://github.com/fraugster/parquet-go/blob/master/int96_time.go
type Int96Timestamp [12]byte

const (
	jan011970 = 2440588 // Julian day for Jan 1, 1970
	secPerDay = 24 * 60 * 60
)

// jdToTime converts Julian day and nanoseconds to time.Time
func jdToTime(jd uint32, nsec uint64) time.Time {
	sec := int64(jd-jan011970) * secPerDay
	return time.Unix(sec, int64(nsec)).UTC()
}

// Time converts Int96Timestamp to time.Time
func (t Int96Timestamp) Time() time.Time {
	// Extract nanoseconds from first 8 bytes (little-endian)
	nano := binary.LittleEndian.Uint64(t[:8])
	// Extract Julian days from last 4 bytes (little-endian)
	days := binary.LittleEndian.Uint32(t[8:])
	// Check if zero
	if days == 0 && nano == 0 {
		return time.Time{}
	}
	return jdToTime(days, nano)
}

// String implements fmt.Stringer to format Int96Timestamp as DateTime string
func (t Int96Timestamp) String() string {
	ts := t.Time()
	if ts.IsZero() {
		return "<nil>"
	}
	return ts.Format("2006-01-02 15:04:05")
}

// UnmarshalParquet unmarshals an int96 timestamp from parquet
// int96 format: first 8 bytes are nanoseconds (little-endian), last 4 bytes are Julian days (little-endian)
// Reference: https://github.com/fraugster/parquet-go/blob/master/int96_time.go
func (t *Int96Timestamp) UnmarshalParquet(value parquet.Value) error {
	if value.IsNull() {
		*t = Int96Timestamp{}
		return nil
	}
	data := value.ByteArray()
	if len(data) < 12 {
		*t = Int96Timestamp{}
		return nil
	}
	// Copy the 12 bytes directly
	copy(t[:], data[:12])
	return nil
}

// BtcBlock represents a Bitcoin block from parquet
type BtcBlock struct {
	Date              string         `parquet:"date"`
	Hash              string         `parquet:"hash"`
	Size              int64          `parquet:"size,optional"`
	StrippedSize      int64          `parquet:"stripped_size,optional"`
	Weight            int64          `parquet:"weight,optional"`
	Number            int64          `parquet:"number"`
	Version           int32          `parquet:"version,optional"`
	MerkleRoot        string         `parquet:"merkle_root,optional"`
	Timestamp         Int96Timestamp `parquet:"timestamp,optional"`
	Nonce             int64          `parquet:"nonce,optional"`
	Bits              string         `parquet:"bits,optional"`
	CoinbaseParam     string         `parquet:"coinbase_param,optional"`
	TransactionCount  int64          `parquet:"transaction_count,optional"`
	Mediantime        Int96Timestamp `parquet:"mediantime,optional"`
	Difficulty        float64        `parquet:"difficulty,optional"`
	Chainwork         string         `parquet:"chainwork,optional"`
	Previousblockhash string         `parquet:"previousblockhash,optional"`
}

// String implements fmt.Stringer for BtcBlock with JSON-style pretty printing
func (b BtcBlock) String() string {
	return fmt.Sprintf(`BtcBlock {
  Date: %q
  Hash: %q
  Size: %d
  StrippedSize: %d
  Weight: %d
  Number: %d
  Version: %d
  MerkleRoot: %q
  Timestamp: %s
  Nonce: %d
  Bits: %q
  CoinbaseParam: %q
  TransactionCount: %d
  Mediantime: %s
  Difficulty: %f
  Chainwork: %q
  Previousblockhash: %q
}`,
		b.Date, b.Hash, b.Size, b.StrippedSize, b.Weight, b.Number, b.Version, b.MerkleRoot, b.Timestamp.String(), b.Nonce, b.Bits, b.CoinbaseParam, b.TransactionCount, b.Mediantime.String(), b.Difficulty, b.Chainwork, b.Previousblockhash)
}

// BtcTransaction represents a Bitcoin transaction from parquet
type BtcTransaction struct {
	Date           string                 `parquet:"date"`
	Hash           string                 `parquet:"hash"`
	Size           int64                  `parquet:"size,optional"`
	VirtualSize    int64                  `parquet:"virtual_size,optional"`
	Version        int64                  `parquet:"version,optional"`
	LockTime       int64                  `parquet:"lock_time,optional"`
	BlockHash      string                 `parquet:"block_hash"`
	BlockNumber    int64                  `parquet:"block_number"`
	BlockTimestamp Int96Timestamp         `parquet:"block_timestamp,optional"`
	Index          int64                  `parquet:"index"`
	InputCount     int64                  `parquet:"input_count,optional"`
	OutputCount    int64                  `parquet:"output_count,optional"`
	InputValue     float64                `parquet:"input_value,optional"`
	OutputValue    float64                `parquet:"output_value,optional"`
	IsCoinbase     bool                   `parquet:"is_coinbase,optional"`
	Fee            float64                `parquet:"fee,optional"`
	Inputs         []BtcTransactionInput  `parquet:"inputs,list,optional"`
	Outputs        []BtcTransactionOutput `parquet:"outputs,list,optional"`
}

// String implements fmt.Stringer for BtcTransaction with JSON-style pretty printing
func (t BtcTransaction) String() string {
	var inputsStr string
	if len(t.Inputs) == 0 {
		inputsStr = "[]"
	} else {
		inputsStr = "[\n"
		for i, input := range t.Inputs {
			inputsStr += fmt.Sprintf("    %d: %s", i, input.String())
			if i < len(t.Inputs)-1 {
				inputsStr += ",\n"
			} else {
				inputsStr += "\n"
			}
		}
		inputsStr += "  ]"
	}

	var outputsStr string
	if len(t.Outputs) == 0 {
		outputsStr = "[]"
	} else {
		outputsStr = "[\n"
		for i, output := range t.Outputs {
			outputsStr += fmt.Sprintf("    %d: %s", i, output.String())
			if i < len(t.Outputs)-1 {
				outputsStr += ",\n"
			} else {
				outputsStr += "\n"
			}
		}
		outputsStr += "  ]"
	}

	return fmt.Sprintf(`BtcTransaction {
  Date: %q
  Hash: %q
  Size: %d
  VirtualSize: %d
  Version: %d
  LockTime: %d
  BlockHash: %q
  BlockNumber: %d
  BlockTimestamp: %s
  Index: %d
  InputCount: %d
  OutputCount: %d
  InputValue: %f
  OutputValue: %f
  IsCoinbase: %t
  Fee: %f
  Inputs: %s
  Outputs: %s
}`,
		t.Date, t.Hash, t.Size, t.VirtualSize, t.Version, t.LockTime, t.BlockHash, t.BlockNumber, t.BlockTimestamp.String(), t.Index, t.InputCount, t.OutputCount, t.InputValue, t.OutputValue, t.IsCoinbase, t.Fee, inputsStr, outputsStr)
}

// BtcTransactionInput represents a transaction input from parquet
// This is a repeated group under "inputs" in the parquet schema
type BtcTransactionInput struct {
	SpentTransactionHash string  `parquet:"spent_transaction_hash,optional"`
	SpentOutputIndex     int64   `parquet:"spent_output_index,optional"`
	ScriptAsm            string  `parquet:"script_asm,optional"`
	ScriptHex            string  `parquet:"script_hex,optional"`
	Sequence             int64   `parquet:"sequence,optional"`
	RequiredSignatures   int64   `parquet:"required_signatures,optional"`
	Type                 string  `parquet:"type,optional"`
	Address              string  `parquet:"address,optional"`
	Value                float64 `parquet:"value,optional"`
}

// String implements fmt.Stringer for BtcTransactionInput with JSON-style pretty printing
func (i BtcTransactionInput) String() string {
	return fmt.Sprintf(`{
      SpentTransactionHash: %q
      SpentOutputIndex: %d
      ScriptAsm: %q
      ScriptHex: %q
      Sequence: %d
      RequiredSignatures: %d
      Type: %q
      Address: %q
      Value: %f
    }`,
		i.SpentTransactionHash, i.SpentOutputIndex, i.ScriptAsm, i.ScriptHex, i.Sequence, i.RequiredSignatures, i.Type, i.Address, i.Value)
}

// BtcTransactionOutput represents a transaction output from parquet
// This is a repeated group under "outputs" in the parquet schema
type BtcTransactionOutput struct {
	ScriptAsm          string  `parquet:"script_asm,optional"`
	ScriptHex          string  `parquet:"script_hex,optional"`
	RequiredSignatures int64   `parquet:"required_signatures,optional"`
	Type               string  `parquet:"type,optional"`
	Address            string  `parquet:"address,optional"`
	Value              float64 `parquet:"value,optional"`
}

// String implements fmt.Stringer for BtcTransactionOutput with JSON-style pretty printing
func (o BtcTransactionOutput) String() string {
	return fmt.Sprintf(`{
      ScriptAsm: %q
      ScriptHex: %q
      RequiredSignatures: %d
      Type: %q
      Address: %q
      Value: %f
    }`,
		o.ScriptAsm, o.ScriptHex, o.RequiredSignatures, o.Type, o.Address, o.Value)
}
