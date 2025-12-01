-- BTC Blocks Table
-- Schema based on AWS blockchain data and bitcoin-etl project
-- References:
-- - https://raw.githubusercontent.com/aws-solutions-library-samples/guidance-for-digital-assets-on-aws/main/analytics/consumer/schema/btc.md
-- - https://github.com/blockchain-etl/bitcoin-etl?tab=readme-ov-file#schema
--
-- Partitioned by date (YYYY-MM-DD) using TiDB automated monthly partitioning
-- Partitions automatically created from 2009-01 to 2109-01

CREATE TABLE IF NOT EXISTS `btc_blocks` (
  `record_date` DATE NOT NULL COMMENT 'Partition column (YYYY-MM-DD)',
  `hash` VARCHAR(80) NOT NULL COMMENT 'Hash of this block',
  `size` BIGINT NULL COMMENT 'The size of block data in bytes',
  `stripped_size` BIGINT NULL COMMENT 'The size of block data in bytes excluding witness data',
  `weight` BIGINT NULL COMMENT 'Three times the base size plus the total size',
  `number` BIGINT NOT NULL COMMENT 'The number of the block',
  `version` INT NULL COMMENT 'Protocol version specified in block header',
  `merkle_root` VARCHAR(80) NULL COMMENT 'The root node of a Merkle tree, where leaves are transaction hashes',
  `block_timestamp` TIMESTAMP NULL COMMENT 'Block creation timestamp specified in block header',
  `nonce` BIGINT NULL COMMENT 'Difficulty solution specified in block header',
  `bits` VARCHAR(32) NULL COMMENT 'Difficulty threshold specified in block header',
  `coinbase_param` TEXT NULL COMMENT 'Data specified in the coinbase transaction of this block',
  `transaction_count` BIGINT NULL COMMENT 'Number of transactions included in this block',
  `mediantime` TIMESTAMP NULL COMMENT 'Median time of previous blocks',
  `difficulty` DOUBLE NULL COMMENT 'Block difficulty',
  `chainwork` VARCHAR(128) NULL COMMENT 'Total work done by the chain',
  `previousblockhash` VARCHAR(80) NULL COMMENT 'Hash of the previous block',
  PRIMARY KEY (`record_date`, `hash`)
)
PARTITION BY RANGE COLUMNS(`record_date`)
INTERVAL (1 MONTH)
FIRST PARTITION LESS THAN ('2009-01-01')
LAST PARTITION LESS THAN ('2109-01-01')
MAXVALUE PARTITION;

-- BTC Transactions Table
-- Schema based on AWS blockchain data and bitcoin-etl project
-- Partitioned by date (YYYY-MM-DD) using TiDB automated monthly partitioning
-- Partitions automatically created from 2009-01 to 2109-01

CREATE TABLE IF NOT EXISTS `btc_transactions` (
  `record_date` DATE NOT NULL COMMENT 'Partition column (YYYY-MM-DD)',
  `hash` VARCHAR(80) NOT NULL COMMENT 'The hash of this transaction',
  `size` BIGINT NULL COMMENT 'The size of this transaction in bytes',
  `virtual_size` BIGINT NULL COMMENT 'The virtual transaction size (differs from size for witness transactions)',
  `version` BIGINT NULL COMMENT 'Protocol version specified in block which contained this transaction',
  `lock_time` BIGINT NULL COMMENT 'Earliest time that miners can include the transaction in their hashing of the Merkle root',
  `block_hash` VARCHAR(80) NOT NULL COMMENT 'Hash of the block which contains this transaction',
  `block_number` BIGINT NOT NULL COMMENT 'Number of the block which contains this transaction',
  `block_timestamp` TIMESTAMP NULL COMMENT 'Timestamp of the block which contains this transaction',
  `tx_index` BIGINT NOT NULL COMMENT 'The index of the transaction in the block',
  `input_count` BIGINT NULL COMMENT 'The number of inputs in the transaction',
  `output_count` BIGINT NULL COMMENT 'The number of outputs in the transaction',
  `input_value` DOUBLE NULL COMMENT 'Total value of inputs in the transaction (in BTC)',
  `output_value` DOUBLE NULL COMMENT 'Total value of outputs in the transaction (in BTC)',
  `is_coinbase` BOOLEAN NULL COMMENT 'True if this transaction is a coinbase transaction',
  `fee` DOUBLE NULL COMMENT 'The fee paid by this transaction',
  PRIMARY KEY (`record_date`, `hash`)
)
PARTITION BY RANGE COLUMNS(`record_date`)
INTERVAL (1 MONTH)
FIRST PARTITION LESS THAN ('2009-01-01')
LAST PARTITION LESS THAN ('2109-01-01')
MAXVALUE PARTITION;

-- BTC Transaction Inputs Table
-- Stores individual transaction inputs, denormalized from the nested inputs array
-- Schema based on AWS blockchain data and bitcoin-etl project
-- Partitioned by date (YYYY-MM-DD) using TiDB automated monthly partitioning
-- Partitions automatically created from 2009-01 to 2109-01

CREATE TABLE IF NOT EXISTS `btc_transaction_inputs` (
  `record_date` DATE NOT NULL COMMENT 'Partition column (YYYY-MM-DD)',
  `transaction_hash` VARCHAR(80) NOT NULL COMMENT 'The hash of the transaction this input belongs to',
  `input_index` BIGINT NOT NULL COMMENT '0 indexed number of an input within a transaction',
  `spent_transaction_hash` VARCHAR(80) NULL COMMENT 'The hash of the transaction which contains the output that this input spends',
  `spent_output_index` BIGINT NULL COMMENT 'The index of the output this input spends',
  `script_asm` TEXT NULL COMMENT 'Symbolic representation of the bitcoins script language op-codes',
  `script_hex` TEXT NULL COMMENT 'Hexadecimal representation of the bitcoins script language op-codes',
  `sequence` BIGINT NULL COMMENT 'A number intended to allow unconfirmed time-locked transactions to be updated before being finalized',
  `required_signatures` BIGINT NULL COMMENT 'The number of signatures required to authorize the spent output',
  `input_type` VARCHAR(32) NULL COMMENT 'The address type of the spent output',
  `address` VARCHAR(128) NULL COMMENT 'Address which owns the spent output',
  `spent_value` DOUBLE NULL COMMENT 'The value in BTC attached to the spent output',
  PRIMARY KEY (`record_date`, `transaction_hash`, `input_index`)
)
PARTITION BY RANGE COLUMNS(`record_date`)
INTERVAL (1 MONTH)
FIRST PARTITION LESS THAN ('2009-01-01')
LAST PARTITION LESS THAN ('2109-01-01')
MAXVALUE PARTITION;

-- BTC Transaction Outputs Table
-- Stores individual transaction outputs, denormalized from the nested outputs array
-- Schema based on AWS blockchain data and bitcoin-etl project
-- Partitioned by date (YYYY-MM-DD) using TiDB automated monthly partitioning
-- Partitions automatically created from 2009-01 to 2109-01

CREATE TABLE IF NOT EXISTS `btc_transaction_outputs` (
  `record_date` DATE NOT NULL COMMENT 'Partition column (YYYY-MM-DD)',
  `transaction_hash` VARCHAR(80) NOT NULL COMMENT 'The hash of the transaction this output belongs to',
  `output_index` BIGINT NOT NULL COMMENT '0 indexed number of an output within a transaction used by a later transaction to refer to that specific output',
  `script_asm` TEXT NULL COMMENT 'Symbolic representation of the bitcoins script language op-codes',
  `script_hex` TEXT NULL COMMENT 'Hexadecimal representation of the bitcoins script language op-codes',
  `required_signatures` BIGINT NULL COMMENT 'The number of signatures required to authorize spending of this output',
  `output_type` VARCHAR(32) NULL COMMENT 'The address type of the output',
  `address` VARCHAR(128) NULL COMMENT 'Address which owns this output',
  `output_amount` DOUBLE NULL COMMENT 'The value in BTC attached to this output',
  PRIMARY KEY (`record_date`, `transaction_hash`, `output_index`)
)
PARTITION BY RANGE COLUMNS(`record_date`)
INTERVAL (1 MONTH)
FIRST PARTITION LESS THAN ('2009-01-01')
LAST PARTITION LESS THAN ('2109-01-01')
MAXVALUE PARTITION;

