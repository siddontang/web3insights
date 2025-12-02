# Web3Insights

A complete Bitcoin blockchain analytics platform that ingests raw blockchain data from AWS Public Blockchain Datasets, processes it into a TiDB database, and provides a modern web dashboard for visualization and analysis.

## Overview

Web3Insights is a full-stack solution for Bitcoin blockchain analytics, consisting of:

- **Data Pipeline (Go)**: Command-line tools to download, parse, and sync Bitcoin blockchain data from AWS S3 into TiDB
- **Web Dashboard (Next.js)**: A modern, responsive web application for visualizing blockchain statistics, trends, and recent activity

The system is designed to handle large-scale blockchain data efficiently, with support for incremental syncing, resumable processing, and scalable database operations.

## Features

### Data Pipeline
- 📥 **Download**: Fetch Bitcoin blockchain data from AWS Public Blockchain Datasets (S3)
- 🔄 **Sync**: Load Parquet files into TiDB with progress tracking and resumable operations
- 🔍 **Parse**: Inspect and validate downloaded Parquet files
- ⚡ **Batch Processing**: Efficient batch inserts with configurable batch sizes
- 🔁 **Resumable**: Automatic progress tracking allows resuming interrupted syncs

### Web Dashboard
- 📊 **Real-time Statistics**: Total blocks, transactions, volume, and fees
- 📈 **Interactive Charts**: Daily block production, transaction counts, volume, and fees
- 🔍 **Recent Blocks Explorer**: Browse the latest blocks with detailed information
- 📅 **Time Range Selection**: View data for 7, 30, 90, or 365 days
- 🎨 **Modern UI**: Beautiful, responsive design with Tailwind CSS
- ⚡ **Fast API Routes**: Optimized queries with TiDB

## Architecture

```
┌─────────────────┐
│   AWS S3        │
│  (Blockchain    │
│   Datasets)     │
└────────┬────────┘
         │
         │ Download
         ▼
┌─────────────────┐
│  Parquet Files  │
│   (Local/out/)  │
└────────┬────────┘
         │
         │ Sync
         ▼
┌─────────────────┐
│   TiDB Cloud    │
│   (Database)    │
└────────┬────────┘
         │
         │ Query
         ▼
┌─────────────────┐
│  Next.js Web    │
│   Dashboard     │
└─────────────────┘
```

## Prerequisites

### For Data Pipeline
- Go 1.25.2 or later
- TiDB Cloud account (or self-hosted TiDB/MySQL)
- AWS credentials (for downloading from S3)

### For Web Dashboard
- Node.js 20+ and npm
- TiDB Cloud database with Bitcoin data loaded

## Installation

### 1. Clone the Repository

```bash
git clone <repository-url>
cd web3insights
```

### 2. Setup Data Pipeline

```bash
# Install Go dependencies
go mod tidy

# Build all commands
make all

# Or build individually
make download
make sync
make parse
```

### 3. Setup Web Dashboard

```bash
cd web
npm install
```

## Configuration

### Data Pipeline Configuration

Create a `.config` file in the project root (or set `WEB3INSIGHTS_CONFIG` environment variable):

```ini
# General settings
env = dev
log_level = info
chain = bitcoin
out_dir = ./out

# AWS S3 settings
aws_region = us-east-2
aws_bucket = aws-public-blockchain
aws_btc_prefix = v1.0/btc/

# TiDB settings
tidb_database = web3insights
tidb_sql_host = your-tidb-host.tidbcloud.com
tidb_sql_port = 4000
tidb_sql_user = your-username
tidb_sql_password = your-password

# Batch sizes (optional, defaults shown)
block_batch_size = 100
transaction_batch_size = 500
input_batch_size = 1000
output_batch_size = 1000
```

Alternatively, you can use environment variables (they override config file values):

```bash
export TIDB_SQL_HOST=your-tidb-host.tidbcloud.com
export TIDB_SQL_PORT=4000
export TIDB_SQL_USER=your-username
export TIDB_SQL_PASSWORD=your-password
export TIDB_DATABASE=web3insights
```

### Web Dashboard Configuration

Create a `.env.local` file in the `web` directory:

```env
TIDB_SQL_HOST=your-tidb-host.tidbcloud.com
TIDB_SQL_PORT=4000
TIDB_SQL_USER=your-username
TIDB_SQL_PASSWORD=your-password
TIDB_DATABASE=web3insights
```

**Note**: TiDB Cloud requires SSL/TLS connections, which are automatically configured. Certificate verification is skipped for TiDB Cloud's self-signed certificates.

## Usage

### Data Pipeline

#### Download Data

Download data for a specific date:
```bash
./bin/download -date 2024-01-01
```

Download data for a date range:
```bash
./bin/download -start 2024-01-01 -end 2024-01-31
```

#### Sync to TiDB

Sync a single date:
```bash
./bin/sync -date 2024-01-01
```

Sync a date range:
```bash
./bin/sync -start 2024-01-01 -end 2024-01-31
```

The sync command will:
1. Automatically download files if they don't exist locally
2. Load blocks and transactions into TiDB
3. Track progress and allow resuming interrupted syncs
4. Skip already completed files

#### Parse Files

Inspect downloaded Parquet files:
```bash
./bin/parse -date 2024-01-01
```

Parse a date range:
```bash
./bin/parse -start 2024-01-01 -end 2024-01-31
```

### Web Dashboard

#### Development

```bash
cd web
npm run dev
```

Open [http://localhost:3000](http://localhost:3000) in your browser.

#### Production Build

```bash
cd web
npm run build
npm start
```

## Data Sources

This project uses the [AWS Public Blockchain Datasets](https://registry.opendata.aws/blockchain/), specifically the Bitcoin dataset available at:
- **Bucket**: `aws-public-blockchain`
- **Region**: `us-east-2`
- **Prefix**: `v1.0/btc/`

The data is provided in Parquet format, organized by date.

## License

This project is licensed under the MIT License - see the [LICENSE](LICENSE) file for details.

