import { NextResponse } from 'next/server';
import { query } from '@/lib/db';

export async function GET() {
  try {
    // Get total blocks count
    const blockCountResult = await query<{ count: number }>(
      'SELECT COUNT(*) as count FROM btc_blocks'
    );
    const blockCount = blockCountResult[0] || { count: 0 };

    // Get total transactions count
    const txCountResult = await query<{ count: number }>(
      'SELECT COUNT(*) as count FROM btc_transactions'
    );
    const txCount = txCountResult[0] || { count: 0 };

    // Get total transaction volume (sum of output_value)
    const txVolumeResult = await query<{ total: number }>(
      'SELECT COALESCE(SUM(output_value), 0) as total FROM btc_transactions'
    );
    const txVolume = txVolumeResult[0] || { total: 0 };

    // Get total fees
    const totalFeesResult = await query<{ total: number }>(
      'SELECT COALESCE(SUM(fee), 0) as total FROM btc_transactions WHERE fee IS NOT NULL'
    );
    const totalFees = totalFeesResult[0] || { total: 0 };

    // Get latest block
    const latestBlockResult = await query<{
      number: number;
      hash: string;
      block_timestamp: Date;
      transaction_count: number;
    }>(
      'SELECT number, hash, block_timestamp, transaction_count FROM btc_blocks ORDER BY number DESC LIMIT 1'
    );
    const latestBlock = latestBlockResult[0] || null;

    // Get date range
    const dateRangeResult = await query<{
      min_date: Date;
      max_date: Date;
    }>(
      'SELECT MIN(record_date) as min_date, MAX(record_date) as max_date FROM btc_blocks'
    );
    const dateRange = dateRangeResult[0] || null;

    return NextResponse.json({
      blocks: blockCount.count,
      transactions: txCount.count,
      totalVolume: txVolume.total,
      totalFees: totalFees.total,
      latestBlock: latestBlock
        ? {
            number: latestBlock.number,
            hash: latestBlock.hash,
            timestamp: latestBlock.block_timestamp,
            transactionCount: latestBlock.transaction_count,
          }
        : null,
      dateRange: dateRange
        ? {
            min: dateRange.min_date,
            max: dateRange.max_date,
          }
        : null,
    });
  } catch (error) {
    console.error('Error fetching stats:', error);
    return NextResponse.json(
      { error: 'Failed to fetch statistics' },
      { status: 500 }
    );
  }
}

