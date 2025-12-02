import { NextResponse } from 'next/server';
import { query } from '@/lib/db';

export async function GET(request: Request) {
  try {
    const { searchParams } = new URL(request.url);
    const range = searchParams.get('range') || '1d';
    
    // Parse range: 1d, 3d, 5d, 7d
    const validRanges = ['1d', '3d', '5d', '7d'];
    const timeRange = validRanges.includes(range) ? range : '1d';
    
    // Extract days from range (e.g., '1d' -> 1, '3d' -> 3)
    const days = parseInt(timeRange);

    const results = await query<{
      record_date: Date;
      block_count: number;
      total_transactions: number;
      avg_difficulty: number;
    }>(
      `SELECT 
        record_date,
        COUNT(*) as block_count,
        SUM(transaction_count) as total_transactions,
        AVG(difficulty) as avg_difficulty
      FROM btc_blocks
      WHERE record_date >= DATE_SUB(CURDATE(), INTERVAL ? DAY)
      GROUP BY record_date
      ORDER BY record_date ASC`,
      [days]
    );

    return NextResponse.json(results);
  } catch (error) {
    console.error('Error fetching daily blocks:', error);
    return NextResponse.json(
      { error: 'Failed to fetch daily blocks data' },
      { status: 500 }
    );
  }
}

