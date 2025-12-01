import { NextResponse } from 'next/server';
import { query, buildLimitClause } from '@/lib/db';

export async function GET(request: Request) {
  try {
    const { searchParams } = new URL(request.url);
    const limit = parseInt(searchParams.get('limit') || '10', 10);

    // MySQL/TiDB doesn't support parameterized LIMIT, so we use a safe helper
    const limitClause = buildLimitClause(limit, 1, 100);

    const results = await query<{
      number: number;
      hash: string;
      block_timestamp: Date;
      transaction_count: number;
      size: number;
      difficulty: number;
    }>(
      `SELECT 
        number,
        hash,
        block_timestamp,
        transaction_count,
        size,
        difficulty
      FROM btc_blocks
      ORDER BY number DESC
      ${limitClause}`
    );

    return NextResponse.json(results);
  } catch (error) {
    console.error('Error fetching recent blocks:', error);
    return NextResponse.json(
      { error: 'Failed to fetch recent blocks' },
      { status: 500 }
    );
  }
}

