import mysql from 'mysql2/promise';

let pool: mysql.Pool | null = null;

export function getDbPool(): mysql.Pool {
  if (pool) {
    return pool;
  }

  const host = process.env.TIDB_SQL_HOST;
  const port = parseInt(process.env.TIDB_SQL_PORT || '4000', 10);
  const user = process.env.TIDB_SQL_USER;
  const password = process.env.TIDB_SQL_PASSWORD;
  const database = process.env.TIDB_DATABASE || 'web3insights';

  if (!host || !user || !password) {
    throw new Error('Missing TiDB connection credentials');
  }

  // TiDB Cloud requires TLS/SSL connections
  // TiDB Cloud uses self-signed certificates, so we need to skip verification
  pool = mysql.createPool({
    host,
    port,
    user,
    password,
    database,
    ssl: {
      rejectUnauthorized: false, // TiDB Cloud uses self-signed certificates
    },
    waitForConnections: true,
    connectionLimit: 10,
    queueLimit: 0,
    enableKeepAlive: true,
    keepAliveInitialDelay: 0,
  });

  return pool;
}

export async function query<T = any>(
  sql: string,
  params?: any[]
): Promise<T[]> {
  const pool = getDbPool();
  const [rows] = await pool.execute(sql, params);
  return rows as T[];
}

/**
 * Safely builds a LIMIT clause for SQL queries.
 * MySQL/TiDB doesn't support parameterized LIMIT values, so we validate
 * the limit and use it directly in the SQL string.
 * 
 * @param limit - The limit value (will be clamped between min and max)
 * @param min - Minimum allowed limit (default: 1)
 * @param max - Maximum allowed limit (default: 1000)
 * @returns A safe LIMIT clause string
 */
export function buildLimitClause(
  limit: number,
  min: number = 1,
  max: number = 1000
): string {
  const clamped = Math.max(min, Math.min(limit, max));
  // Ensure it's a valid integer
  const safeLimit = Math.floor(clamped);
  if (isNaN(safeLimit) || safeLimit < min) {
    return `LIMIT ${min}`;
  }
  return `LIMIT ${safeLimit}`;
}

