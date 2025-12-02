/**
 * Cache key generator for date-based queries
 */
export function getCacheKey(prefix: string, range: string): string {
  return `${prefix}:${range}`;
}

/**
 * Get today's date in YYYY-MM-DD format (UTC)
 */
export function getTodayDate(): string {
  const today = new Date();
  today.setUTCHours(0, 0, 0, 0);
  return today.toISOString().split('T')[0];
}

/**
 * Check if a date string is today
 */
export function isToday(dateString: string | Date): boolean {
  const date = typeof dateString === 'string' ? dateString : dateString.toISOString().split('T')[0];
  return date === getTodayDate();
}


/**
 * Create a cached query function for historical data
 * Caching has been disabled; this now just executes the query function.
 */
export function createCachedQuery<T>(
  queryFn: () => Promise<T[]>,
  _cacheKey: string
): Promise<T[]> {
  return queryFn();
}

