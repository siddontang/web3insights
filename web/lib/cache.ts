import { unstable_cache } from 'next/cache';

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
 * Historical data is cached for 24 hours since it doesn't change
 */
export function createCachedQuery<T>(
  queryFn: () => Promise<T[]>,
  cacheKey: string
): Promise<T[]> {
  return unstable_cache(
    async () => {
      return queryFn();
    },
    [cacheKey],
    {
      revalidate: 86400, // 24 hours - historical data doesn't change
      tags: [cacheKey],
    }
  )();
}

