'use client';

import { useEffect, useState } from 'react';
import {
  Blocks,
  Coins,
  TrendingUp,
  Activity,
  ArrowUpRight,
  ExternalLink,
  Cloud,
} from 'lucide-react';
import StatCard from '@/components/StatCard';
import ChartCard from '@/components/ChartCard';
import BlockTable from '@/components/BlockTable';
import {
  LineChart,
  Line,
  AreaChart,
  Area,
  XAxis,
  YAxis,
  CartesianGrid,
  Tooltip,
  ResponsiveContainer,
  BarChart,
  Bar,
} from 'recharts';
import { formatNumber, formatBTC, formatDate } from '@/lib/utils';

interface Stats {
  blocks: number;
  transactions: number;
  totalVolume: number;
  totalFees: number;
  latestBlock: {
    number: number;
    hash: string;
    timestamp: Date;
    transactionCount: number;
  } | null;
  dateRange: {
    min: Date;
    max: Date;
  } | null;
}

interface DailyBlock {
  record_date: Date;
  block_count: number;
  total_transactions: number;
  avg_difficulty: number;
}

interface DailyTransaction {
  record_date: Date;
  transaction_count: number;
  total_volume: number;
  total_fees: number;
  avg_fee: number;
}

interface RecentBlock {
  number: number;
  hash: string;
  block_timestamp: Date;
  transaction_count: number;
  size: number;
  difficulty: number;
}

export default function Home() {
  const [stats, setStats] = useState<Stats | null>(null);
  const [dailyBlocks, setDailyBlocks] = useState<DailyBlock[]>([]);
  const [dailyTransactions, setDailyTransactions] = useState<DailyTransaction[]>([]);
  const [recentBlocks, setRecentBlocks] = useState<RecentBlock[]>([]);
  const [loading, setLoading] = useState(true);
  const [timeRange, setTimeRange] = useState<'1d' | '3d' | '5d' | '7d'>('1d');

  useEffect(() => {
    fetchData();
  }, [timeRange]);

  const fetchData = async () => {
    setLoading(true);
    try {
      // Fetch stats
      const statsRes = await fetch('/api/stats');
      const statsData = await statsRes.json();
      setStats(statsData);

      // Fetch blocks data
      const blocksRes = await fetch(`/api/blocks/daily?range=${timeRange}`);
      const blocksData = await blocksRes.json();
      setDailyBlocks(blocksData);

      // Fetch transactions data
      const txRes = await fetch(`/api/transactions/daily?range=${timeRange}`);
      const txData = await txRes.json();
      setDailyTransactions(txData);

      // Fetch recent blocks
      const recentRes = await fetch('/api/blocks/recent?limit=10');
      const recentData = await recentRes.json();
      setRecentBlocks(recentData);
    } catch (error) {
      console.error('Error fetching data:', error);
    } finally {
      setLoading(false);
    }
  };

  const formatChartDate = (date: Date | string) => {
    try {
      const d = typeof date === 'string' ? new Date(date) : date;
      if (isNaN(d.getTime())) return '';
      return d.toLocaleDateString('en-US', { month: 'short', day: 'numeric' });
    } catch {
      return '';
    }
  };

  const getTimeRangeLabel = () => {
    const labels: Record<typeof timeRange, string> = {
      '1d': '1 day',
      '3d': '3 days',
      '5d': '5 days',
      '7d': '7 days',
    };
    return labels[timeRange];
  };

  if (loading && !stats) {
    return (
      <div className="min-h-screen flex items-center justify-center">
        <div className="animate-spin rounded-full h-12 w-12 border-b-2 border-primary"></div>
      </div>
    );
  }

  return (
    <div className="min-h-screen bg-background">
      {/* Header */}
      <header className="border-b bg-card/50 backdrop-blur supports-[backdrop-filter]:bg-card/50">
        <div className="container mx-auto px-4 py-6">
          <div className="flex items-center justify-between">
            <div>
              <h1 className="text-3xl font-bold bg-gradient-to-r from-primary to-accent bg-clip-text text-transparent">
                Web3Insights
              </h1>
              <p className="text-sm text-muted-foreground mt-1">
                Bitcoin Blockchain Analytics
              </p>
            </div>
            <div className="flex items-center gap-3">
              <div className="flex gap-2">
                {(['1d', '3d', '5d', '7d'] as const).map((range) => (
                  <button
                    key={range}
                    onClick={() => setTimeRange(range)}
                    className={`px-4 py-2 rounded-md text-sm font-medium transition-colors ${
                      timeRange === range
                        ? 'bg-primary text-primary-foreground'
                        : 'bg-muted hover:bg-muted/80'
                    }`}
                  >
                    {range}
                  </button>
                ))}
              </div>
              <a
                href="https://tidbcloud.com/free-trial/?utm_source=sales_bdm&utm_medium=sales&utm_content=Siddon"
                target="_blank"
                rel="noopener noreferrer"
                className="flex items-center gap-2 px-4 py-2 bg-primary text-primary-foreground rounded-md text-sm font-medium hover:bg-primary/90 transition-colors shadow-sm"
              >
                <Cloud className="h-4 w-4" />
                Start TiDB Cloud
                <ExternalLink className="h-3 w-3" />
              </a>
            </div>
          </div>
        </div>
      </header>

      {/* Main Content */}
      <main className="container mx-auto px-4 py-8">
        {/* Stats Grid */}
        <div className="grid grid-cols-1 md:grid-cols-2 lg:grid-cols-4 gap-6 mb-8">
          <StatCard
            title="Total Blocks"
            value={stats ? formatNumber(stats.blocks) : '0'}
            icon={Blocks}
          />
          <StatCard
            title="Total Transactions"
            value={stats ? formatNumber(stats.transactions) : '0'}
            icon={Activity}
          />
          <StatCard
            title="Total Volume"
            value={stats ? formatBTC(stats.totalVolume) : '0 BTC'}
            icon={Coins}
          />
          <StatCard
            title="Total Fees"
            value={stats ? formatBTC(stats.totalFees) : '0 BTC'}
            icon={TrendingUp}
          />
        </div>

        {/* Charts Grid */}
        <div className="grid grid-cols-1 lg:grid-cols-2 gap-6 mb-8">
          <ChartCard
            title="Blocks per Day"
            description={`Block production over the last ${getTimeRangeLabel()}`}
          >
            <ResponsiveContainer width="100%" height={300}>
              <AreaChart data={dailyBlocks}>
                <defs>
                  <linearGradient id="colorBlocks" x1="0" y1="0" x2="0" y2="1">
                    <stop offset="5%" stopColor="#f7931a" stopOpacity={0.8} />
                    <stop offset="95%" stopColor="#f7931a" stopOpacity={0} />
                  </linearGradient>
                </defs>
                <CartesianGrid strokeDasharray="3 3" className="stroke-muted" />
                <XAxis
                  dataKey="record_date"
                  tickFormatter={formatChartDate}
                  className="text-xs"
                />
                <YAxis className="text-xs" />
                <Tooltip
                  labelFormatter={(value) => formatDate(value)}
                  contentStyle={{
                    backgroundColor: 'var(--card)',
                    border: '1px solid var(--border)',
                    borderRadius: '0.5rem',
                  }}
                />
                <Area
                  type="monotone"
                  dataKey="block_count"
                  stroke="#f7931a"
                  fillOpacity={1}
                  fill="url(#colorBlocks)"
                />
              </AreaChart>
            </ResponsiveContainer>
          </ChartCard>

          <ChartCard
            title="Transactions per Day"
            description={`Transaction volume over the last ${getTimeRangeLabel()}`}
          >
            <ResponsiveContainer width="100%" height={300}>
              <BarChart data={dailyTransactions}>
                <CartesianGrid strokeDasharray="3 3" className="stroke-muted" />
                <XAxis
                  dataKey="record_date"
                  tickFormatter={formatChartDate}
                  className="text-xs"
                />
                <YAxis className="text-xs" />
                <Tooltip
                  labelFormatter={(value) => formatDate(value)}
                  contentStyle={{
                    backgroundColor: 'var(--card)',
                    border: '1px solid var(--border)',
                    borderRadius: '0.5rem',
                  }}
                />
                <Bar dataKey="transaction_count" fill="#f7931a" radius={[4, 4, 0, 0]} />
              </BarChart>
            </ResponsiveContainer>
          </ChartCard>

          <ChartCard
            title="Transaction Volume"
            description={`BTC volume over the last ${getTimeRangeLabel()}`}
          >
            <ResponsiveContainer width="100%" height={300}>
              <LineChart data={dailyTransactions}>
                <CartesianGrid strokeDasharray="3 3" className="stroke-muted" />
                <XAxis
                  dataKey="record_date"
                  tickFormatter={formatChartDate}
                  className="text-xs"
                />
                <YAxis className="text-xs" />
                <Tooltip
                  labelFormatter={(value) => formatDate(value)}
                  formatter={(value: number) => formatBTC(value)}
                  contentStyle={{
                    backgroundColor: 'var(--card)',
                    border: '1px solid var(--border)',
                    borderRadius: '0.5rem',
                  }}
                />
                <Line
                  type="monotone"
                  dataKey="total_volume"
                  stroke="#ff6b35"
                  strokeWidth={2}
                  dot={false}
                />
              </LineChart>
            </ResponsiveContainer>
          </ChartCard>

          <ChartCard
            title="Average Transaction Fees"
            description={`Fee trends over the last ${getTimeRangeLabel()}`}
          >
            <ResponsiveContainer width="100%" height={300}>
              <AreaChart data={dailyTransactions}>
                <defs>
                  <linearGradient id="colorFees" x1="0" y1="0" x2="0" y2="1">
                    <stop offset="5%" stopColor="#ff6b35" stopOpacity={0.8} />
                    <stop offset="95%" stopColor="#ff6b35" stopOpacity={0} />
                  </linearGradient>
                </defs>
                <CartesianGrid strokeDasharray="3 3" className="stroke-muted" />
                <XAxis
                  dataKey="record_date"
                  tickFormatter={formatChartDate}
                  className="text-xs"
                />
                <YAxis className="text-xs" />
                <Tooltip
                  labelFormatter={(value) => formatDate(value)}
                  formatter={(value: number) => formatBTC(value)}
                  contentStyle={{
                    backgroundColor: 'var(--card)',
                    border: '1px solid var(--border)',
                    borderRadius: '0.5rem',
                  }}
                />
                <Area
                  type="monotone"
                  dataKey="avg_fee"
                  stroke="#ff6b35"
                  fillOpacity={1}
                  fill="url(#colorFees)"
                />
              </AreaChart>
            </ResponsiveContainer>
          </ChartCard>
        </div>

        {/* Recent Blocks Table */}
        <div className="mb-8">
          <BlockTable blocks={recentBlocks} />
        </div>
      </main>

      {/* Footer */}
      <footer className="border-t mt-12">
        <div className="container mx-auto px-4 py-6">
          <p className="text-center text-sm text-muted-foreground">
            Powered by TiDB • Data from Bitcoin Blockchain
          </p>
        </div>
      </footer>
    </div>
  );
}

