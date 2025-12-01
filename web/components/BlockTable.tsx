'use client';

import { formatDate, formatNumber } from '@/lib/utils';
import { ExternalLink } from 'lucide-react';

interface Block {
  number: number;
  hash: string;
  block_timestamp: Date;
  transaction_count: number;
  size: number;
  difficulty: number;
}

interface BlockTableProps {
  blocks: Block[];
}

export default function BlockTable({ blocks }: BlockTableProps) {
  return (
    <div className="rounded-lg border bg-card">
      <div className="p-6">
        <h3 className="text-lg font-semibold mb-4">Recent Blocks</h3>
        <div className="overflow-x-auto">
          <table className="w-full">
            <thead>
              <tr className="border-b">
                <th className="text-left py-3 px-4 text-sm font-medium text-muted-foreground">
                  Block
                </th>
                <th className="text-left py-3 px-4 text-sm font-medium text-muted-foreground">
                  Hash
                </th>
                <th className="text-left py-3 px-4 text-sm font-medium text-muted-foreground">
                  Time
                </th>
                <th className="text-right py-3 px-4 text-sm font-medium text-muted-foreground">
                  Transactions
                </th>
                <th className="text-right py-3 px-4 text-sm font-medium text-muted-foreground">
                  Size
                </th>
              </tr>
            </thead>
            <tbody>
              {blocks.map((block) => (
                <tr
                  key={block.number}
                  className="border-b hover:bg-muted/50 transition-colors"
                >
                  <td className="py-3 px-4 font-mono font-medium">
                    {formatNumber(block.number)}
                  </td>
                  <td className="py-3 px-4">
                    <div className="flex items-center gap-2">
                      <span className="font-mono text-sm">
                        {block.hash.slice(0, 16)}...
                      </span>
                      <a
                        href={`https://blockstream.info/block/${block.hash}`}
                        target="_blank"
                        rel="noopener noreferrer"
                        className="text-primary hover:underline"
                      >
                        <ExternalLink className="h-4 w-4" />
                      </a>
                    </div>
                  </td>
                  <td className="py-3 px-4 text-sm text-muted-foreground">
                    {formatDate(block.block_timestamp)}
                  </td>
                  <td className="py-3 px-4 text-right font-mono">
                    {formatNumber(block.transaction_count)}
                  </td>
                  <td className="py-3 px-4 text-right font-mono text-sm">
                    {formatNumber(block.size)} B
                  </td>
                </tr>
              ))}
            </tbody>
          </table>
        </div>
      </div>
    </div>
  );
}

