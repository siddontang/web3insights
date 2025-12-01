# Web3Insights Web

A modern, beautiful web application for Bitcoin blockchain analytics powered by TiDB.

## Features

- 📊 Real-time blockchain statistics
- 📈 Interactive charts and visualizations
- 🔍 Recent blocks explorer
- 🎨 Modern, responsive UI
- ⚡ Fast API routes with TiDB

## Getting Started

### Prerequisites

- Node.js 20+ and npm
- TiDB Cloud database with Bitcoin data loaded
- **Note**: TiDB Cloud requires SSL/TLS connections (automatically configured)

### Environment Variables

Create a `.env.local` file in the `web` directory:

```env
TIDB_SQL_HOST=your-tidb-host.tidbcloud.com
TIDB_SQL_PORT=4000
TIDB_SQL_USER=your-username
TIDB_SQL_PASSWORD=your-password
TIDB_DATABASE=web3insights
```

**Important**: The connection automatically uses SSL/TLS as required by TiDB Cloud. Certificate verification is skipped for TiDB Cloud's self-signed certificates.

### Installation

```bash
cd web
npm install
```

### Development

```bash
npm run dev
```

Open [http://localhost:3000](http://localhost:3000) in your browser.

### Build

```bash
npm run build
npm start
```

## Deployment to Vercel

1. Push your code to GitHub
2. Import your repository in Vercel
3. Add environment variables in Vercel dashboard
4. Deploy!

The app will automatically build and deploy on every push to your main branch.

## Tech Stack

- **Next.js 15** - React framework
- **React 19** - UI library
- **TypeScript** - Type safety
- **Tailwind CSS** - Styling
- **Recharts** - Data visualization
- **Lucide React** - Icons
- **MySQL2** - TiDB connection with SSL/TLS support

