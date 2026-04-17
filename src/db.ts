import { Pool } from 'pg';
import * as dotenv from 'dotenv';
import { fileURLToPath } from 'url';
import path from 'path';

// Try multiple .env locations so it works regardless of CWD
dotenv.config({ path: path.resolve('mcp/.env') });
dotenv.config({ path: path.resolve('.env') });
dotenv.config({ path: path.resolve(__dirname, '.env') });
dotenv.config({ path: path.resolve(__dirname, '..', '.env') });

if (!process.env.DATABASE_URL) {
  throw new Error('DATABASE_URL environment variable is required');
}

export const pool = new Pool({
  connectionString: process.env.DATABASE_URL,
  max: 5,
  idleTimeoutMillis: 60000,
  connectionTimeoutMillis: 10000,
});

// Prevent unhandled pool errors from crashing the process (kills MCP stdio)
pool.on('error', (err) => {
  process.stderr.write(`Pool error (non-fatal): ${err.message}\n`);
});

// Warm up the pool in the background so the first tool call doesn't cold-start
export function warmUp() {
  pool.query('SELECT 1').catch(() => {
    // Retry once after 2s if Neon is cold-starting
    setTimeout(() => pool.query('SELECT 1').catch(() => {}), 2000);
  });
}

// Falls back to env var — MCP tools can override per call via args.companyId
export const COMPANY_ID = process.env.COMPANY_ID ?? '';
