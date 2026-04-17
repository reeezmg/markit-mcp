// Direct-import entry point for Nuxt server (no MCP protocol, no child process).
// Used in production (Vercel) instead of spawning mcp/dist/index.js over stdio.

import { productTools } from './tools/products';
import { purchaseOrderTools } from './tools/purchaseOrders';
import { catalogTools } from './tools/catalog';
import { expenseTools } from './tools/expenses';
import { financeTools } from './tools/finance';
import { accountTools } from './tools/accounts';
import { memoryTools } from './tools/memory';
import { distributorTools } from './tools/distributors';
import { statementTools } from './tools/statementMappings';
import { reportTools } from './tools/reports';
import { queryDbTools } from './tools/queryDb';
export const allTools = [
  ...productTools,
  ...purchaseOrderTools,
  ...catalogTools,
  ...expenseTools,
  ...financeTools,
  ...accountTools,
  ...memoryTools,
  ...distributorTools,
  ...statementTools,
  ...reportTools,
  ...queryDbTools,
];

const handlerMap = new Map<string, (args: any) => Promise<unknown>>(
  allTools.map(t => [t.name, t.handler as (args: any) => Promise<unknown>])
);

export async function callTool(name: string, args: unknown): Promise<unknown> {
  const handler = handlerMap.get(name);
  if (!handler) throw new Error(`Unknown tool: ${name}`);
  return handler(args);
}
