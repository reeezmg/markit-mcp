import crypto from 'crypto';
import { pool, COMPANY_ID } from '../db';

const cid = (args: { companyId?: string }) => args.companyId ?? COMPANY_ID;

export const statementTools = [
  // ─── Save Statement Rows ──────────────────────────────────────────────────────

  {
    name: 'save_statement_rows',
    description: 'Save extracted bank statement rows to DB. Creates a StatementBatch + all rows. Returns a batchId and link for the user to process on the statement page.',
    inputSchema: {
      type: 'object' as const,
      properties: {
        rows: {
          type: 'array',
          items: {
            type: 'object',
            properties: {
              sno: { type: 'number', description: 'Serial number / row number' },
              date: { type: 'string', description: 'Transaction date as shown in statement' },
              description: { type: 'string', description: 'Narration/remarks from statement' },
              debit: { type: 'number', description: 'Withdrawal amount (null if credit)' },
              credit: { type: 'number', description: 'Deposit amount (null if debit)' },
              balance: { type: 'number', description: 'Running balance (optional)' },
            },
            required: ['sno', 'date', 'description'],
          },
          description: 'Array of transaction rows extracted from the statement',
        },
        sourceFileName: { type: 'string', description: 'Original file name of the statement' },
        chatId: { type: 'string', description: 'AI chat ID to post done message after execution' },
        companyId: { type: 'string' },
        userId: { type: 'string' },
      },
      required: ['rows'],
    },
    handler: async (args: {
      rows: Array<{ sno: number; date: string; description: string; debit?: number; credit?: number; balance?: number }>;
      sourceFileName?: string; chatId?: string; companyId?: string; userId?: string;
    }) => {
      const companyId = cid(args);
      const userId = args.userId;
      if (!userId) return { error: 'userId is required' };
      if (!args.rows?.length) return { error: 'No rows provided' };

      const batchId = crypto.randomUUID();
      const client = await pool.connect();
      try {
        await client.query('BEGIN');

        // Create batch
        await client.query(
          `INSERT INTO statement_batches (id, company_id, user_id, chat_id, source_file_name, status, created_at)
           VALUES ($1, $2, $3, $4, $5, 'PENDING', now())`,
          [batchId, companyId, userId, args.chatId ?? null, args.sourceFileName ?? null]
        );

        // Insert all rows
        for (const row of args.rows) {
          const rowId = crypto.randomUUID();
          await client.query(
            `INSERT INTO statement_rows (id, batch_id, s_no, date, description, debit, credit, balance, executed)
             VALUES ($1, $2, $3, $4, $5, $6, $7, $8, false)`,
            [rowId, batchId, row.sno, row.date, row.description,
             row.debit ?? null, row.credit ?? null, row.balance ?? null]
          );
        }

        await client.query('COMMIT');
        return { success: true, batchId, rowCount: args.rows.length };
      } catch (err: any) {
        await client.query('ROLLBACK');
        throw err;
      } finally {
        client.release();
      }
    },
  },

  // ─── Find Statement Mappings ──────────────────────────────────────────────────

  {
    name: 'find_statement_mappings',
    description: 'Look up known remark→operation mappings for a statement batch. Auto-assigns operations to rows that match previously saved mappings. Returns how many were matched vs unmatched.',
    inputSchema: {
      type: 'object' as const,
      properties: {
        batchId: { type: 'string', description: 'Statement batch UUID' },
        companyId: { type: 'string' },
      },
      required: ['batchId'],
    },
    handler: async (args: { batchId: string; companyId?: string }) => {
      const companyId = cid(args);

      // Fetch all rows in this batch
      const { rows: stmtRows } = await pool.query(
        `SELECT id, description FROM statement_rows WHERE batch_id = $1 AND operation IS NULL`,
        [args.batchId]
      );

      if (!stmtRows.length) return { matched: 0, unmatched: 0, batchId: args.batchId };

      // Fetch all mappings for this company
      const { rows: mappings } = await pool.query(
        `SELECT remarks, operation, operation_meta, operation_label, user_input FROM statement_mappings WHERE company_id = $1`,
        [companyId]
      );

      let matched = 0;
      let unmatched = 0;

      for (const row of stmtRows) {
        const desc = (row.description || '').toLowerCase().trim();

        // Try exact match first, then 30% keyword overlap
        let bestMatch = null;
        let bestScore = 0;
        for (const m of mappings) {
          const mRemarks = (m.remarks || '').toLowerCase().trim();
          if (desc === mRemarks) {
            bestMatch = m;
            bestScore = 1;
            break;
          }
          // Split on spaces, slashes, hyphens, dots — common bank statement separators
          const keywords = mRemarks.split(/[\s\/\-\.]+/).filter((w: string) => w.length > 2);
          if (keywords.length === 0) continue;
          const matchedKw = keywords.filter((kw: string) => desc.includes(kw)).length;
          const score = matchedKw / keywords.length;
          // 30% threshold — catches same UPI ID, account number, company name
          if (score >= 0.3 && score > bestScore) {
            bestMatch = m;
            bestScore = score;
          }
        }

        if (bestMatch) {
          await pool.query(
            `UPDATE statement_rows SET operation = $2, operation_meta = $3, operation_label = $4, user_input = $5 WHERE id = $1`,
            [row.id, bestMatch.operation, bestMatch.operation_meta, bestMatch.operation_label, bestMatch.user_input ?? null]
          );
          matched++;
        } else {
          unmatched++;
        }
      }

      return { matched, unmatched, batchId: args.batchId };
    },
  },
];
