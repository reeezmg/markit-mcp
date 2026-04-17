import crypto from 'crypto';
import { pool, COMPANY_ID } from '../db';

const cid = (args: { companyId?: string }) => args.companyId ?? COMPANY_ID;

export const accountTools = [
  // ─── Bank Accounts ──────────────────────────────────────────────────────────

  {
    name: 'list_bank_accounts',
    description: 'List all bank accounts for this company. Returns the primary bank (from company record) and secondary banks (from bank_accounts table), each labeled with type PRIMARY or SECONDARY.',
    inputSchema: {
      type: 'object' as const,
      properties: {
        companyId: { type: 'string' },
      },
    },
    handler: async (args: { companyId?: string }) => {
      const companyId = cid(args);

      // Primary bank from companies table
      const { rows: companyRows } = await pool.query(
        `SELECT bank_name, acc_holder_name, account_no, ifsc, gstin, upi_id, bank AS opening_balance
         FROM companies WHERE id = $1`,
        [companyId]
      );

      const primary = companyRows[0] ? {
        id: 'PRIMARY',
        type: 'PRIMARY',
        bankName: companyRows[0].bank_name,
        accHolderName: companyRows[0].acc_holder_name,
        accountNo: companyRows[0].account_no,
        ifsc: companyRows[0].ifsc,
        gstin: companyRows[0].gstin,
        upiId: companyRows[0].upi_id,
        openingBalance: Number(companyRows[0].opening_balance || 0),
      } : null;

      // Secondary banks
      const { rows: secondaryRows } = await pool.query(
        `SELECT id, bank_name, acc_holder_name, account_no, ifsc, gstin, upi_id,
                opening_balance::float, created_at
         FROM bank_accounts WHERE company_id = $1 ORDER BY created_at`,
        [companyId]
      );

      const secondary = secondaryRows.map(r => ({
        ...r,
        type: 'SECONDARY',
        openingBalance: Number(r.opening_balance || 0),
      }));

      return {
        accounts: [
          ...(primary ? [primary] : []),
          ...secondary,
        ],
        primaryCount: primary ? 1 : 0,
        secondaryCount: secondary.length,
      };
    },
  },

  {
    name: 'create_bank_account',
    description: 'Create a secondary bank account. All fields are optional except that at least bankName should be provided.',
    inputSchema: {
      type: 'object' as const,
      properties: {
        bankName: { type: 'string', description: 'Bank name' },
        accHolderName: { type: 'string', description: 'Account holder name' },
        accountNo: { type: 'string', description: 'Bank account number' },
        ifsc: { type: 'string', description: 'IFSC code' },
        gstin: { type: 'string', description: 'GST number' },
        upiId: { type: 'string', description: 'UPI ID' },
        openingBalance: { type: 'number', description: 'Opening balance (default 0)' },
        companyId: { type: 'string' },
      },
    },
    handler: async (args: {
      bankName?: string; accHolderName?: string; accountNo?: string; ifsc?: string;
      gstin?: string; upiId?: string; openingBalance?: number; companyId?: string;
    }) => {
      const companyId = cid(args);
      const id = crypto.randomUUID();

      await pool.query(
        `INSERT INTO bank_accounts (id, company_id, bank_name, acc_holder_name, account_no, ifsc, gstin, upi_id, opening_balance, created_at, updated_at)
         VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, now(), now())`,
        [id, companyId, args.bankName ?? null, args.accHolderName ?? null,
         args.accountNo ?? null, args.ifsc ?? null, args.gstin ?? null,
         args.upiId ?? null, args.openingBalance ?? 0]
      );

      return {
        success: true,
        bankAccountId: id,
        bankName: args.bankName,
        type: 'SECONDARY',
      };
    },
  },

  {
    name: 'update_bank_account',
    description: "Update a secondary bank account's details.",
    inputSchema: {
      type: 'object' as const,
      properties: {
        bankAccountId: { type: 'string', description: 'Bank account UUID (REQUIRED)' },
        bankName: { type: 'string' },
        accHolderName: { type: 'string' },
        accountNo: { type: 'string' },
        ifsc: { type: 'string' },
        gstin: { type: 'string' },
        upiId: { type: 'string' },
        openingBalance: { type: 'number' },
        companyId: { type: 'string' },
      },
      required: ['bankAccountId'],
    },
    handler: async (args: {
      bankAccountId: string; bankName?: string; accHolderName?: string; accountNo?: string;
      ifsc?: string; gstin?: string; upiId?: string; openingBalance?: number; companyId?: string;
    }) => {
      const companyId = cid(args);
      const sets: string[] = ['updated_at = now()'];
      const params: unknown[] = [args.bankAccountId, companyId];

      if (args.bankName !== undefined) { params.push(args.bankName); sets.push(`bank_name = $${params.length}`); }
      if (args.accHolderName !== undefined) { params.push(args.accHolderName); sets.push(`acc_holder_name = $${params.length}`); }
      if (args.accountNo !== undefined) { params.push(args.accountNo); sets.push(`account_no = $${params.length}`); }
      if (args.ifsc !== undefined) { params.push(args.ifsc); sets.push(`ifsc = $${params.length}`); }
      if (args.gstin !== undefined) { params.push(args.gstin); sets.push(`gstin = $${params.length}`); }
      if (args.upiId !== undefined) { params.push(args.upiId); sets.push(`upi_id = $${params.length}`); }
      if (args.openingBalance !== undefined) { params.push(args.openingBalance); sets.push(`opening_balance = $${params.length}`); }

      if (sets.length === 1) return { error: 'No fields to update' };

      await pool.query(
        `UPDATE bank_accounts SET ${sets.join(', ')} WHERE id = $1 AND company_id = $2`,
        params
      );

      const { rows } = await pool.query(
        `SELECT id, bank_name, acc_holder_name, account_no, ifsc, opening_balance::float
         FROM bank_accounts WHERE id = $1 AND company_id = $2`,
        [args.bankAccountId, companyId]
      );

      return { success: true, bankAccountId: args.bankAccountId, ...rows[0] };
    },
  },

  {
    name: 'delete_bank_account',
    description: 'Delete a secondary bank account. Fails if money transactions or account transfers reference it.',
    inputSchema: {
      type: 'object' as const,
      properties: {
        bankAccountId: { type: 'string', description: 'Bank account UUID' },
        companyId: { type: 'string' },
      },
      required: ['bankAccountId'],
    },
    handler: async (args: { bankAccountId: string; companyId?: string }) => {
      const companyId = cid(args);

      // Check for linked transactions
      const { rows: txLinked } = await pool.query(
        `SELECT COUNT(*)::int AS count FROM money_transactions WHERE account_id = $1 AND company_id = $2`,
        [args.bankAccountId, companyId]
      );
      if (txLinked[0].count > 0) {
        return { error: `Cannot delete — ${txLinked[0].count} money transaction(s) reference this bank account.` };
      }

      // Check for linked transfers
      const { rows: trLinked } = await pool.query(
        `SELECT COUNT(*)::int AS count FROM account_transfers
         WHERE company_id = $1 AND (from_account_id = $2 OR to_account_id = $2)`,
        [companyId, args.bankAccountId]
      );
      if (trLinked[0].count > 0) {
        return { error: `Cannot delete — ${trLinked[0].count} account transfer(s) reference this bank account.` };
      }

      const { rows } = await pool.query(
        `SELECT bank_name FROM bank_accounts WHERE id = $1 AND company_id = $2`,
        [args.bankAccountId, companyId]
      );
      if (!rows.length) return { error: `Bank account "${args.bankAccountId}" not found` };

      await pool.query(`DELETE FROM bank_accounts WHERE id = $1 AND company_id = $2`, [args.bankAccountId, companyId]);
      return { success: true, deletedBankAccountId: args.bankAccountId, bankName: rows[0].bank_name };
    },
  },

  // ─── Account Transfers ──────────────────────────────────────────────────────

  {
    name: 'list_account_transfers',
    description: 'List account transfers — internal fund movements between your own CASH, BANK, and INVESTMENT accounts. Do NOT use for external exchanges with a party — use money transactions for that.',
    inputSchema: {
      type: 'object' as const,
      properties: {
        fromType: { type: 'string', enum: ['CASH', 'BANK', 'INVESTMENT'], description: 'Filter by source account type' },
        toType: { type: 'string', enum: ['CASH', 'BANK', 'INVESTMENT'], description: 'Filter by destination account type' },
        fromDate: { type: 'string', description: 'Filter from this date (ISO string)' },
        toDate: { type: 'string', description: 'Filter up to this date (ISO string)' },
        limit: { type: 'number' },
        offset: { type: 'number' },
        companyId: { type: 'string' },
      },
    },
    handler: async (args: {
      fromType?: string; toType?: string; fromDate?: string; toDate?: string;
      limit?: number; offset?: number; companyId?: string;
    }) => {
      const companyId = cid(args);
      const limit = args.limit ?? 50;
      const offset = args.offset ?? 0;
      const params: unknown[] = [companyId, limit, offset];
      const wheres: string[] = ['at.company_id = $1'];

      if (args.fromType) { params.push(args.fromType); wheres.push(`at.from_type = $${params.length}`); }
      if (args.toType) { params.push(args.toType); wheres.push(`at.to_type = $${params.length}`); }
      if (args.fromDate) { params.push(args.fromDate); wheres.push(`at.created_at >= $${params.length}::timestamptz`); }
      if (args.toDate) { params.push(args.toDate); wheres.push(`at.created_at <= $${params.length}::timestamptz`); }

      const { rows } = await pool.query(
        `SELECT at.id, at.created_at, at.from_type, at.to_type, at.amount::float, at.note,
                at.from_account_id, at.to_account_id,
                fb.bank_name AS from_bank_name, tb.bank_name AS to_bank_name
         FROM account_transfers at
         LEFT JOIN bank_accounts fb ON fb.id = at.from_account_id
         LEFT JOIN bank_accounts tb ON tb.id = at.to_account_id
         WHERE ${wheres.join(' AND ')}
         ORDER BY at.created_at DESC
         LIMIT $2 OFFSET $3`,
        params
      );

      const countParams: unknown[] = [companyId];
      const countWheres: string[] = ['company_id = $1'];
      if (args.fromType) { countParams.push(args.fromType); countWheres.push(`from_type = $${countParams.length}`); }
      if (args.toType) { countParams.push(args.toType); countWheres.push(`to_type = $${countParams.length}`); }
      const { rows: countRows } = await pool.query(
        `SELECT COUNT(*)::int AS total FROM account_transfers WHERE ${countWheres.join(' AND ')}`,
        countParams
      );

      return { transfers: rows, total: countRows[0]?.total ?? rows.length };
    },
  },

  {
    name: 'create_account_transfer',
    description: `Create an account transfer — internal fund movement between own CASH, BANK, INVESTMENT accounts. Use for "transfer cash to bank", "move to investment". Do NOT use for external exchanges with a party — use create_money_transaction for that.

REQUIRED: fromType (CASH/BANK/INVESTMENT), toType (CASH/BANK/INVESTMENT), amount
OPTIONAL: fromAccountId (bank account UUID — needed when fromType=BANK for secondary bank, null = primary bank), toAccountId (same logic), note, date (ISO string, default now)`,
    inputSchema: {
      type: 'object' as const,
      properties: {
        fromType: { type: 'string', enum: ['CASH', 'BANK', 'INVESTMENT'], description: 'Source account type (REQUIRED)' },
        toType: { type: 'string', enum: ['CASH', 'BANK', 'INVESTMENT'], description: 'Destination account type (REQUIRED)' },
        amount: { type: 'number', description: 'Transfer amount (REQUIRED)' },
        fromAccountId: { type: 'string', description: 'Source bank account UUID (only for secondary banks, null = primary)' },
        toAccountId: { type: 'string', description: 'Destination bank account UUID (only for secondary banks, null = primary)' },
        note: { type: 'string', description: 'Optional note' },
        date: { type: 'string', description: 'Transfer date (ISO string). Defaults to now' },
        companyId: { type: 'string' },
      },
      required: ['fromType', 'toType', 'amount'],
    },
    handler: async (args: {
      fromType: string; toType: string; amount: number;
      fromAccountId?: string; toAccountId?: string; note?: string;
      date?: string; companyId?: string;
    }) => {
      const companyId = cid(args);
      const id = crypto.randomUUID();
      const createdAt = args.date ? new Date(args.date).toISOString() : new Date().toISOString();

      await pool.query(
        `INSERT INTO account_transfers (id, company_id, from_type, to_type, amount, from_account_id, to_account_id, note, created_at)
         VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9)`,
        [id, companyId, args.fromType, args.toType, args.amount,
         args.fromAccountId ?? null, args.toAccountId ?? null,
         args.note ?? null, createdAt]
      );

      return {
        success: true,
        transferId: id,
        fromType: args.fromType,
        toType: args.toType,
        amount: args.amount,
      };
    },
  },

  {
    name: 'update_account_transfer',
    description: "Update an account transfer (internal fund movement). Can change fromType, toType, account IDs, amount, or note. Note: AccountTransfer has no updatedAt field.",
    inputSchema: {
      type: 'object' as const,
      properties: {
        transferId: { type: 'string', description: 'Transfer UUID (REQUIRED)' },
        fromType: { type: 'string', enum: ['CASH', 'BANK', 'INVESTMENT'] },
        toType: { type: 'string', enum: ['CASH', 'BANK', 'INVESTMENT'] },
        amount: { type: 'number' },
        fromAccountId: { type: 'string', description: 'Source bank account UUID (null = primary)' },
        toAccountId: { type: 'string', description: 'Destination bank account UUID (null = primary)' },
        note: { type: 'string' },
        date: { type: 'string', description: 'Transfer date (ISO string)' },
        companyId: { type: 'string' },
      },
      required: ['transferId'],
    },
    handler: async (args: {
      transferId: string; fromType?: string; toType?: string; amount?: number;
      fromAccountId?: string; toAccountId?: string; note?: string;
      date?: string; companyId?: string;
    }) => {
      const companyId = cid(args);
      const sets: string[] = [];
      const params: unknown[] = [args.transferId, companyId];

      if (args.fromType !== undefined) { params.push(args.fromType); sets.push(`from_type = $${params.length}`); }
      if (args.toType !== undefined) { params.push(args.toType); sets.push(`to_type = $${params.length}`); }
      if (args.amount !== undefined) { params.push(args.amount); sets.push(`amount = $${params.length}`); }
      if (args.fromAccountId !== undefined) { params.push(args.fromAccountId || null); sets.push(`from_account_id = $${params.length}`); }
      if (args.toAccountId !== undefined) { params.push(args.toAccountId || null); sets.push(`to_account_id = $${params.length}`); }
      if (args.note !== undefined) { params.push(args.note); sets.push(`note = $${params.length}`); }
      if (args.date !== undefined) { params.push(new Date(args.date).toISOString()); sets.push(`created_at = $${params.length}`); }

      if (!sets.length) return { error: 'No fields to update' };

      await pool.query(
        `UPDATE account_transfers SET ${sets.join(', ')} WHERE id = $1 AND company_id = $2`,
        params
      );

      const { rows } = await pool.query(
        `SELECT id, from_type, to_type, amount::float, note, created_at
         FROM account_transfers WHERE id = $1 AND company_id = $2`,
        [args.transferId, companyId]
      );

      return { success: true, transferId: args.transferId, ...rows[0] };
    },
  },

  {
    name: 'delete_account_transfer',
    description: 'Delete an account transfer (internal fund movement) by ID.',
    inputSchema: {
      type: 'object' as const,
      properties: {
        transferId: { type: 'string', description: 'Transfer UUID' },
        companyId: { type: 'string' },
      },
      required: ['transferId'],
    },
    handler: async (args: { transferId: string; companyId?: string }) => {
      const companyId = cid(args);

      const { rows } = await pool.query(
        `SELECT from_type, to_type, amount::float FROM account_transfers WHERE id = $1 AND company_id = $2`,
        [args.transferId, companyId]
      );
      if (!rows.length) return { error: `Transfer "${args.transferId}" not found` };

      await pool.query(`DELETE FROM account_transfers WHERE id = $1 AND company_id = $2`, [args.transferId, companyId]);
      return {
        success: true,
        deletedTransferId: args.transferId,
        fromType: rows[0].from_type,
        toType: rows[0].to_type,
        amount: rows[0].amount,
      };
    },
  },

  // ─── Ledgers (Read-Only) ────────────────────────────────────────────────────

  {
    name: 'get_cash_ledger',
    description: `Get the cash account ledger with running balance. Shows sales (cash), expenses (cash), money transactions (cash), and account transfers affecting cash. Returns opening balance, ledger rows (date, source, description, debit, credit, runningBalance), and closing balance. Filter by date range.`,
    inputSchema: {
      type: 'object' as const,
      properties: {
        fromDate: { type: 'string', description: 'Start date (ISO string). Defaults to all time' },
        toDate: { type: 'string', description: 'End date (ISO string). Defaults to now' },
        companyId: { type: 'string' },
      },
    },
    handler: async (args: { fromDate?: string; toDate?: string; companyId?: string }) => {
      const companyId = cid(args);
      const from = args.fromDate ? new Date(args.fromDate) : new Date('1970-01-01');
      const to = args.toDate ? new Date(args.toDate) : new Date();
      const cleanup = false; // MCP doesn't have session cleanup flag

      const client = await pool.connect();
      try {
        const rows: any[] = [];

        // Base opening from company cash field
        const cashRes = await client.query(`SELECT cash FROM companies WHERE id = $1`, [companyId]);
        const baseOpening = Number(cashRes.rows[0]?.cash || 0);

        // Opening sums (before from date)
        const salesBeforeRes = await client.query(
          `WITH split AS (
            SELECT (elem->>'amount')::numeric AS amount
            FROM bills b
            JOIN LATERAL jsonb_array_elements(
              CASE WHEN jsonb_typeof(b.split_payments::jsonb) = 'array' THEN b.split_payments::jsonb ELSE '[]'::jsonb END
            ) elem ON true
            WHERE b.company_id = $1 AND b.payment_method = 'Split' AND (elem->>'method') = 'Cash'
              AND b.deleted = false AND b.payment_status IN ('PAID','PENDING') AND b.is_markit = false
              AND b.created_at < $2 AND ($3 = true OR b.precedence IS NOT TRUE)
          )
          SELECT COALESCE(SUM(CASE WHEN payment_method = 'Cash' THEN grand_total ELSE 0 END), 0)
            + COALESCE((SELECT SUM(amount) FROM split), 0) AS total
          FROM bills WHERE company_id = $1 AND deleted = false AND payment_status IN ('PAID','PENDING')
            AND is_markit = false AND created_at < $2 AND ($3 = true OR precedence IS NOT TRUE)`,
          [companyId, from, cleanup]
        );
        const salesBefore = Number(salesBeforeRes.rows[0].total || 0);

        const expBeforeRes = await client.query(
          `SELECT COALESCE(SUM(total_amount), 0) AS total FROM expenses
           WHERE company_id = $1 AND payment_mode = 'CASH' AND UPPER(status) = 'PAID' AND expense_date < $2`,
          [companyId, from]
        );
        const expensesBefore = Number(expBeforeRes.rows[0].total || 0);

        const moneyBeforeRes = await client.query(
          `SELECT COALESCE(SUM(CASE WHEN direction = 'RECEIVED' THEN amount ELSE -amount END), 0) AS net
           FROM money_transactions WHERE company_id = $1 AND payment_mode = 'CASH' AND status = 'PAID' AND created_at < $2`,
          [companyId, from]
        );
        const moneyNetBefore = Number(moneyBeforeRes.rows[0].net || 0);

        const transferBeforeRes = await client.query(
          `SELECT COALESCE(SUM(CASE WHEN to_type = 'CASH' THEN amount ELSE 0 END), 0)
            - COALESCE(SUM(CASE WHEN from_type = 'CASH' THEN amount ELSE 0 END), 0) AS net
           FROM account_transfers WHERE company_id = $1 AND created_at < $2
            AND (from_type = 'CASH' OR to_type = 'CASH')`,
          [companyId, from]
        );
        const transferNetBefore = Number(transferBeforeRes.rows[0].net || 0);

        const openingBalance = baseOpening + salesBefore - expensesBefore + moneyNetBefore + transferNetBefore;

        rows.push({ date: from, source: 'OPENING', ref: '-', description: 'Opening Balance', debit: 0, credit: openingBalance });

        // Ledger rows in period
        // Sales (cash)
        const salesRes = await client.query(
          `WITH split AS (
            SELECT b.created_at, b.invoice_number, (elem->>'method') AS method, (elem->>'amount')::numeric AS amount
            FROM bills b
            JOIN LATERAL jsonb_array_elements(
              CASE WHEN jsonb_typeof(b.split_payments::jsonb) = 'array' THEN b.split_payments::jsonb ELSE '[]'::jsonb END
            ) elem ON true
            WHERE b.company_id = $1 AND b.payment_method = 'Split' AND b.deleted = false
              AND b.payment_status IN ('PAID','PENDING') AND b.is_markit = false
              AND b.created_at BETWEEN $2 AND $3 AND ($4 = true OR b.precedence IS NOT TRUE)
          )
          SELECT b.created_at AS date, 'SALE' AS source, b.invoice_number::text AS ref,
            'Sale via Cash' AS description, 0 AS debit, b.grand_total AS credit
          FROM bills b WHERE b.company_id = $1 AND b.payment_method = 'Cash' AND b.deleted = false
            AND b.payment_status IN ('PAID','PENDING') AND b.is_markit = false
            AND b.created_at BETWEEN $2 AND $3 AND ($4 = true OR b.precedence IS NOT TRUE)
          UNION ALL
          SELECT s.created_at, 'SALE', s.invoice_number::text, 'Sale via Cash (Split)', 0, s.amount
          FROM split s WHERE s.method = 'Cash'`,
          [companyId, from, to, cleanup]
        );
        rows.push(...salesRes.rows);

        // Expenses (cash)
        const expRes = await client.query(
          `SELECT expense_date AS date, 'EXPENSE' AS source, id::text AS ref,
            'Expense paid by cash' AS description, total_amount AS debit, 0 AS credit
           FROM expenses WHERE company_id = $1 AND payment_mode = 'CASH' AND UPPER(status) = 'PAID'
            AND expense_date BETWEEN $2 AND $3`,
          [companyId, from, to]
        );
        rows.push(...expRes.rows);

        // Money transactions (cash)
        const moneyRes = await client.query(
          `SELECT created_at AS date, 'TRANSACTION' AS source, id::text AS ref,
            direction || ' via cash' AS description,
            CASE WHEN direction = 'GIVEN' THEN amount ELSE 0 END AS debit,
            CASE WHEN direction = 'RECEIVED' THEN amount ELSE 0 END AS credit
           FROM money_transactions WHERE company_id = $1 AND payment_mode = 'CASH' AND status = 'PAID'
            AND created_at BETWEEN $2 AND $3`,
          [companyId, from, to]
        );
        rows.push(...moneyRes.rows);

        // Account transfers (cash)
        const transferRes = await client.query(
          `SELECT created_at AS date, 'TRANSFER' AS source, id::text AS ref,
            from_type || ' → ' || to_type AS description,
            CASE WHEN from_type = 'CASH' THEN amount ELSE 0 END AS debit,
            CASE WHEN to_type = 'CASH' THEN amount ELSE 0 END AS credit
           FROM account_transfers WHERE company_id = $1 AND created_at BETWEEN $2 AND $3
            AND (from_type = 'CASH' OR to_type = 'CASH')`,
          [companyId, from, to]
        );
        rows.push(...transferRes.rows);

        // Sort + running balance
        rows.sort((a, b) => new Date(a.date).getTime() - new Date(b.date).getTime());
        let balance = 0;
        const ledger = rows.map(r => {
          balance += Number(r.credit || 0) - Number(r.debit || 0);
          return { ...r, debit: Number(r.debit || 0), credit: Number(r.credit || 0), runningBalance: balance };
        });

        return { openingBalance, from, to, ledger, closingBalance: balance };
      } finally {
        client.release();
      }
    },
  },

  {
    name: 'get_primary_bank_ledger',
    description: `Get the primary bank account ledger with running balance. Shows sales (UPI/Card), expenses (bank), distributor payments (bank), money transactions (bank, no accountId), and transfers affecting primary bank. Returns opening balance, ledger rows, and closing balance.`,
    inputSchema: {
      type: 'object' as const,
      properties: {
        fromDate: { type: 'string', description: 'Start date (ISO string). Defaults to all time' },
        toDate: { type: 'string', description: 'End date (ISO string). Defaults to now' },
        companyId: { type: 'string' },
      },
    },
    handler: async (args: { fromDate?: string; toDate?: string; companyId?: string }) => {
      const companyId = cid(args);
      const from = args.fromDate ? new Date(args.fromDate) : new Date(0);
      const to = args.toDate ? new Date(args.toDate) : new Date();
      const cleanup = false;

      const client = await pool.connect();
      try {
        const rows: any[] = [];

        // Primary bank info
        const bankRes = await client.query(
          `SELECT bank, opening_bank_date, bank_name, acc_holder_name, account_no, ifsc, upi_id
           FROM companies WHERE id = $1`, [companyId]
        );
        const c = bankRes.rows[0];
        let baseOpening = 0;
        if (c?.bank && c?.opening_bank_date && new Date(c.opening_bank_date) <= from) {
          baseOpening = Number(c.bank);
        }

        // Opening sums
        const salesBeforeRes = await client.query(
          `WITH split AS (
            SELECT (elem->>'amount')::numeric AS amount
            FROM bills b JOIN LATERAL jsonb_array_elements(
              CASE WHEN jsonb_typeof(b.split_payments::jsonb) = 'array' THEN b.split_payments::jsonb ELSE '[]'::jsonb END
            ) elem ON true
            WHERE b.company_id = $1 AND b.payment_method = 'Split' AND (elem->>'method') IN ('UPI','Card')
              AND b.deleted = false AND b.payment_status IN ('PAID') AND b.is_markit = false
              AND b.created_at < $2 AND ($3 = true OR b.precedence IS NOT TRUE)
          )
          SELECT COALESCE(SUM(CASE WHEN payment_method IN ('UPI','Card') THEN grand_total ELSE 0 END), 0)
            + COALESCE((SELECT SUM(amount) FROM split), 0) AS total
          FROM bills WHERE company_id = $1 AND deleted = false AND payment_status IN ('PAID')
            AND is_markit = false AND created_at < $2 AND ($3 = true OR precedence IS NOT TRUE)`,
          [companyId, from, cleanup]
        );
        const salesBefore = Number(salesBeforeRes.rows[0].total || 0);

        const expBeforeRes = await client.query(
          `SELECT COALESCE(SUM(total_amount), 0) AS total FROM expenses
           WHERE company_id = $1 AND payment_mode = 'BANK' AND UPPER(status) = 'PAID' AND expense_date < $2`,
          [companyId, from]
        );
        const expensesBefore = Number(expBeforeRes.rows[0].total || 0);

        const moneyBeforeRes = await client.query(
          `SELECT COALESCE(SUM(CASE WHEN direction = 'RECEIVED' THEN amount ELSE -amount END), 0) AS net
           FROM money_transactions WHERE company_id = $1 AND payment_mode = 'BANK' AND status = 'PAID'
            AND account_id IS NULL AND created_at < $2`,
          [companyId, from]
        );
        const moneyNetBefore = Number(moneyBeforeRes.rows[0].net || 0);

        const transferBeforeRes = await client.query(
          `SELECT COALESCE(SUM(CASE WHEN to_type = 'BANK' AND to_account_id IS NULL THEN amount ELSE 0 END), 0)
            - COALESCE(SUM(CASE WHEN from_type = 'BANK' AND from_account_id IS NULL THEN amount ELSE 0 END), 0) AS net
           FROM account_transfers WHERE company_id = $1 AND created_at < $2`,
          [companyId, from]
        );
        const transferNetBefore = Number(transferBeforeRes.rows[0].net || 0);

        const distBeforeRes = await client.query(
          `SELECT COALESCE(SUM(amount), 0) AS total FROM distributor_payments
           WHERE company_id = $1 AND payment_type = 'BANK' AND created_at < $2`,
          [companyId, from]
        );
        const distributorBefore = Number(distBeforeRes.rows[0].total || 0);

        const openingBalance = baseOpening + salesBefore - expensesBefore - distributorBefore + moneyNetBefore + transferNetBefore;

        rows.push({ date: from, source: 'OPENING', ref: '-', description: 'Opening Balance', debit: 0, credit: openingBalance });

        // Sales (UPI/Card)
        const salesRes = await client.query(
          `WITH split AS (
            SELECT b.created_at, b.invoice_number, (elem->>'method') AS method, (elem->>'amount')::numeric AS amount
            FROM bills b JOIN LATERAL jsonb_array_elements(
              CASE WHEN jsonb_typeof(b.split_payments::jsonb) = 'array' THEN b.split_payments::jsonb ELSE '[]'::jsonb END
            ) elem ON true
            WHERE b.company_id = $1 AND b.payment_method = 'Split' AND b.deleted = false
              AND b.payment_status IN ('PAID') AND b.is_markit = false
              AND b.created_at BETWEEN $2 AND $3 AND ($4 = true OR b.precedence IS NOT TRUE)
          )
          SELECT b.created_at AS date, 'SALE' AS source, b.invoice_number::text AS ref,
            'Sale via ' || b.payment_method AS description, 0 AS debit, b.grand_total AS credit
          FROM bills b WHERE b.company_id = $1 AND b.payment_method IN ('UPI','Card') AND b.deleted = false
            AND b.payment_status IN ('PAID') AND b.is_markit = false
            AND b.created_at BETWEEN $2 AND $3 AND ($4 = true OR b.precedence IS NOT TRUE)
          UNION ALL
          SELECT s.created_at, 'SALE', s.invoice_number::text, 'Sale via ' || s.method || ' (Split)', 0, s.amount
          FROM split s WHERE s.method IN ('UPI','Card')`,
          [companyId, from, to, cleanup]
        );
        rows.push(...salesRes.rows);

        // Expenses (bank)
        const expRes = await client.query(
          `SELECT e.expense_date AS date, 'EXPENSE' AS source, e.id::text AS ref,
            ec.name || ' (' || COALESCE(e.note,'none') || ')' AS description,
            e.total_amount AS debit, 0 AS credit
           FROM expenses e JOIN expense_categories ec ON ec.id = e.expense_category_id
           WHERE e.company_id = $1 AND e.payment_mode = 'BANK' AND UPPER(e.status) = 'PAID'
            AND e.expense_date BETWEEN $2 AND $3`,
          [companyId, from, to]
        );
        rows.push(...expRes.rows);

        // Distributor payments (bank)
        const distRes = await client.query(
          `SELECT dp.created_at AS date, 'PURCHASE' AS source, dp.id::text AS ref,
            'Paid to ' || d.name || ' (' || COALESCE(po.purchase_order_no::text,'-') || ')' AS description,
            dp.amount AS debit, 0 AS credit
           FROM distributor_payments dp
           JOIN distributor_companies dc ON dc.distributor_id = dp.distributor_id AND dc.company_id = dp.company_id
           JOIN distributors d ON d.id = dc.distributor_id
           LEFT JOIN purchase_orders po ON po.id = dp.purchase_order_id
           WHERE dp.company_id = $1 AND dp.payment_type = 'BANK' AND dp.created_at BETWEEN $2 AND $3`,
          [companyId, from, to]
        );
        rows.push(...distRes.rows);

        // Money transactions (bank, primary only)
        const moneyRes = await client.query(
          `SELECT mt.created_at AS date, 'TRANSACTION' AS source, mt.id::text AS ref,
            mt.direction || ' via bank (' || COALESCE(mt.note,'none') || ')' AS description,
            CASE WHEN mt.direction = 'GIVEN' THEN mt.amount ELSE 0 END AS debit,
            CASE WHEN mt.direction = 'RECEIVED' THEN mt.amount ELSE 0 END AS credit
           FROM money_transactions mt WHERE mt.company_id = $1 AND mt.payment_mode = 'BANK'
            AND mt.status = 'PAID' AND mt.account_id IS NULL AND mt.created_at BETWEEN $2 AND $3`,
          [companyId, from, to]
        );
        rows.push(...moneyRes.rows);

        // Account transfers (primary bank only)
        const transferRes = await client.query(
          `SELECT at.created_at AS date, 'TRANSFER' AS source, at.id::text AS ref,
            at.from_type || ' → ' || at.to_type || ' (' || COALESCE(at.note,'none') || ')' AS description,
            CASE WHEN at.from_type = 'BANK' AND at.from_account_id IS NULL THEN at.amount ELSE 0 END AS debit,
            CASE WHEN at.to_type = 'BANK' AND at.to_account_id IS NULL THEN at.amount ELSE 0 END AS credit
           FROM account_transfers at WHERE at.company_id = $1 AND at.created_at BETWEEN $2 AND $3
            AND ((at.from_type = 'BANK' AND at.from_account_id IS NULL) OR (at.to_type = 'BANK' AND at.to_account_id IS NULL))`,
          [companyId, from, to]
        );
        rows.push(...transferRes.rows);

        // Sort + running balance
        rows.sort((a, b) => new Date(a.date).getTime() - new Date(b.date).getTime());
        let balance = 0;
        const ledger = rows.map(r => {
          balance += Number(r.credit || 0) - Number(r.debit || 0);
          return { ...r, debit: Number(r.debit || 0), credit: Number(r.credit || 0), runningBalance: balance };
        });

        return {
          bank: { type: 'PRIMARY', bankName: c?.bank_name || 'Primary Bank', openingBalance },
          from, to, ledger, closingBalance: balance,
        };
      } finally {
        client.release();
      }
    },
  },

  {
    name: 'get_secondary_bank_ledger',
    description: `Get a secondary bank account ledger by bankId. Shows money transactions and account transfers affecting that specific bank. Returns opening balance, ledger rows, and closing balance.`,
    inputSchema: {
      type: 'object' as const,
      properties: {
        bankId: { type: 'string', description: 'Secondary bank account UUID (REQUIRED)' },
        fromDate: { type: 'string', description: 'Start date (ISO string). Defaults to all time' },
        toDate: { type: 'string', description: 'End date (ISO string). Defaults to now' },
        companyId: { type: 'string' },
      },
      required: ['bankId'],
    },
    handler: async (args: { bankId: string; fromDate?: string; toDate?: string; companyId?: string }) => {
      const companyId = cid(args);
      const from = args.fromDate ? new Date(args.fromDate) : new Date('1970-01-01');
      const to = args.toDate ? new Date(args.toDate) : new Date();

      const client = await pool.connect();
      try {
        const rows: any[] = [];

        // Bank info
        const bankRes = await client.query(
          `SELECT id, bank_name, acc_holder_name, account_no, ifsc, upi_id, opening_balance
           FROM bank_accounts WHERE id = $1 AND company_id = $2`,
          [args.bankId, companyId]
        );
        const b = bankRes.rows[0];
        if (!b) return { error: `Bank account "${args.bankId}" not found` };

        const baseOpening = Number(b.opening_balance || 0);

        // Opening sums
        const moneyBeforeRes = await client.query(
          `SELECT COALESCE(SUM(CASE WHEN direction = 'RECEIVED' THEN amount ELSE -amount END), 0) AS net
           FROM money_transactions WHERE company_id = $1 AND payment_mode = 'BANK' AND status = 'PAID'
            AND account_id = $2 AND created_at < $3`,
          [companyId, args.bankId, from]
        );
        const moneyNetBefore = Number(moneyBeforeRes.rows[0].net || 0);

        const transferBeforeRes = await client.query(
          `SELECT COALESCE(SUM(CASE WHEN to_type = 'BANK' AND to_account_id = $2 THEN amount ELSE 0 END), 0)
            - COALESCE(SUM(CASE WHEN from_type = 'BANK' AND from_account_id = $2 THEN amount ELSE 0 END), 0) AS net
           FROM account_transfers WHERE company_id = $1 AND created_at < $3
            AND (from_account_id = $2 OR to_account_id = $2)`,
          [companyId, args.bankId, from]
        );
        const transferNetBefore = Number(transferBeforeRes.rows[0].net || 0);

        const openingBalance = baseOpening + moneyNetBefore + transferNetBefore;

        rows.push({ date: from, source: 'OPENING', ref: '-', description: 'Opening Balance', debit: 0, credit: openingBalance });

        // Money transactions
        const moneyRes = await client.query(
          `SELECT created_at AS date, 'TRANSACTION' AS source, id::text AS ref,
            direction || ' via bank' AS description,
            CASE WHEN direction = 'GIVEN' THEN amount ELSE 0 END AS debit,
            CASE WHEN direction = 'RECEIVED' THEN amount ELSE 0 END AS credit
           FROM money_transactions WHERE company_id = $1 AND payment_mode = 'BANK' AND status = 'PAID'
            AND account_id = $2 AND created_at BETWEEN $3 AND $4`,
          [companyId, args.bankId, from, to]
        );
        rows.push(...moneyRes.rows);

        // Account transfers
        const transferRes = await client.query(
          `SELECT created_at AS date, 'TRANSFER' AS source, id::text AS ref,
            from_type || ' → ' || to_type AS description,
            CASE WHEN from_type = 'BANK' AND from_account_id = $2 THEN amount ELSE 0 END AS debit,
            CASE WHEN to_type = 'BANK' AND to_account_id = $2 THEN amount ELSE 0 END AS credit
           FROM account_transfers WHERE company_id = $1 AND created_at BETWEEN $3 AND $4
            AND (from_account_id = $2 OR to_account_id = $2)`,
          [companyId, args.bankId, from, to]
        );
        rows.push(...transferRes.rows);

        // Sort + running balance
        rows.sort((a, b) => new Date(a.date).getTime() - new Date(b.date).getTime());
        let balance = 0;
        const ledger = rows.map(r => {
          balance += Number(r.credit || 0) - Number(r.debit || 0);
          return { ...r, debit: Number(r.debit || 0), credit: Number(r.credit || 0), runningBalance: balance };
        });

        return {
          bank: { id: b.id, type: 'SECONDARY', bankName: b.bank_name, openingBalance },
          from, to, ledger, closingBalance: balance,
        };
      } finally {
        client.release();
      }
    },
  },
];
