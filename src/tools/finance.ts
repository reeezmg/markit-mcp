import crypto from 'crypto';
import { pool, COMPANY_ID } from '../db';

const cid = (args: { companyId?: string }) => args.companyId ?? COMPANY_ID;

export const financeTools = [
  // ─── Investments ────────────────────────────────────────────────────────────

  {
    name: 'list_investments',
    description: 'List investments with optional filters. Returns paginated list with user info.',
    inputSchema: {
      type: 'object' as const,
      properties: {
        direction: { type: 'string', enum: ['IN', 'OUT'], description: 'Filter by direction (IN = invested, OUT = withdrawn)' },
        status: { type: 'string', enum: ['COMPLETED', 'PENDING'], description: 'Filter by status' },
        userId: { type: 'string', description: 'Filter by user UUID' },
        paymentMode: { type: 'string', enum: ['CASH', 'BANK', 'UPI', 'CARD', 'CHEQUE'], description: 'Filter by payment mode' },
        fromDate: { type: 'string', description: 'Filter from this date (ISO string)' },
        toDate: { type: 'string', description: 'Filter up to this date (ISO string)' },
        limit: { type: 'number' },
        offset: { type: 'number' },
        companyId: { type: 'string' },
      },
    },
    handler: async (args: {
      direction?: string; status?: string; userId?: string; paymentMode?: string;
      fromDate?: string; toDate?: string; limit?: number; offset?: number; companyId?: string;
    }) => {
      const companyId = cid(args);
      const limit = args.limit ?? 50;
      const offset = args.offset ?? 0;
      const params: unknown[] = [companyId, limit, offset];
      const wheres: string[] = ['i.company_id = $1'];

      if (args.direction) { params.push(args.direction); wheres.push(`i.direction = $${params.length}`); }
      if (args.status) { params.push(args.status); wheres.push(`i.status = $${params.length}`); }
      if (args.userId) { params.push(args.userId); wheres.push(`i."userId" = $${params.length}`); }
      if (args.paymentMode) { params.push(args.paymentMode); wheres.push(`i.payment_mode = $${params.length}`); }
      if (args.fromDate) { params.push(args.fromDate); wheres.push(`i.created_at >= $${params.length}::timestamptz`); }
      if (args.toDate) { params.push(args.toDate); wheres.push(`i.created_at <= $${params.length}::timestamptz`); }

      const { rows } = await pool.query(
        `SELECT i.id, i.created_at, i.direction, i.amount::float, i.payment_mode, i.status, i.note,
                i."userId" AS user_id, cu.name AS user_name, cu.phone AS user_phone
         FROM investments i
         LEFT JOIN company_users cu ON cu.company_id = i.company_id AND cu.user_id = i."userId"
         WHERE ${wheres.join(' AND ')}
         ORDER BY i.created_at DESC
         LIMIT $2 OFFSET $3`,
        params
      );

      const countParams: unknown[] = [companyId];
      const countWheres: string[] = ['company_id = $1'];
      if (args.direction) { countParams.push(args.direction); countWheres.push(`direction = $${countParams.length}`); }
      if (args.status) { countParams.push(args.status); countWheres.push(`status = $${countParams.length}`); }
      const { rows: countRows } = await pool.query(
        `SELECT COUNT(*)::int AS total FROM investments WHERE ${countWheres.join(' AND ')}`,
        countParams
      );

      return { investments: rows, total: countRows[0]?.total ?? rows.length };
    },
  },

  {
    name: 'create_investment',
    description: `Create an investment record (capital IN or drawings OUT).

REQUIRED: userId (employee/owner UUID), direction (IN or OUT), amount
OPTIONAL: paymentMode (CASH/BANK/UPI/CARD/CHEQUE, default CASH), status (COMPLETED/PENDING, default COMPLETED), note, date (ISO string, default now)

The userId must be a valid company user.`,
    inputSchema: {
      type: 'object' as const,
      properties: {
        userId: { type: 'string', description: 'User UUID who invested/withdrew (REQUIRED)' },
        direction: { type: 'string', enum: ['IN', 'OUT'], description: 'IN = capital invested, OUT = capital withdrawn/drawings (REQUIRED)' },
        amount: { type: 'number', description: 'Investment amount (REQUIRED)' },
        paymentMode: { type: 'string', enum: ['CASH', 'BANK', 'UPI', 'CARD', 'CHEQUE'], description: 'Default: CASH' },
        status: { type: 'string', enum: ['COMPLETED', 'PENDING'], description: 'Default: COMPLETED' },
        note: { type: 'string', description: 'Optional note' },
        date: { type: 'string', description: 'Investment date (ISO string). Defaults to now' },
        companyId: { type: 'string' },
      },
      required: ['userId', 'direction', 'amount'],
    },
    handler: async (args: {
      userId: string; direction: string; amount: number; paymentMode?: string;
      status?: string; note?: string; date?: string; companyId?: string;
    }) => {
      const companyId = cid(args);

      // Validate userId
      const { rows: userRows } = await pool.query(
        `SELECT user_id, name FROM company_users WHERE company_id = $1 AND user_id = $2 AND deleted = false`,
        [companyId, args.userId]
      );
      if (!userRows.length) return { error: `User "${args.userId}" not found in this company` };

      const id = crypto.randomUUID();
      const createdAt = args.date ? new Date(args.date).toISOString() : new Date().toISOString();

      await pool.query(
        `INSERT INTO investments (id, company_id, "userId", direction, amount, payment_mode, status, note, created_at, updated_at)
         VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, now())`,
        [id, companyId, args.userId, args.direction, args.amount,
         args.paymentMode ?? 'CASH', args.status ?? 'COMPLETED', args.note ?? null, createdAt]
      );

      return {
        success: true,
        investmentId: id,
        direction: args.direction,
        amount: args.amount,
        userName: userRows[0].name,
        status: args.status ?? 'COMPLETED',
      };
    },
  },

  {
    name: 'update_investment',
    description: "Update an investment's direction, amount, paymentMode, status, note, date, or assigned user.",
    inputSchema: {
      type: 'object' as const,
      properties: {
        investmentId: { type: 'string', description: 'Investment UUID (REQUIRED)' },
        userId: { type: 'string', description: 'User UUID' },
        direction: { type: 'string', enum: ['IN', 'OUT'] },
        amount: { type: 'number' },
        paymentMode: { type: 'string', enum: ['CASH', 'BANK', 'UPI', 'CARD', 'CHEQUE'] },
        status: { type: 'string', enum: ['COMPLETED', 'PENDING'] },
        note: { type: 'string' },
        date: { type: 'string', description: 'Investment date (ISO string)' },
        companyId: { type: 'string' },
      },
      required: ['investmentId'],
    },
    handler: async (args: {
      investmentId: string; userId?: string; direction?: string; amount?: number;
      paymentMode?: string; status?: string; note?: string; date?: string; companyId?: string;
    }) => {
      const companyId = cid(args);
      const sets: string[] = ['updated_at = now()'];
      const params: unknown[] = [args.investmentId, companyId];

      if (args.userId !== undefined) { params.push(args.userId); sets.push(`"userId" = $${params.length}`); }
      if (args.direction !== undefined) { params.push(args.direction); sets.push(`direction = $${params.length}`); }
      if (args.amount !== undefined) { params.push(args.amount); sets.push(`amount = $${params.length}`); }
      if (args.paymentMode !== undefined) { params.push(args.paymentMode); sets.push(`payment_mode = $${params.length}`); }
      if (args.status !== undefined) { params.push(args.status); sets.push(`status = $${params.length}`); }
      if (args.note !== undefined) { params.push(args.note); sets.push(`note = $${params.length}`); }
      if (args.date !== undefined) { params.push(new Date(args.date).toISOString()); sets.push(`created_at = $${params.length}`); }

      if (sets.length === 1) return { error: 'No fields to update' };

      await pool.query(
        `UPDATE investments SET ${sets.join(', ')} WHERE id = $1 AND company_id = $2`,
        params
      );

      const { rows } = await pool.query(
        `SELECT i.id, i.direction, i.amount::float, i.payment_mode, i.status, i.note,
                cu.name AS user_name
         FROM investments i
         LEFT JOIN company_users cu ON cu.company_id = i.company_id AND cu.user_id = i."userId"
         WHERE i.id = $1 AND i.company_id = $2`,
        [args.investmentId, companyId]
      );

      return { success: true, investmentId: args.investmentId, ...rows[0] };
    },
  },

  {
    name: 'delete_investment',
    description: 'Delete an investment by ID.',
    inputSchema: {
      type: 'object' as const,
      properties: {
        investmentId: { type: 'string', description: 'Investment UUID' },
        companyId: { type: 'string' },
      },
      required: ['investmentId'],
    },
    handler: async (args: { investmentId: string; companyId?: string }) => {
      const companyId = cid(args);

      const { rows } = await pool.query(
        `SELECT i.amount::float, i.direction, cu.name AS user_name
         FROM investments i
         LEFT JOIN company_users cu ON cu.company_id = i.company_id AND cu.user_id = i."userId"
         WHERE i.id = $1 AND i.company_id = $2`,
        [args.investmentId, companyId]
      );
      if (!rows.length) return { error: `Investment "${args.investmentId}" not found` };

      await pool.query(`DELETE FROM investments WHERE id = $1 AND company_id = $2`, [args.investmentId, companyId]);
      return {
        success: true,
        deletedInvestmentId: args.investmentId,
        amount: rows[0].amount,
        direction: rows[0].direction,
        userName: rows[0].user_name,
      };
    },
  },

  // ─── Money Transactions ─────────────────────────────────────────────────────

  {
    name: 'list_money_transactions',
    description: 'List money transactions — external cash/bank exchanges WITH a party (customer, supplier, employee, owner, etc.). Do NOT use for internal fund movements between own accounts — use account transfers for that.',
    inputSchema: {
      type: 'object' as const,
      properties: {
        partyType: { type: 'string', enum: ['CUSTOMER', 'SUPPLIER', 'EMPLOYEE', 'OWNER', 'OTHER'], description: 'Filter by party type' },
        direction: { type: 'string', enum: ['GIVEN', 'RECEIVED'], description: 'Filter by direction' },
        status: { type: 'string', enum: ['PENDING', 'PAID'], description: 'Filter by status' },
        paymentMode: { type: 'string', enum: ['CASH', 'BANK', 'UPI', 'CARD', 'CHEQUE'], description: 'Filter by payment mode' },
        fromDate: { type: 'string', description: 'Filter from this date (ISO string)' },
        toDate: { type: 'string', description: 'Filter up to this date (ISO string)' },
        limit: { type: 'number' },
        offset: { type: 'number' },
        companyId: { type: 'string' },
      },
    },
    handler: async (args: {
      partyType?: string; direction?: string; status?: string; paymentMode?: string;
      fromDate?: string; toDate?: string; limit?: number; offset?: number; companyId?: string;
    }) => {
      const companyId = cid(args);
      const limit = args.limit ?? 50;
      const offset = args.offset ?? 0;
      const params: unknown[] = [companyId, limit, offset];
      const wheres: string[] = ['mt.company_id = $1'];

      if (args.partyType) { params.push(args.partyType); wheres.push(`mt.party_type = $${params.length}`); }
      if (args.direction) { params.push(args.direction); wheres.push(`mt.direction = $${params.length}`); }
      if (args.status) { params.push(args.status); wheres.push(`mt.status = $${params.length}`); }
      if (args.paymentMode) { params.push(args.paymentMode); wheres.push(`mt.payment_mode = $${params.length}`); }
      if (args.fromDate) { params.push(args.fromDate); wheres.push(`mt.created_at >= $${params.length}::timestamptz`); }
      if (args.toDate) { params.push(args.toDate); wheres.push(`mt.created_at <= $${params.length}::timestamptz`); }

      const { rows } = await pool.query(
        `SELECT mt.id, mt.created_at, mt.party_type, mt.direction, mt.status,
                mt.amount::float, mt.payment_mode, mt.account_id, mt.note,
                ba.bank_name AS bank_name
         FROM money_transactions mt
         LEFT JOIN bank_accounts ba ON ba.id = mt.account_id
         WHERE ${wheres.join(' AND ')}
         ORDER BY mt.created_at DESC
         LIMIT $2 OFFSET $3`,
        params
      );

      const countParams: unknown[] = [companyId];
      const countWheres: string[] = ['company_id = $1'];
      if (args.partyType) { countParams.push(args.partyType); countWheres.push(`party_type = $${countParams.length}`); }
      if (args.direction) { countParams.push(args.direction); countWheres.push(`direction = $${countParams.length}`); }
      if (args.status) { countParams.push(args.status); countWheres.push(`status = $${countParams.length}`); }
      const { rows: countRows } = await pool.query(
        `SELECT COUNT(*)::int AS total FROM money_transactions WHERE ${countWheres.join(' AND ')}`,
        countParams
      );

      return { transactions: rows, total: countRows[0]?.total ?? rows.length };
    },
  },

  {
    name: 'create_money_transaction',
    description: `Create a money transaction — an external cash/bank exchange WITH a party. Use for "gave money to supplier", "received from customer". Do NOT use for internal fund movements — use create_account_transfer for that.

REQUIRED: partyType (CUSTOMER/SUPPLIER/EMPLOYEE/OWNER/OTHER), direction (GIVEN/RECEIVED), amount
OPTIONAL: paymentMode (CASH/BANK/UPI/CARD/CHEQUE, default CASH), status (PENDING/PAID, default PENDING), accountId (bank account UUID — only needed when paymentMode is BANK), note, date (ISO string, default now)`,
    inputSchema: {
      type: 'object' as const,
      properties: {
        partyType: { type: 'string', enum: ['CUSTOMER', 'SUPPLIER', 'EMPLOYEE', 'OWNER', 'OTHER'], description: 'Who the transaction is with (REQUIRED)' },
        direction: { type: 'string', enum: ['GIVEN', 'RECEIVED'], description: 'GIVEN = paid out, RECEIVED = collected (REQUIRED)' },
        amount: { type: 'number', description: 'Transaction amount (REQUIRED)' },
        paymentMode: { type: 'string', enum: ['CASH', 'BANK', 'UPI', 'CARD', 'CHEQUE'], description: 'Default: CASH' },
        status: { type: 'string', enum: ['PENDING', 'PAID'], description: 'Default: PENDING' },
        accountId: { type: 'string', description: 'Bank account UUID (only when paymentMode=BANK)' },
        note: { type: 'string', description: 'Optional note' },
        date: { type: 'string', description: 'Transaction date (ISO string). Defaults to now' },
        companyId: { type: 'string' },
      },
      required: ['partyType', 'direction', 'amount'],
    },
    handler: async (args: {
      partyType: string; direction: string; amount: number; paymentMode?: string;
      status?: string; accountId?: string; note?: string; date?: string; companyId?: string;
    }) => {
      const companyId = cid(args);

      // Validate accountId if provided
      if (args.accountId) {
        const { rows } = await pool.query(
          `SELECT id FROM bank_accounts WHERE id = $1 AND company_id = $2`,
          [args.accountId, companyId]
        );
        if (!rows.length) return { error: `Bank account "${args.accountId}" not found` };
      }

      const id = crypto.randomUUID();
      const createdAt = args.date ? new Date(args.date).toISOString() : new Date().toISOString();

      await pool.query(
        `INSERT INTO money_transactions (id, company_id, party_type, direction, status, amount, payment_mode, account_id, note, created_at, updated_at)
         VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, now())`,
        [id, companyId, args.partyType, args.direction, args.status ?? 'PENDING',
         args.amount, args.paymentMode ?? 'CASH', args.accountId ?? null, args.note ?? null, createdAt]
      );

      return {
        success: true,
        transactionId: id,
        partyType: args.partyType,
        direction: args.direction,
        amount: args.amount,
        status: args.status ?? 'PENDING',
      };
    },
  },

  {
    name: 'update_money_transaction',
    description: "Update a money transaction (external exchange with a party). Can change partyType, direction, status, amount, paymentMode, accountId, or note.",
    inputSchema: {
      type: 'object' as const,
      properties: {
        transactionId: { type: 'string', description: 'Transaction UUID (REQUIRED)' },
        partyType: { type: 'string', enum: ['CUSTOMER', 'SUPPLIER', 'EMPLOYEE', 'OWNER', 'OTHER'] },
        direction: { type: 'string', enum: ['GIVEN', 'RECEIVED'] },
        status: { type: 'string', enum: ['PENDING', 'PAID'] },
        amount: { type: 'number' },
        paymentMode: { type: 'string', enum: ['CASH', 'BANK', 'UPI', 'CARD', 'CHEQUE'] },
        accountId: { type: 'string', description: 'Bank account UUID (null to clear)' },
        note: { type: 'string' },
        date: { type: 'string', description: 'Transaction date (ISO string)' },
        companyId: { type: 'string' },
      },
      required: ['transactionId'],
    },
    handler: async (args: {
      transactionId: string; partyType?: string; direction?: string; status?: string;
      amount?: number; paymentMode?: string; accountId?: string; note?: string;
      date?: string; companyId?: string;
    }) => {
      const companyId = cid(args);
      const sets: string[] = ['updated_at = now()'];
      const params: unknown[] = [args.transactionId, companyId];

      if (args.partyType !== undefined) { params.push(args.partyType); sets.push(`party_type = $${params.length}`); }
      if (args.direction !== undefined) { params.push(args.direction); sets.push(`direction = $${params.length}`); }
      if (args.status !== undefined) { params.push(args.status); sets.push(`status = $${params.length}`); }
      if (args.amount !== undefined) { params.push(args.amount); sets.push(`amount = $${params.length}`); }
      if (args.paymentMode !== undefined) { params.push(args.paymentMode); sets.push(`payment_mode = $${params.length}`); }
      if (args.accountId !== undefined) { params.push(args.accountId || null); sets.push(`account_id = $${params.length}`); }
      if (args.note !== undefined) { params.push(args.note); sets.push(`note = $${params.length}`); }
      if (args.date !== undefined) { params.push(new Date(args.date).toISOString()); sets.push(`created_at = $${params.length}`); }

      if (sets.length === 1) return { error: 'No fields to update' };

      await pool.query(
        `UPDATE money_transactions SET ${sets.join(', ')} WHERE id = $1 AND company_id = $2`,
        params
      );

      const { rows } = await pool.query(
        `SELECT id, party_type, direction, status, amount::float, payment_mode, note
         FROM money_transactions WHERE id = $1 AND company_id = $2`,
        [args.transactionId, companyId]
      );

      return { success: true, transactionId: args.transactionId, ...rows[0] };
    },
  },

  {
    name: 'delete_money_transaction',
    description: 'Delete a money transaction (external exchange with a party) by ID.',
    inputSchema: {
      type: 'object' as const,
      properties: {
        transactionId: { type: 'string', description: 'Transaction UUID' },
        companyId: { type: 'string' },
      },
      required: ['transactionId'],
    },
    handler: async (args: { transactionId: string; companyId?: string }) => {
      const companyId = cid(args);

      const { rows } = await pool.query(
        `SELECT amount::float, direction, party_type, note FROM money_transactions WHERE id = $1 AND company_id = $2`,
        [args.transactionId, companyId]
      );
      if (!rows.length) return { error: `Transaction "${args.transactionId}" not found` };

      await pool.query(`DELETE FROM money_transactions WHERE id = $1 AND company_id = $2`, [args.transactionId, companyId]);
      return {
        success: true,
        deletedTransactionId: args.transactionId,
        amount: rows[0].amount,
        direction: rows[0].direction,
        partyType: rows[0].party_type,
      };
    },
  },
];
