import crypto from 'crypto';
import { pool, COMPANY_ID } from '../db';

const cid = (args: { companyId?: string }) => args.companyId ?? COMPANY_ID;

export const expenseTools = [
  // ─── List Expenses ────────────────────────────────────────────────────────────

  {
    name: 'list_expenses',
    description: 'List expenses with optional filters. Returns paginated list with category and user info.',
    inputSchema: {
      type: 'object' as const,
      properties: {
        search: { type: 'string', description: 'Search by note content' },
        categoryId: { type: 'string', description: 'Filter by expense category UUID' },
        userId: { type: 'string', description: 'Filter by user (from_id) UUID' },
        status: { type: 'string', description: 'Filter by status (e.g. Paid, Pending, Approved, Rejected)' },
        paymentMode: { type: 'string', description: 'Filter by payment mode (CASH, BANK, UPI, CARD, CHEQUE)' },
        fromDate: { type: 'string', description: 'Filter expenses from this date (ISO string)' },
        toDate: { type: 'string', description: 'Filter expenses up to this date (ISO string)' },
        limit: { type: 'number' },
        offset: { type: 'number' },
        companyId: { type: 'string' },
      },
    },
    handler: async (args: {
      search?: string; categoryId?: string; userId?: string; status?: string;
      paymentMode?: string; fromDate?: string; toDate?: string;
      limit?: number; offset?: number; companyId?: string;
    }) => {
      const companyId = cid(args);
      const limit = args.limit ?? 50;
      const offset = args.offset ?? 0;
      const params: unknown[] = [companyId, limit, offset];
      const wheres: string[] = ['e.company_id = $1'];

      if (args.search) { params.push(`%${args.search}%`); wheres.push(`e.note ILIKE $${params.length}`); }
      if (args.categoryId) { params.push(args.categoryId); wheres.push(`e.expense_category_id = $${params.length}`); }
      if (args.userId) { params.push(args.userId); wheres.push(`e.from_id = $${params.length}`); }
      if (args.status) { params.push(args.status); wheres.push(`e.status = $${params.length}`); }
      if (args.paymentMode) { params.push(args.paymentMode); wheres.push(`e.payment_mode = $${params.length}`); }
      if (args.fromDate) { params.push(args.fromDate); wheres.push(`e.expense_date >= $${params.length}::timestamptz`); }
      if (args.toDate) { params.push(args.toDate); wheres.push(`e.expense_date <= $${params.length}::timestamptz`); }

      const { rows } = await pool.query(
        `SELECT e.id, e.expense_date, e.note, e.payment_mode, e.status,
                e.total_amount, e.tax_amount, e.currency, e.created_at,
                ec.id AS category_id, ec.name AS category_name,
                cu.name AS user_name, cu.phone AS user_phone, e.from_id AS user_id
         FROM expenses e
         LEFT JOIN expense_categories ec ON ec.id = e.expense_category_id
         LEFT JOIN company_users cu ON cu.company_id = e.company_id AND cu.user_id = e.from_id
         WHERE ${wheres.join(' AND ')}
         ORDER BY e.expense_date DESC, e.created_at DESC
         LIMIT $2 OFFSET $3`,
        params
      );

      const countParams: unknown[] = [companyId];
      const countWheres: string[] = ['e.company_id = $1'];
      if (args.categoryId) { countParams.push(args.categoryId); countWheres.push(`e.expense_category_id = $${countParams.length}`); }
      if (args.status) { countParams.push(args.status); countWheres.push(`e.status = $${countParams.length}`); }
      if (args.fromDate) { countParams.push(args.fromDate); countWheres.push(`e.expense_date >= $${countParams.length}::timestamptz`); }
      if (args.toDate) { countParams.push(args.toDate); countWheres.push(`e.expense_date <= $${countParams.length}::timestamptz`); }

      const { rows: countRows } = await pool.query(
        `SELECT COUNT(*)::int AS total FROM expenses e WHERE ${countWheres.join(' AND ')}`,
        countParams
      );

      return { expenses: rows, total: countRows[0]?.total ?? rows.length };
    },
  },

  // ─── Create Expense ───────────────────────────────────────────────────────────

  {
    name: 'create_expense',
    description: `Create a new expense record.

REQUIRED: category (name or UUID), amount
OPTIONAL: date, userId (employee who incurred the expense), paymentMode, status, note

FLOW:
1. If user provides a category name, look it up. If not found, ask: "Expense category '<name>' doesn't exist. Should I create it?"
2. If userId is provided, it links the expense to a company user (employee)`,
    inputSchema: {
      type: 'object' as const,
      properties: {
        category: { type: 'string', description: 'Expense category name or UUID (REQUIRED)' },
        amount: { type: 'number', description: 'Total expense amount (REQUIRED)' },
        date: { type: 'string', description: 'Expense date (ISO string). Defaults to now' },
        userId: { type: 'string', description: 'User UUID who incurred the expense (from_id)' },
        paymentMode: { type: 'string', description: 'Payment mode: CASH, BANK, UPI, CARD, CHEQUE (default: CASH)' },
        status: { type: 'string', description: 'Status: Paid, Pending, Approved, Rejected (default: Paid)' },
        note: { type: 'string', description: 'Optional note/description' },
        companyId: { type: 'string' },
      },
      required: ['category', 'amount'],
    },
    handler: async (args: {
      category: string; amount: number; date?: string; userId?: string;
      paymentMode?: string; status?: string; note?: string; companyId?: string;
    }) => {
      const companyId = cid(args);

      // Resolve category — try UUID first, then name
      let categoryId: string;
      const isUUID = /^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$/i.test(args.category);
      if (isUUID) {
        const { rows } = await pool.query(
          `SELECT id FROM expense_categories WHERE id = $1 AND company_id = $2`,
          [args.category, companyId]
        );
        if (!rows.length) return { error: `Expense category UUID "${args.category}" not found` };
        categoryId = rows[0].id;
      } else {
        const { rows } = await pool.query(
          `SELECT id FROM expense_categories WHERE LOWER(name) = LOWER($1) AND company_id = $2 LIMIT 1`,
          [args.category, companyId]
        );
        if (!rows.length) {
          return { error: `Expense category "${args.category}" not found. Create it first using create_expense_category.` };
        }
        categoryId = rows[0].id;
      }

      // Validate userId if provided
      if (args.userId) {
        const { rows } = await pool.query(
          `SELECT user_id FROM company_users WHERE company_id = $1 AND user_id = $2 AND deleted = false`,
          [companyId, args.userId]
        );
        if (!rows.length) return { error: `User "${args.userId}" not found in this company` };
      }

      const id = crypto.randomUUID();
      const expenseDate = args.date ? new Date(args.date).toISOString() : new Date().toISOString();
      const paymentMode = args.paymentMode ?? 'CASH';
      const status = args.status ?? 'Paid';

      await pool.query(
        `INSERT INTO expenses (id, expense_date, note, payment_mode, status, total_amount,
          expense_category_id, company_id, from_id, created_at, updated_at)
         VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, now(), now())`,
        [id, expenseDate, args.note ?? null, paymentMode, status, args.amount,
         categoryId, companyId, args.userId ?? null]
      );

      // Return created expense with category name
      const { rows: created } = await pool.query(
        `SELECT e.id, e.expense_date, e.total_amount, e.payment_mode, e.status, e.note,
                ec.name AS category_name
         FROM expenses e
         LEFT JOIN expense_categories ec ON ec.id = e.expense_category_id
         WHERE e.id = $1`,
        [id]
      );

      return {
        success: true,
        expenseId: id,
        ...created[0],
      };
    },
  },

  // ─── Update Expense ───────────────────────────────────────────────────────────

  {
    name: 'update_expense',
    description: 'Update an expense\'s category, amount, date, payment mode, status, note, or assigned user.',
    inputSchema: {
      type: 'object' as const,
      properties: {
        expenseId: { type: 'string', description: 'Expense UUID (REQUIRED)' },
        category: { type: 'string', description: 'Expense category name or UUID' },
        amount: { type: 'number', description: 'Total expense amount' },
        date: { type: 'string', description: 'Expense date (ISO string)' },
        userId: { type: 'string', description: 'User UUID who incurred the expense' },
        paymentMode: { type: 'string', description: 'Payment mode: CASH, BANK, UPI, CARD, CHEQUE' },
        status: { type: 'string', description: 'Status: Paid, Pending, Approved, Rejected' },
        note: { type: 'string', description: 'Note/description' },
        companyId: { type: 'string' },
      },
      required: ['expenseId'],
    },
    handler: async (args: {
      expenseId: string; category?: string; amount?: number; date?: string;
      userId?: string; paymentMode?: string; status?: string; note?: string;
      companyId?: string;
    }) => {
      const companyId = cid(args);
      const sets: string[] = ['updated_at = now()'];
      const params: unknown[] = [args.expenseId, companyId];

      // Resolve category if provided
      if (args.category) {
        const isUUID = /^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$/i.test(args.category);
        let categoryId: string;
        if (isUUID) {
          const { rows } = await pool.query(
            `SELECT id FROM expense_categories WHERE id = $1 AND company_id = $2`,
            [args.category, companyId]
          );
          if (!rows.length) return { error: `Expense category UUID "${args.category}" not found` };
          categoryId = rows[0].id;
        } else {
          const { rows } = await pool.query(
            `SELECT id FROM expense_categories WHERE LOWER(name) = LOWER($1) AND company_id = $2 LIMIT 1`,
            [args.category, companyId]
          );
          if (!rows.length) return { error: `Expense category "${args.category}" not found. Create it first.` };
          categoryId = rows[0].id;
        }
        params.push(categoryId); sets.push(`expense_category_id = $${params.length}`);
      }

      if (args.amount !== undefined) { params.push(args.amount); sets.push(`total_amount = $${params.length}`); }
      if (args.date) { params.push(new Date(args.date).toISOString()); sets.push(`expense_date = $${params.length}`); }
      if (args.userId !== undefined) { params.push(args.userId); sets.push(`from_id = $${params.length}`); }
      if (args.paymentMode) { params.push(args.paymentMode); sets.push(`payment_mode = $${params.length}`); }
      if (args.status) { params.push(args.status); sets.push(`status = $${params.length}`); }
      if (args.note !== undefined) { params.push(args.note); sets.push(`note = $${params.length}`); }

      if (sets.length === 1) return { error: 'No fields to update' };

      await pool.query(
        `UPDATE expenses SET ${sets.join(', ')} WHERE id = $1 AND company_id = $2`,
        params
      );

      const { rows } = await pool.query(
        `SELECT e.id, e.expense_date, e.total_amount, e.payment_mode, e.status, e.note,
                ec.name AS category_name, e.from_id AS user_id
         FROM expenses e
         LEFT JOIN expense_categories ec ON ec.id = e.expense_category_id
         WHERE e.id = $1 AND e.company_id = $2`,
        [args.expenseId, companyId]
      );

      return { success: true, expenseId: args.expenseId, ...rows[0] };
    },
  },

  // ─── Delete Expense ───────────────────────────────────────────────────────────

  {
    name: 'delete_expense',
    description: 'Delete an expense by ID.',
    inputSchema: {
      type: 'object' as const,
      properties: {
        expenseId: { type: 'string', description: 'Expense UUID' },
        companyId: { type: 'string' },
      },
      required: ['expenseId'],
    },
    handler: async (args: { expenseId: string; companyId?: string }) => {
      const companyId = cid(args);

      const { rows } = await pool.query(
        `SELECT e.total_amount, e.note, ec.name AS category_name
         FROM expenses e
         LEFT JOIN expense_categories ec ON ec.id = e.expense_category_id
         WHERE e.id = $1 AND e.company_id = $2`,
        [args.expenseId, companyId]
      );
      if (!rows.length) return { error: `Expense "${args.expenseId}" not found` };

      await pool.query(`DELETE FROM expenses WHERE id = $1 AND company_id = $2`, [args.expenseId, companyId]);
      return {
        success: true,
        deletedExpenseId: args.expenseId,
        amount: rows[0].total_amount,
        category: rows[0].category_name,
        note: rows[0].note,
      };
    },
  },

  // ─── List Expense Categories ──────────────────────────────────────────────────

  {
    name: 'list_expense_categories',
    description: 'List all expense categories for this company with expense count and total amount.',
    inputSchema: {
      type: 'object' as const,
      properties: {
        search: { type: 'string', description: 'Search by category name' },
        companyId: { type: 'string' },
      },
    },
    handler: async (args: { search?: string; companyId?: string }) => {
      const companyId = cid(args);
      const params: unknown[] = [companyId];
      const wheres: string[] = ['ec.company_id = $1'];

      if (args.search) { params.push(`%${args.search}%`); wheres.push(`ec.name ILIKE $${params.length}`); }

      const { rows } = await pool.query(
        `SELECT ec.id, ec.name, ec.status, ec.created_at,
                COUNT(e.id)::int AS expense_count,
                COALESCE(SUM(e.total_amount), 0)::float AS total_amount
         FROM expense_categories ec
         LEFT JOIN expenses e ON e.expense_category_id = ec.id AND e.company_id = ec.company_id
         WHERE ${wheres.join(' AND ')}
         GROUP BY ec.id ORDER BY ec.name`,
        params
      );

      return { categories: rows };
    },
  },

  // ─── Create Expense Category ──────────────────────────────────────────────────

  {
    name: 'create_expense_category',
    description: 'Create a new expense category. Returns error if it already exists (with its ID).',
    inputSchema: {
      type: 'object' as const,
      properties: {
        name: { type: 'string', description: 'Category name (REQUIRED)' },
        companyId: { type: 'string' },
      },
      required: ['name'],
    },
    handler: async (args: { name: string; companyId?: string }) => {
      const companyId = cid(args);

      const { rows: existing } = await pool.query(
        `SELECT id, name FROM expense_categories WHERE LOWER(name) = LOWER($1) AND company_id = $2 LIMIT 1`,
        [args.name, companyId]
      );
      if (existing.length) {
        return { error: `Expense category "${existing[0].name}" already exists`, existingId: existing[0].id };
      }

      const id = crypto.randomUUID();
      await pool.query(
        `INSERT INTO expense_categories (id, name, status, company_id, created_at, updated_at)
         VALUES ($1, $2, true, $3, now(), now())`,
        [id, args.name, companyId]
      );

      return { success: true, categoryId: id, name: args.name };
    },
  },
];
