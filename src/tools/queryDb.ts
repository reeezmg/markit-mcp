import { pool, COMPANY_ID } from '../db';

const BLOCKED_PATTERNS = /\b(DROP|TRUNCATE|ALTER|CREATE|GRANT|REVOKE|DELETE\s+FROM|INSERT\s+INTO|UPDATE\s+\w+\s+SET)\b/i;

const QUERY_DB_DESCRIPTION = `Run a read-only SQL query against the database.

Rules:
- SELECT only — no INSERT, UPDATE, DELETE, DROP, ALTER, TRUNCATE
- companyId is auto-injected as $1 — use it in your WHERE clause to scope data
- Max 100 rows returned
- Use this for ad-hoc lookups, debugging, or data exploration

Key tables (all have company_id for tenant scope):
- products (id, name, brand_id, category_id, subcategory_id, purchaseorder_id, status, company_id)
- variants (id, name, code, s_price, p_price, d_price, discount, tax, images, delivery_type, product_id, company_id)
- items (id, variant_id, size, qty, initial_qty, sold_qty, barcode, company_id)
- brands (id, name, description, image, status, company_id)
- categories (id, name, hsn, tax_type, fixed_tax, threshold_amount, tax_below_threshold, tax_above_threshold, margin, target_audience, company_id)
- subcategories (id, name, category_id, company_id)
- purchase_orders (id, distributor_id, bill_no, payment_type, total_amount, sub_total_amount, discount, tax, adjustment, company_id)
- distributors (id, name, gstin, status) — linked via distributor_companies(distributor_id, company_id)
- expenses (id, expense_date, note, payment_mode, status, total_amount, tax_amount, expense_category_id, from_id, company_id)
- expense_categories (id, name, status, company_id)
- company_users (company_id, user_id, name, phone, role, deleted) — compound PK (company_id, user_id)
- bills (id, bill_no, total, discount, tax, payment_mode, status, client_id, company_id, created_at)
- bill_items (id, bill_id, item_id, variant_id, qty, price, discount, tax)
- clients (id, name, phone, email, company_id)
- coupons (id, code, type, value, min_amount, max_discount, usage_limit, used_count, company_id)
- accounts (id, name, phone, company_id) — B2B credit accounts
- investments (id, company_id, "userId", direction, amount, payment_mode, status, note, created_at) — note: "userId" is camelCase in DB
- money_transactions (id, company_id, party_type, direction, status, amount, payment_mode, account_id, note, created_at)
- bank_accounts (id, company_id, bank_name, acc_holder_name, account_no, ifsc, gstin, upi_id, opening_balance)
- account_transfers (id, company_id, from_type, from_account_id, to_type, to_account_id, amount, note, created_at)
- cash_accounts (id, company_id, name) — one per company
- distributor_payments (id, distributor_id, company_id, purchase_order_id, amount, payment_type, created_at)`;

export const queryDbTools = [
  {
    name: 'query_db',
    description: QUERY_DB_DESCRIPTION,
    inputSchema: {
      type: 'object' as const,
      properties: {
        sql: { type: 'string', description: 'SQL query. Use $1 for companyId. Example: SELECT * FROM products WHERE company_id = $1 LIMIT 10' },
        params: { type: 'array', items: { type: 'string' }, description: 'Extra params starting from $2' },
        companyId: { type: 'string' },
      },
      required: ['sql'],
    },
    handler: async (args: { sql: string; params?: (string | number | boolean)[]; companyId?: string }) => {
      const companyId = args.companyId || COMPANY_ID;
      const sql = args.sql.trim();

      if (BLOCKED_PATTERNS.test(sql)) {
        return { error: 'Only SELECT queries are allowed. No INSERT, UPDATE, DELETE, DROP, ALTER, or TRUNCATE.' };
      }
      if (!/^\s*(SELECT|WITH)\b/i.test(sql)) {
        return { error: 'Query must start with SELECT or WITH.' };
      }

      const queryParams: unknown[] = [companyId, ...(args.params ?? [])];
      const limited = /\bLIMIT\s+\d+/i.test(sql) ? sql : `${sql} LIMIT 100`;
      const { rows, rowCount } = await pool.query(limited, queryParams);
      return { rows, rowCount };
    },
  },
];
