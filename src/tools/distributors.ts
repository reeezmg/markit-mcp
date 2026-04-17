import crypto from 'crypto';
import { pool, COMPANY_ID } from '../db';

const cid = (args: { companyId?: string }) => args.companyId ?? COMPANY_ID;

export const distributorTools = [
  // ─── List Distributors ────────────────────────────────────────────────────────

  {
    name: 'list_distributors',
    description: 'List all distributors linked to this company with credit/payment/due summary and opening due.',
    inputSchema: {
      type: 'object' as const,
      properties: {
        search: { type: 'string', description: 'Search by distributor name' },
        companyId: { type: 'string' },
      },
    },
    handler: async (args: { search?: string; companyId?: string }) => {
      const companyId = cid(args);
      const params: unknown[] = [companyId];
      let nameFilter = '';
      if (args.search) {
        params.push(`%${args.search}%`);
        nameFilter = `AND d.name ILIKE $${params.length}`;
      }
      const { rows } = await pool.query(
        `SELECT d.id, d.name, d.status, d.gstin,
                dc.opening_due,
                dc.opening_due_date,
                COUNT(DISTINCT po.id)::int AS po_count,
                COALESCE(SUM(DISTINCT po.total_amount), 0)::numeric(12,2) AS total_purchased,
                COALESCE((SELECT SUM(cr.amount) FROM distributor_credits cr
                          WHERE cr.distributor_id = d.id AND cr.company_id = dc.company_id), 0)::numeric(12,2) AS total_credits,
                COALESCE((SELECT SUM(dp.amount) FROM distributor_payments dp
                          WHERE dp.distributor_id = d.id AND dp.company_id = dc.company_id), 0)::numeric(12,2) AS total_payments,
                (COALESCE(dc.opening_due, 0) +
                 COALESCE((SELECT SUM(cr.amount) FROM distributor_credits cr
                           WHERE cr.distributor_id = d.id AND cr.company_id = dc.company_id), 0) -
                 COALESCE((SELECT SUM(dp.amount) FROM distributor_payments dp
                           WHERE dp.distributor_id = d.id AND dp.company_id = dc.company_id), 0))::numeric(12,2) AS total_due
         FROM distributor_companies dc
         JOIN distributors d ON dc.distributor_id = d.id
         LEFT JOIN purchase_orders po ON po.distributor_id = d.id AND po.company_id = dc.company_id
         WHERE dc.company_id = $1 ${nameFilter}
         GROUP BY d.id, d.name, d.status, d.gstin, dc.opening_due, dc.opening_due_date
         ORDER BY d.name`,
        params
      );
      return { distributors: rows };
    },
  },

  // ─── Get Distributor ──────────────────────────────────────────────────────────

  {
    name: 'get_distributor',
    description: 'Get full distributor details: address, bank info, payment/credit summary, opening due, and total due.',
    inputSchema: {
      type: 'object' as const,
      properties: {
        distributorId: { type: 'string', description: 'Distributor UUID' },
        companyId: { type: 'string' },
      },
      required: ['distributorId'],
    },
    handler: async (args: { distributorId: string; companyId?: string }) => {
      const companyId = cid(args);
      const { rows } = await pool.query(
        `SELECT d.id, d.name, d.status, d.gstin,
                d.acc_holder_name, d.ifsc, d.account_no, d.bank_name, d.upi_id,
                dc.opening_due, dc.opening_due_date,
                a.street, a.locality, a.city, a.state, a.pincode,
                COALESCE((SELECT SUM(cr.amount) FROM distributor_credits cr
                          WHERE cr.distributor_id = d.id AND cr.company_id = $2), 0)::numeric(12,2) AS total_credits,
                COALESCE((SELECT SUM(dp.amount) FROM distributor_payments dp
                          WHERE dp.distributor_id = d.id AND dp.company_id = $2), 0)::numeric(12,2) AS total_payments,
                (SELECT COUNT(*)::int FROM purchase_orders po
                 WHERE po.distributor_id = d.id AND po.company_id = $2) AS po_count
         FROM distributors d
         JOIN distributor_companies dc ON dc.distributor_id = d.id AND dc.company_id = $2
         LEFT JOIN addresses a ON a.distributor_id = d.id
         WHERE d.id = $1`,
        [args.distributorId, companyId]
      );
      if (!rows.length) return { error: 'Distributor not found or not linked to this company' };
      const r = rows[0];
      const totalDue = (parseFloat(r.opening_due) || 0) + parseFloat(r.total_credits) - parseFloat(r.total_payments);
      return {
        ...r,
        total_due: totalDue.toFixed(2),
      };
    },
  },

  // ─── Create Distributor ───────────────────────────────────────────────────────

  {
    name: 'create_distributor',
    description: 'Create a new distributor with optional address, bank details, and opening due. Creates both the distributor record and the company link.',
    inputSchema: {
      type: 'object' as const,
      properties: {
        name: { type: 'string', description: 'Distributor/supplier name' },
        gstin: { type: 'string' },
        accHolderName: { type: 'string', description: 'Bank account holder name' },
        ifsc: { type: 'string' },
        accountNo: { type: 'string' },
        bankName: { type: 'string' },
        upiId: { type: 'string' },
        street: { type: 'string' },
        locality: { type: 'string' },
        city: { type: 'string' },
        state: { type: 'string' },
        pincode: { type: 'string' },
        openingDue: { type: 'number', description: 'Opening due amount. Positive = company owes distributor, negative = distributor owes company' },
        openingDueDate: { type: 'string', description: 'Opening due date (ISO string)' },
        companyId: { type: 'string' },
      },
      required: ['name'],
    },
    handler: async (args: {
      name: string; gstin?: string; accHolderName?: string; ifsc?: string;
      accountNo?: string; bankName?: string; upiId?: string;
      street?: string; locality?: string; city?: string; state?: string; pincode?: string;
      openingDue?: number; openingDueDate?: string; companyId?: string;
    }) => {
      const companyId = cid(args);

      // Check duplicate name within this company
      const { rows: existing } = await pool.query(
        `SELECT d.id, d.name FROM distributors d
         JOIN distributor_companies dc ON dc.distributor_id = d.id
         WHERE LOWER(d.name) = LOWER($1) AND dc.company_id = $2 LIMIT 1`,
        [args.name, companyId]
      );
      if (existing.length) {
        return { error: `Distributor "${existing[0].name}" already exists`, existingId: existing[0].id };
      }

      const id = crypto.randomUUID();
      const client = await pool.connect();
      try {
        await client.query('BEGIN');

        // Insert distributor
        await client.query(
          `INSERT INTO distributors (id, name, images, status, acc_holder_name, ifsc, account_no, bank_name, gstin, upi_id)
           VALUES ($1, $2, NULL, true, $3, $4, $5, $6, $7, $8)`,
          [id, args.name, args.accHolderName ?? null, args.ifsc ?? null,
           args.accountNo ?? null, args.bankName ?? null, args.gstin ?? null, args.upiId ?? null]
        );

        // Insert distributor-company link with opening due
        const openingDueDate = args.openingDueDate ? new Date(args.openingDueDate).toISOString() : null;
        await client.query(
          `INSERT INTO distributor_companies (distributor_id, company_id, opening_due, opening_due_date)
           VALUES ($1, $2, $3, $4)`,
          [id, companyId, args.openingDue ?? 0, openingDueDate]
        );

        // Insert address if any address field provided
        const hasAddress = args.street || args.locality || args.city || args.state || args.pincode;
        if (hasAddress) {
          const addrId = crypto.randomUUID();
          await client.query(
            `INSERT INTO addresses (id, street, locality, city, state, pincode, distributor_id, created_at, updated_at)
             VALUES ($1, $2, $3, $4, $5, $6, $7, now(), now())`,
            [addrId, args.street ?? null, args.locality ?? null, args.city ?? null,
             args.state ?? null, args.pincode ?? null, id]
          );
        }

        await client.query('COMMIT');
        return { success: true, distributorId: id, name: args.name };
      } catch (err: any) {
        await client.query('ROLLBACK');
        throw err;
      } finally {
        client.release();
      }
    },
  },

  // ─── Update Distributor ───────────────────────────────────────────────────────

  {
    name: 'update_distributor',
    description: "Update a distributor's details, bank info, address, or opening due.",
    inputSchema: {
      type: 'object' as const,
      properties: {
        distributorId: { type: 'string', description: 'Distributor UUID' },
        name: { type: 'string' },
        gstin: { type: 'string' },
        accHolderName: { type: 'string' },
        ifsc: { type: 'string' },
        accountNo: { type: 'string' },
        bankName: { type: 'string' },
        upiId: { type: 'string' },
        status: { type: 'boolean' },
        street: { type: 'string' },
        locality: { type: 'string' },
        city: { type: 'string' },
        state: { type: 'string' },
        pincode: { type: 'string' },
        openingDue: { type: 'number', description: 'Opening due amount. Positive = company owes distributor, negative = distributor owes company' },
        openingDueDate: { type: 'string', description: 'Opening due date (ISO string)' },
        companyId: { type: 'string' },
      },
      required: ['distributorId'],
    },
    handler: async (args: {
      distributorId: string; name?: string; gstin?: string; accHolderName?: string;
      ifsc?: string; accountNo?: string; bankName?: string; upiId?: string; status?: boolean;
      street?: string; locality?: string; city?: string; state?: string; pincode?: string;
      openingDue?: number; openingDueDate?: string; companyId?: string;
    }) => {
      const companyId = cid(args);

      // Verify link exists
      const { rows: link } = await pool.query(
        `SELECT 1 FROM distributor_companies WHERE distributor_id = $1 AND company_id = $2`,
        [args.distributorId, companyId]
      );
      if (!link.length) return { error: 'Distributor not found or not linked to this company' };

      // Dynamic update on distributors table
      const distSets: string[] = [];
      const distParams: unknown[] = [args.distributorId];
      const addDistField = (val: unknown, col: string) => {
        if (val !== undefined) { distParams.push(val); distSets.push(`${col} = $${distParams.length}`); }
      };
      addDistField(args.name, 'name');
      addDistField(args.gstin, 'gstin');
      addDistField(args.accHolderName, 'acc_holder_name');
      addDistField(args.ifsc, 'ifsc');
      addDistField(args.accountNo, 'account_no');
      addDistField(args.bankName, 'bank_name');
      addDistField(args.upiId, 'upi_id');
      addDistField(args.status, 'status');

      if (distSets.length) {
        await pool.query(`UPDATE distributors SET ${distSets.join(', ')} WHERE id = $1`, distParams);
      }

      // Dynamic update on distributor_companies (opening due)
      const dcSets: string[] = [];
      const dcParams: unknown[] = [args.distributorId, companyId];
      if (args.openingDue !== undefined) {
        dcParams.push(args.openingDue);
        dcSets.push(`opening_due = $${dcParams.length}`);
      }
      if (args.openingDueDate !== undefined) {
        dcParams.push(new Date(args.openingDueDate).toISOString());
        dcSets.push(`opening_due_date = $${dcParams.length}`);
      }
      if (dcSets.length) {
        await pool.query(
          `UPDATE distributor_companies SET ${dcSets.join(', ')} WHERE distributor_id = $1 AND company_id = $2`,
          dcParams
        );
      }

      // Upsert address if any address field provided
      const hasAddress = args.street !== undefined || args.locality !== undefined ||
        args.city !== undefined || args.state !== undefined || args.pincode !== undefined;
      if (hasAddress) {
        const addrId = crypto.randomUUID();
        await pool.query(
          `INSERT INTO addresses (id, distributor_id, street, locality, city, state, pincode, created_at, updated_at)
           VALUES ($1, $2, $3, $4, $5, $6, $7, now(), now())
           ON CONFLICT (distributor_id) DO UPDATE SET
             street = COALESCE(EXCLUDED.street, addresses.street),
             locality = COALESCE(EXCLUDED.locality, addresses.locality),
             city = COALESCE(EXCLUDED.city, addresses.city),
             state = COALESCE(EXCLUDED.state, addresses.state),
             pincode = COALESCE(EXCLUDED.pincode, addresses.pincode),
             updated_at = now()`,
          [addrId, args.distributorId, args.street ?? null, args.locality ?? null,
           args.city ?? null, args.state ?? null, args.pincode ?? null]
        );
      }

      if (!distSets.length && !dcSets.length && !hasAddress) {
        return { error: 'No fields to update' };
      }

      // Return updated record
      const { rows } = await pool.query(
        `SELECT d.id, d.name, d.status, d.gstin, dc.opening_due, dc.opening_due_date
         FROM distributors d
         JOIN distributor_companies dc ON dc.distributor_id = d.id AND dc.company_id = $2
         WHERE d.id = $1`,
        [args.distributorId, companyId]
      );
      return { success: true, ...rows[0] };
    },
  },

  // ─── Delete Distributor ───────────────────────────────────────────────────────

  {
    name: 'delete_distributor',
    description: 'Remove a distributor from this company. Fails if purchase orders, credits, or payments are linked. Only unlinks from company — does not delete the shared distributor record.',
    inputSchema: {
      type: 'object' as const,
      properties: {
        distributorId: { type: 'string', description: 'Distributor UUID' },
        companyId: { type: 'string' },
      },
      required: ['distributorId'],
    },
    handler: async (args: { distributorId: string; companyId?: string }) => {
      const companyId = cid(args);

      // Get name
      const { rows: dist } = await pool.query(
        `SELECT d.name FROM distributors d
         JOIN distributor_companies dc ON dc.distributor_id = d.id AND dc.company_id = $2
         WHERE d.id = $1`,
        [args.distributorId, companyId]
      );
      if (!dist.length) return { error: 'Distributor not found or not linked to this company' };

      // Check linked records
      const { rows: poCnt } = await pool.query(
        `SELECT COUNT(*)::int AS count FROM purchase_orders WHERE distributor_id = $1 AND company_id = $2`,
        [args.distributorId, companyId]
      );
      if (poCnt[0].count > 0) {
        return { error: `Cannot delete — ${poCnt[0].count} purchase order(s) are linked. Remove them first.` };
      }

      const { rows: crCnt } = await pool.query(
        `SELECT COUNT(*)::int AS count FROM distributor_credits WHERE distributor_id = $1 AND company_id = $2`,
        [args.distributorId, companyId]
      );
      if (crCnt[0].count > 0) {
        return { error: `Cannot delete — ${crCnt[0].count} credit(s) are linked. Remove them first.` };
      }

      const { rows: payCnt } = await pool.query(
        `SELECT COUNT(*)::int AS count FROM distributor_payments WHERE distributor_id = $1 AND company_id = $2`,
        [args.distributorId, companyId]
      );
      if (payCnt[0].count > 0) {
        return { error: `Cannot delete — ${payCnt[0].count} payment(s) are linked. Remove them first.` };
      }

      await pool.query(
        `DELETE FROM distributor_companies WHERE distributor_id = $1 AND company_id = $2`,
        [args.distributorId, companyId]
      );
      return { success: true, deletedDistributorId: args.distributorId, name: dist[0].name };
    },
  },

  // ─── Create Distributor Payment ───────────────────────────────────────────────

  {
    name: 'create_distributor_payment',
    description: 'Record a payment made to a distributor. Reduces the amount due. Use this for actual money paid to the distributor.',
    inputSchema: {
      type: 'object' as const,
      properties: {
        distributorId: { type: 'string', description: 'Distributor UUID' },
        amount: { type: 'number', description: 'Payment amount' },
        paymentType: { type: 'string', enum: ['CASH', 'CREDIT', 'CARD', 'UPI', 'BANK', 'CHEQUE', 'RETURN'], description: 'Default: CASH' },
        remarks: { type: 'string' },
        billNo: { type: 'string' },
        date: { type: 'string', description: 'Payment date (ISO string). Defaults to now' },
        purchaseOrderId: { type: 'string', description: 'Link payment to a specific purchase order' },
        companyId: { type: 'string' },
      },
      required: ['distributorId', 'amount'],
    },
    handler: async (args: {
      distributorId: string; amount: number; paymentType?: string;
      remarks?: string; billNo?: string; date?: string; purchaseOrderId?: string; companyId?: string;
    }) => {
      const companyId = cid(args);

      // Verify distributor-company link
      const { rows: link } = await pool.query(
        `SELECT 1 FROM distributor_companies WHERE distributor_id = $1 AND company_id = $2`,
        [args.distributorId, companyId]
      );
      if (!link.length) return { error: 'Distributor not found or not linked to this company' };

      // Verify PO if provided
      if (args.purchaseOrderId) {
        const { rows: po } = await pool.query(
          `SELECT 1 FROM purchase_orders WHERE id = $1 AND distributor_id = $2 AND company_id = $3`,
          [args.purchaseOrderId, args.distributorId, companyId]
        );
        if (!po.length) return { error: 'Purchase order not found or does not belong to this distributor' };
      }

      const id = crypto.randomUUID();
      const createdAt = args.date ? new Date(args.date).toISOString() : new Date().toISOString();

      await pool.query(
        `INSERT INTO distributor_payments (id, distributor_id, company_id, amount, payment_type, remarks, bill_no, created_at, purchase_order_id)
         VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9)`,
        [id, args.distributorId, companyId, args.amount,
         args.paymentType ?? 'CASH', args.remarks ?? null, args.billNo ?? null,
         createdAt, args.purchaseOrderId ?? null]
      );

      return { success: true, paymentId: id, amount: args.amount, paymentType: args.paymentType ?? 'CASH' };
    },
  },

  // ─── Create Distributor Credit ────────────────────────────────────────────────

  {
    name: 'create_distributor_credit',
    description: 'Record a credit (amount owed) to a distributor. Increases the amount due. Use this when the company owes money to the distributor (e.g. for goods received).',
    inputSchema: {
      type: 'object' as const,
      properties: {
        distributorId: { type: 'string', description: 'Distributor UUID' },
        amount: { type: 'number', description: 'Credit amount' },
        remarks: { type: 'string' },
        billNo: { type: 'string' },
        date: { type: 'string', description: 'Credit date (ISO string). Defaults to now' },
        purchaseOrderId: { type: 'string', description: 'Link credit to a specific purchase order' },
        companyId: { type: 'string' },
      },
      required: ['distributorId', 'amount'],
    },
    handler: async (args: {
      distributorId: string; amount: number; remarks?: string;
      billNo?: string; date?: string; purchaseOrderId?: string; companyId?: string;
    }) => {
      const companyId = cid(args);

      // Verify distributor-company link
      const { rows: link } = await pool.query(
        `SELECT 1 FROM distributor_companies WHERE distributor_id = $1 AND company_id = $2`,
        [args.distributorId, companyId]
      );
      if (!link.length) return { error: 'Distributor not found or not linked to this company' };

      // Verify PO if provided
      if (args.purchaseOrderId) {
        const { rows: po } = await pool.query(
          `SELECT 1 FROM purchase_orders WHERE id = $1 AND distributor_id = $2 AND company_id = $3`,
          [args.purchaseOrderId, args.distributorId, companyId]
        );
        if (!po.length) return { error: 'Purchase order not found or does not belong to this distributor' };
      }

      const id = crypto.randomUUID();
      const createdAt = args.date ? new Date(args.date).toISOString() : new Date().toISOString();

      await pool.query(
        `INSERT INTO distributor_credits (id, distributor_id, company_id, amount, remarks, bill_no, created_at, purchase_order_id)
         VALUES ($1, $2, $3, $4, $5, $6, $7, $8)`,
        [id, args.distributorId, companyId, args.amount,
         args.remarks ?? null, args.billNo ?? null, createdAt, args.purchaseOrderId ?? null]
      );

      return { success: true, creditId: id, amount: args.amount };
    },
  },
];
