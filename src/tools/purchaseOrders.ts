import crypto from 'crypto';
import { pool, COMPANY_ID } from '../db';

const cid = (args: { companyId?: string }) => args.companyId ?? COMPANY_ID;

export const purchaseOrderTools = [
  {
    name: 'list_purchase_orders',
    description: 'List purchase orders with distributor info and product count.',
    inputSchema: {
      type: 'object' as const,
      properties: {
        distributorId: { type: 'string' },
        limit: { type: 'number' },
        offset: { type: 'number' },
        companyId: { type: 'string' },
      },
    },
    handler: async (args: { distributorId?: string; limit?: number; offset?: number; companyId?: string }) => {
      const companyId = cid(args);
      const limit = args.limit ?? 20;
      const offset = args.offset ?? 0;
      const params: unknown[] = [companyId, limit, offset];
      let whereExtra = '';
      if (args.distributorId) { params.push(args.distributorId); whereExtra = ` AND po.distributor_id = $${params.length}`; }
      const { rows } = await pool.query(
        `SELECT po.id, po.bill_no, po.purchase_order_no, po.total_amount, po.payment_type, po.created_at,
                d.name AS distributor_name, COUNT(p.id)::int AS product_count
         FROM purchase_orders po
         LEFT JOIN distributors d ON po.distributor_id = d.id
         LEFT JOIN products p ON p.purchaseorder_id = po.id
         WHERE po.company_id = $1 ${whereExtra}
         GROUP BY po.id, po.bill_no, po.purchase_order_no, po.total_amount, po.payment_type, po.created_at, d.name
         ORDER BY po.created_at DESC LIMIT $2 OFFSET $3`,
        params
      );
      return { purchaseOrders: rows };
    },
  },

  {
    name: 'get_purchase_order',
    description: 'Get full purchase order details including all products.',
    inputSchema: {
      type: 'object' as const,
      properties: { purchaseOrderId: { type: 'string' }, companyId: { type: 'string' } },
      required: ['purchaseOrderId'],
    },
    handler: async (args: { purchaseOrderId: string; companyId?: string }) => {
      const companyId = cid(args);
      const { rows: pos } = await pool.query(
        `SELECT po.*, d.name AS distributor_name FROM purchase_orders po
         LEFT JOIN distributors d ON po.distributor_id = d.id
         WHERE po.id = $1 AND po.company_id = $2`,
        [args.purchaseOrderId, companyId]
      );
      if (!pos.length) return { error: 'Purchase order not found' };
      const { rows: products } = await pool.query(
        `SELECT p.id, p.name, p.brand, c.name AS category_name,
                COUNT(v.id)::int AS variant_count, SUM(i.qty) AS total_qty
         FROM products p
         LEFT JOIN categories c ON p.category_id = c.id
         LEFT JOIN variants v ON v.product_id = p.id AND v.company_id = p.company_id
         LEFT JOIN items i ON i.variant_id = v.id AND i.company_id = p.company_id
         WHERE p.purchaseorder_id = $1 AND p.company_id = $2
         GROUP BY p.id, p.name, p.brand, c.name`,
        [args.purchaseOrderId, companyId]
      );
      return { purchaseOrder: pos[0], products };
    },
  },

  {
    name: 'create_purchase_order',
    description: 'Create a new purchase order. MUST be called before create_product — every product must belong to a PO. Returns a purchaseOrderId to use for all products in this order.',
    inputSchema: {
      type: 'object' as const,
      properties: {
        distributorId: { type: 'string' },
        billNo: { type: 'string' },
        paymentType: { type: 'string', enum: ['CASH', 'ONLINE', 'CREDIT'] },
        totalAmount: { type: 'number' },
        subTotalAmount: { type: 'number' },
        discount: { type: 'number' },
        tax: { type: 'number' },
        adjustment: { type: 'number' },
        companyId: { type: 'string' },
      },
    },
    handler: async (args: {
      distributorId?: string; billNo?: string; paymentType?: string;
      totalAmount?: number; subTotalAmount?: number; discount?: number;
      tax?: number; adjustment?: number; companyId?: string;
    }) => {
      const companyId = cid(args);
      const id = crypto.randomUUID();

      // Get purchaseCounter from company and increment it
      const { rows: companyRows } = await pool.query(
        `UPDATE companies SET purchase_counter = purchase_counter + 1
         WHERE id = $1 RETURNING purchase_counter - 1 AS current_counter`,
        [companyId]
      );
      const purchaseOrderNo = companyRows[0]?.current_counter ?? 1;

      await pool.query(
        `INSERT INTO purchase_orders (id, company_id, distributor_id, bill_no, purchase_order_no, payment_type,
          total_amount, subtotal_amount, discount, tax, adjustment, created_at, updated_at)
         VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,now(),now())`,
        [id, companyId, args.distributorId ?? null, args.billNo ?? null, purchaseOrderNo, args.paymentType ?? null,
         args.totalAmount ?? 0, args.subTotalAmount ?? 0, args.discount ?? 0, args.tax ?? 0, args.adjustment ?? 0]
      );
      return {
        success: true, purchaseOrderId: id, purchaseOrderNo,
        billNo: args.billNo ?? null, paymentType: args.paymentType ?? null,
        totalAmount: args.totalAmount ?? 0, subTotalAmount: args.subTotalAmount ?? 0,
      };
    },
  },

  {
    name: 'update_purchase_order',
    description: 'Update purchase order fields.',
    inputSchema: {
      type: 'object' as const,
      properties: {
        purchaseOrderId: { type: 'string' },
        distributorId: { type: 'string' },
        billNo: { type: 'string' },
        paymentType: { type: 'string', enum: ['CASH', 'ONLINE', 'CREDIT'] },
        totalAmount: { type: 'number' },
        subTotalAmount: { type: 'number' },
        discount: { type: 'number' },
        tax: { type: 'number' },
        adjustment: { type: 'number' },
        companyId: { type: 'string' },
      },
      required: ['purchaseOrderId'],
    },
    handler: async (args: Record<string, unknown>) => {
      const companyId = (args.companyId as string | undefined) ?? COMPANY_ID;
      const sets: string[] = ['updated_at = now()'];
      const params: unknown[] = [args.purchaseOrderId, companyId];
      const fields: Record<string, string> = {
        distributorId: 'distributor_id', billNo: 'bill_no', paymentType: 'payment_type',
        totalAmount: 'total_amount', subTotalAmount: 'subtotal_amount',
        discount: 'discount', tax: 'tax', adjustment: 'adjustment',
      };
      for (const [key, col] of Object.entries(fields)) {
        if (args[key] !== undefined) { params.push(args[key]); sets.push(`${col} = $${params.length}`); }
      }
      if (sets.length === 1) return { error: 'No fields to update' };
      await pool.query(`UPDATE purchase_orders SET ${sets.join(', ')} WHERE id = $1 AND company_id = $2`, params);
      const { rows } = await pool.query(
        `SELECT po.id, po.bill_no, po.purchase_order_no, po.total_amount, po.subtotal_amount, po.payment_type, d.name AS distributor_name
         FROM purchase_orders po LEFT JOIN distributors d ON po.distributor_id = d.id
         WHERE po.id = $1 AND po.company_id = $2`,
        [args.purchaseOrderId, companyId]
      );
      return { success: true, purchaseOrderId: args.purchaseOrderId as string, ...rows[0] };
    },
  },
];
