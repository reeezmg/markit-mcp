import crypto from 'crypto';
import { pool, COMPANY_ID } from '../db';
import type { VariantInput } from '../types';

const cid = (args: { companyId?: string }) => args.companyId ?? COMPANY_ID;

export const productTools = [
  {
    name: 'list_products',
    description: 'List products for the company with optional search and filters. Returns paginated product list with variant count.',
    inputSchema: {
      type: 'object' as const,
      properties: {
        search: { type: 'string', description: 'Search by product name' },
        categoryId: { type: 'string', description: 'Filter by category UUID' },
        brandId: { type: 'string', description: 'Filter by brand name' },
        limit: { type: 'number', description: 'Results per page (default 20)' },
        offset: { type: 'number', description: 'Pagination offset (default 0)' },
        companyId: { type: 'string', description: 'Company UUID (injected by bridge — do not set manually)' },
      },
    },
    handler: async (args: { search?: string; categoryId?: string; brandId?: string; limit?: number; offset?: number; companyId?: string }) => {
      const companyId = cid(args);
      const limit = args.limit ?? 20;
      const offset = args.offset ?? 0;
      const params: unknown[] = [companyId, limit, offset];
      let whereExtra = '';

      if (args.search) {
        params.push(`%${args.search}%`);
        whereExtra += ` AND p.name ILIKE $${params.length}`;
      }
      if (args.categoryId) {
        params.push(args.categoryId);
        whereExtra += ` AND p.category_id = $${params.length}`;
      }
      if (args.brandId) {
        params.push(`%${args.brandId}%`);
        whereExtra += ` AND b.name ILIKE $${params.length}`;
      }

      const { rows } = await pool.query(
        `SELECT p.id, p.name, b.name AS brand, p.description, p.created_at,
                c.name AS category_name,
                po.bill_no,
                COUNT(v.id)::int AS variant_count
         FROM products p
         LEFT JOIN brands b ON p.brand_id = b.id
         LEFT JOIN categories c ON p.category_id = c.id
         LEFT JOIN purchase_orders po ON p.purchaseorder_id = po.id
         LEFT JOIN variants v ON v.product_id = p.id AND v.company_id = p.company_id
         WHERE p.company_id = $1 AND p.status = true
         ${whereExtra}
         GROUP BY p.id, p.name, b.name, p.description, p.created_at, c.name, po.bill_no
         ORDER BY p.created_at DESC
         LIMIT $2 OFFSET $3`,
        params
      );

      const { rows: countRows } = await pool.query(
        `SELECT COUNT(*)::int AS total FROM products WHERE company_id = $1 AND status = true`,
        [companyId]
      );

      return { products: rows, total: countRows[0].total };
    },
  },

  {
    name: 'get_product',
    description: 'Get full product details including all variants and their inventory items (size, qty, barcode).',
    inputSchema: {
      type: 'object' as const,
      properties: {
        productId: { type: 'string', description: 'Product UUID' },
        companyId: { type: 'string' },
      },
      required: ['productId'],
    },
    handler: async (args: { productId: string; companyId?: string }) => {
      const companyId = cid(args);
      const { rows: products } = await pool.query(
        `SELECT p.*, c.name AS category_name, sc.name AS subcategory_name,
                po.bill_no, po.total_amount, po.payment_type,
                d.name AS distributor_name
         FROM products p
         LEFT JOIN categories c ON p.category_id = c.id
         LEFT JOIN categories sc ON p.subcategory_id = sc.id
         LEFT JOIN purchase_orders po ON p.purchaseorder_id = po.id
         LEFT JOIN distributors d ON po.distributor_id = d.id
         WHERE p.id = $1 AND p.company_id = $2`,
        [args.productId, companyId]
      );

      if (!products.length) return { error: 'Product not found' };

      const { rows: variants } = await pool.query(
        `SELECT v.id, v.name, v.code, v.s_price, v.p_price, v.d_price,
                v.discount, v.tax, v.images, v.delivery_type,
                json_agg(json_build_object('id', i.id, 'size', i.size, 'qty', i.qty, 'initial_qty', i.initial_qty, 'barcode', i.barcode)) AS items
         FROM variants v
         LEFT JOIN items i ON i.variant_id = v.id AND i.company_id = v.company_id
         WHERE v.product_id = $1 AND v.company_id = $2 AND v.status = true
         GROUP BY v.id`,
        [args.productId, companyId]
      );

      return { product: products[0], variants };
    },
  },

  {
    name: 'create_product',
    description: 'Create a new product with variants and inventory under an existing purchase order. Note: Dont Include Brand, Category, or Subcategory in Product Name. IMPORTANT: Always call create_purchase_order first to get a poId, then use that same poId for all products in the same order. After creating, ask the user if they want to add more products to the same order. Brand, category, and subcategory can be passed as names — they will be looked up or auto-created if they don\'t exist.',
    inputSchema: {
      type: 'object' as const,
      properties: {
        name: { type: 'string', description: 'Product name -  Dont Include Brand name, Category name, or Subcategory name in Product Name' },
        poId: { type: 'string', description: 'Purchase order UUID — every product must belong to a PO' },
        brand: { type: 'string', description: 'Brand name (will be looked up or created)' },
        description: { type: 'string' },
        category: { type: 'string', description: 'Category name (will be looked up or created)' },
        subcategory: { type: 'string', description: 'Subcategory name (will be looked up or created under the category)' },
        deliveryType: { type: 'string', description: 'Default: trynbuy' },
        variants: {
          type: 'array',
          description: 'Variants to create',
          items: {
            type: 'object',
            properties: {
              name: { type: 'string' },
              code: { type: 'string' },
              sprice: { type: 'number', description: 'Selling price' },
              pprice: { type: 'number', description: 'Purchase price' },
              dprice: { type: 'number', description: 'Discounted price' },
              discount: { type: 'number' },
              items: { type: 'array', items: { type: 'object', properties: { size: { type: ['string', 'null'] }, qty: { type: 'number' } }, required: ['qty'] } },
            },
            required: ['name', 'sprice'],
          },
        },
        companyId: { type: 'string' },
      },
      required: ['name', 'poId', 'variants'],
    },
    handler: async (args: {
      name: string; poId: string; brand?: string; description?: string;
      category?: string; subcategory?: string; deliveryType?: string;
      variants: VariantInput[]; companyId?: string;
    }) => {
      const companyId = cid(args);
      const TRANSIENT_CODES = ['40001', '40P01', '53300', '57P01', '55006', '08006', '08003'];

      // Resolve brand name → UUID (lookup only, never auto-create)
      let brandId: string | null = null;
      if (args.brand) {
        const { rows: brandRows } = await pool.query(
          `SELECT id FROM brands WHERE LOWER(name) = LOWER($1) AND company_id = $2 LIMIT 1`,
          [args.brand, companyId]
        );
        if (brandRows.length) {
          brandId = brandRows[0].id;
        } else {
          return { error: `Brand "${args.brand}" not found. Ask the user if they want to create it using create_brand, then retry.`, purchaseOrderId: args.poId };
        }
      }

      // Category is REQUIRED
      if (!args.category) {
        return { error: 'Category is required. Ask the user which category this product belongs to.', purchaseOrderId: args.poId };
      }

      // Resolve category name → UUID (lookup only, never auto-create)
      let categoryId: string | null = null;
      const { rows: catRows } = await pool.query(
        `SELECT id FROM categories WHERE LOWER(name) = LOWER($1) AND company_id = $2 LIMIT 1`,
        [args.category, companyId]
      );
      if (catRows.length) {
        categoryId = catRows[0].id;
      } else {
        return { error: `Category "${args.category}" not found. Ask the user if they want to create it using create_category, then retry.`, purchaseOrderId: args.poId };
      }

      // Resolve subcategory name → UUID (lookup only)
      let subcategoryId: string | null = null;
      if (args.subcategory && categoryId) {
        const { rows: subRows } = await pool.query(
          `SELECT id FROM subcategories WHERE LOWER(name) = LOWER($1) AND company_id = $2 AND category_id = $3 LIMIT 1`,
          [args.subcategory, companyId, categoryId]
        );
        if (subRows.length) {
          subcategoryId = subRows[0].id;
        } else {
          return { error: `Subcategory "${args.subcategory}" not found under category "${args.category}". Ask the user if they want to create it using create_subcategory, then retry.`, purchaseOrderId: args.poId };
        }
      }

      async function runTx(attempt = 1): Promise<Record<string, unknown>> {
        const client = await pool.connect();
        const productId = crypto.randomUUID();
        try {
          await client.query('BEGIN');
          await client.query(
            `INSERT INTO products (id, name, brand_id, description, status, company_id, purchaseorder_id,
              category_id, subcategory_id, created_at, updated_at)
             VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,now(),now())`,
            [productId, args.name, brandId, args.description ?? '', true, companyId, args.poId, categoryId, subcategoryId]
          );
          for (const variant of args.variants) {
            const variantId = crypto.randomUUID();
            await client.query(
              `INSERT INTO variants(id, name, code, s_price, p_price, d_price, discount, delivery_type,
                status, tax, images, company_id, product_id, created_at, updated_at)
               VALUES($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,now(),now())`,
              [variantId, variant.name, variant.code ?? null, variant.sprice, variant.pprice ?? 0,
               variant.dprice ?? 0, variant.discount ?? 0, args.deliveryType ?? 'trynbuy', true, 0, [], companyId, productId]
            );
            if (variant.items?.length) {
              for (const item of variant.items) {
                await client.query(
                  `INSERT INTO items(id, size, qty, initial_qty, company_id, variant_id, created_at, updated_at)
                   VALUES($1,$2,$3,$4,$5,$6,now(),now())`,
                  [crypto.randomUUID(), item.size ?? null, item.qty ?? 0, item.qty ?? 0, companyId, variantId]
                );
              }
            }
          }
          // Recalculate PO subtotal_amount and total_amount
          await client.query(
            `UPDATE purchase_orders po
             SET subtotal_amount = COALESCE(sub.total, 0),
                 total_amount = COALESCE(sub.total, 0) - COALESCE(po.discount, 0) + COALESCE(po.tax, 0) + COALESCE(po.adjustment, 0),
                 updated_at = now()
             FROM (
               SELECT SUM(v.p_price * i.qty) AS total
               FROM products p
               JOIN variants v ON v.product_id = p.id AND v.company_id = p.company_id
               JOIN items i ON i.variant_id = v.id AND i.company_id = v.company_id
               WHERE p.purchaseorder_id = $1 AND p.company_id = $2 AND v.status = true AND p.status = true
             ) sub
             WHERE po.id = $1 AND po.company_id = $2`,
            [args.poId, companyId]
          );

          // Fetch updated PO amounts
          const { rows: poRows } = await client.query(
            `SELECT subtotal_amount, total_amount FROM purchase_orders WHERE id = $1 AND company_id = $2`,
            [args.poId, companyId]
          );

          await client.query('COMMIT');
          return {
            success: true,
            productId,
            productName: args.name,
            purchaseOrderId: args.poId,
            brandId,
            categoryId,
            subcategoryId,
            variantCount: args.variants.length,
            poSubTotalAmount: poRows[0]?.subtotal_amount ?? 0,
            poTotalAmount: poRows[0]?.total_amount ?? 0,
          };
        } catch (err: any) {
          await client.query('ROLLBACK');
          if (TRANSIENT_CODES.includes(err.code) && attempt < 3) {
            await new Promise((r) => setTimeout(r, 200 * Math.pow(2, attempt - 1)));
            return runTx(attempt + 1);
          }
          throw err;
        } finally {
          client.release();
        }
      }
      return runTx();
    },
  },

  {
    name: 'update_product',
    description: 'Update a product\'s name, brand, description, category, or subcategory.',
    inputSchema: {
      type: 'object' as const,
      properties: {
        productId: { type: 'string' },
        name: { type: 'string' },
        brand: { type: 'string' },
        description: { type: 'string' },
        categoryId: { type: 'string' },
        subcategoryId: { type: 'string' },
        companyId: { type: 'string' },
      },
      required: ['productId'],
    },
    handler: async (args: { productId: string; name?: string; brand?: string; description?: string; categoryId?: string; subcategoryId?: string; companyId?: string }) => {
      const companyId = cid(args);
      const sets: string[] = ['updated_at = now()'];
      const params: unknown[] = [args.productId, companyId];
      if (args.name !== undefined) { params.push(args.name); sets.push(`name = $${params.length}`); }
      if (args.brand !== undefined) {
        // Resolve brand name → UUID
        const { rows: brandRows } = await pool.query(
          `SELECT id FROM brands WHERE LOWER(name) = LOWER($1) AND company_id = $2 LIMIT 1`,
          [args.brand, companyId]
        );
        if (brandRows.length) {
          params.push(brandRows[0].id); sets.push(`brand_id = $${params.length}`);
        } else {
          const newBrandId = crypto.randomUUID();
          await pool.query(
            `INSERT INTO brands (id, name, company_id, created_at, updated_at) VALUES ($1,$2,$3,now(),now())`,
            [newBrandId, args.brand, companyId]
          );
          params.push(newBrandId); sets.push(`brand_id = $${params.length}`);
        }
      }
      if (args.description !== undefined) { params.push(args.description); sets.push(`description = $${params.length}`); }
      if (args.categoryId !== undefined) { params.push(args.categoryId); sets.push(`category_id = $${params.length}`); }
      if (args.subcategoryId !== undefined) { params.push(args.subcategoryId); sets.push(`subcategory_id = $${params.length}`); }
      if (sets.length === 1) return { error: 'No fields to update' };
      await pool.query(`UPDATE products SET ${sets.join(', ')} WHERE id = $1 AND company_id = $2`, params);
      const { rows } = await pool.query(
        `SELECT p.id, p.name, b.name AS brand, c.name AS category_name, sc.name AS subcategory_name
         FROM products p
         LEFT JOIN brands b ON p.brand_id = b.id
         LEFT JOIN categories c ON p.category_id = c.id
         LEFT JOIN categories sc ON p.subcategory_id = sc.id
         WHERE p.id = $1 AND p.company_id = $2`,
        [args.productId, companyId]
      );
      return { success: true, productId: args.productId, ...rows[0] };
    },
  },

  {
    name: 'update_variant',
    description: 'Update a variant\'s fields (name, prices, discount, status, delivery type) and/or upsert its inventory items. Items are matched by size — existing sizes are updated, new sizes are added.',
    inputSchema: {
      type: 'object' as const,
      properties: {
        variantId: { type: 'string', description: 'Variant UUID' },
        name: { type: 'string' },
        code: { type: 'string' },
        sprice: { type: 'number', description: 'Selling price' },
        pprice: { type: 'number', description: 'Purchase price' },
        dprice: { type: 'number', description: 'Discounted price' },
        discount: { type: 'number' },
        deliveryType: { type: 'string' },
        status: { type: 'boolean' },
        items: {
          type: 'array',
          description: 'Inventory items to upsert — matched by size. Existing sizes get qty updated, new sizes are added.',
          items: {
            type: 'object',
            properties: {
              size: { type: ['string', 'null'], description: 'Size label (e.g. S, M, L, XL, Free, 30, 32). null for no size.' },
              qty: { type: 'number', description: 'Quantity to set' },
              initial_qty: { type: 'number', description: 'Initial quantity — only set when user explicitly asks' },
            },
            required: ['qty'],
          },
        },
        companyId: { type: 'string' },
      },
      required: ['variantId'],
    },
    handler: async (args: {
      variantId: string; name?: string; code?: string; sprice?: number; pprice?: number;
      dprice?: number; discount?: number; deliveryType?: string; status?: boolean;
      items?: { size?: string | null; qty: number; initial_qty?: number }[]; companyId?: string;
    }) => {
      const companyId = cid(args);

      // Update variant fields
      const sets: string[] = ['updated_at = now()'];
      const params: unknown[] = [args.variantId, companyId];
      const fields: Record<string, string> = {
        name: 'name', code: 'code', sprice: 's_price', pprice: 'p_price',
        dprice: 'd_price', discount: 'discount', deliveryType: 'delivery_type', status: 'status',
      };
      for (const [key, col] of Object.entries(fields)) {
        const val = (args as Record<string, unknown>)[key];
        if (val !== undefined) { params.push(val); sets.push(`${col} = $${params.length}`); }
      }
      if (sets.length > 1) {
        await pool.query(`UPDATE variants SET ${sets.join(', ')} WHERE id = $1 AND company_id = $2`, params);
      }

      // Upsert items by size
      const upsertedItems: { size: string | null; qty: number; action: string }[] = [];
      if (args.items && args.items.length > 0) {
        for (const item of args.items) {
          const size = item.size ?? null;
          // Check if item with this size already exists for this variant
          const { rows: existing } = await pool.query(
            `SELECT id, qty FROM items WHERE variant_id = $1 AND company_id = $2 AND ${size === null ? 'size IS NULL' : 'size = $3'}`,
            size === null ? [args.variantId, companyId] : [args.variantId, companyId, size]
          );
          if (existing.length) {
            if (item.initial_qty !== undefined) {
              await pool.query(
                `UPDATE items SET qty = $1, initial_qty = $2, updated_at = now() WHERE id = $3`,
                [item.qty, item.initial_qty, existing[0].id]
              );
            } else {
              await pool.query(
                `UPDATE items SET qty = $1, updated_at = now() WHERE id = $2`,
                [item.qty, existing[0].id]
              );
            }
            upsertedItems.push({ size, qty: item.qty, action: 'updated' });
          } else {
            await pool.query(
              `INSERT INTO items (id, size, qty, initial_qty, company_id, variant_id, created_at, updated_at)
               VALUES ($1,$2,$3,$4,$5,$6,now(),now())`,
              [crypto.randomUUID(), size, item.qty, item.qty, companyId, args.variantId]
            );
            upsertedItems.push({ size, qty: item.qty, action: 'added' });
          }
        }
      }

      return { success: true, variantId: args.variantId, items: upsertedItems };
    },
  },

  {
    name: 'delete_items',
    description: 'Delete specific inventory items from a variant by size or item ID.',
    inputSchema: {
      type: 'object' as const,
      properties: {
        variantId: { type: 'string', description: 'Variant UUID' },
        sizes: { type: 'array', items: { type: ['string', 'null'] }, description: 'Sizes to delete (e.g. ["S", "M"])' },
        itemIds: { type: 'array', items: { type: 'string' }, description: 'Item UUIDs to delete' },
        companyId: { type: 'string' },
      },
      required: ['variantId'],
    },
    handler: async (args: { variantId: string; sizes?: (string | null)[]; itemIds?: string[]; companyId?: string }) => {
      const companyId = cid(args);
      let deleted = 0;
      if (args.itemIds && args.itemIds.length > 0) {
        const { rowCount } = await pool.query(
          `DELETE FROM items WHERE variant_id = $1 AND company_id = $2 AND id = ANY($3)`,
          [args.variantId, companyId, args.itemIds]
        );
        deleted += rowCount ?? 0;
      }
      if (args.sizes && args.sizes.length > 0) {
        for (const size of args.sizes) {
          const { rowCount } = await pool.query(
            `DELETE FROM items WHERE variant_id = $1 AND company_id = $2 AND ${size === null ? 'size IS NULL' : 'size = $3'}`,
            size === null ? [args.variantId, companyId] : [args.variantId, companyId, size]
          );
          deleted += rowCount ?? 0;
        }
      }
      return { success: true, deletedCount: deleted };
    },
  },

  {
    name: 'get_stock_summary',
    description: 'Get aggregated stock summary grouped by category, brand, or distributor.',
    inputSchema: {
      type: 'object' as const,
      properties: {
        groupBy: { type: 'string', enum: ['category', 'brand', 'distributor'] },
        companyId: { type: 'string' },
      },
      required: ['groupBy'],
    },
    handler: async (args: { groupBy: 'category' | 'brand' | 'distributor'; companyId?: string }) => {
      const companyId = cid(args);
      let groupExpr: string;
      let joinExtra = '';
      switch (args.groupBy) {
        case 'category': groupExpr = `COALESCE(c.name, 'Uncategorized')`; joinExtra = `LEFT JOIN categories c ON p.category_id = c.id`; break;
        case 'brand': groupExpr = `COALESCE(b.name, 'Unbranded')`; joinExtra = `LEFT JOIN brands b ON p.brand_id = b.id`; break;
        case 'distributor': groupExpr = `COALESCE(d.name, 'Unknown Distributor')`; joinExtra = `LEFT JOIN purchase_orders po ON p.purchaseorder_id = po.id LEFT JOIN distributors d ON po.distributor_id = d.id`; break;
      }
      const { rows } = await pool.query(
        `SELECT ${groupExpr} AS group_key, SUM(i.qty) AS total_qty,
                SUM(i.qty * v.s_price)::numeric(12,2) AS selling_value,
                SUM(i.qty * v.p_price)::numeric(12,2) AS purchase_value
         FROM items i JOIN variants v ON i.variant_id = v.id JOIN products p ON v.product_id = p.id ${joinExtra}
         WHERE i.company_id = $1 AND v.status = true AND p.status = true
         GROUP BY 1 ORDER BY total_qty DESC`,
        [companyId]
      );
      return { groupBy: args.groupBy, summary: rows };
    },
  },

  {
    name: 'search_by_barcode',
    description: 'Find a product variant and item by barcode.',
    inputSchema: {
      type: 'object' as const,
      properties: {
        barcode: { type: 'string' },
        companyId: { type: 'string' },
      },
      required: ['barcode'],
    },
    handler: async (args: { barcode: string; companyId?: string }) => {
      const companyId = cid(args);
      const { rows } = await pool.query(
        `SELECT i.id AS item_id, i.size, i.qty, i.barcode,
                v.id AS variant_id, v.name AS variant_name, v.code, v.s_price, v.p_price, v.tax,
                p.id AS product_id, p.name AS product_name, b.name AS brand
         FROM items i JOIN variants v ON i.variant_id = v.id JOIN products p ON v.product_id = p.id
         LEFT JOIN brands b ON p.brand_id = b.id
         WHERE i.barcode = $1 AND i.company_id = $2`,
        [args.barcode, companyId]
      );
      if (!rows.length) return { error: 'Barcode not found' };
      return rows[0];
    },
  },
];
