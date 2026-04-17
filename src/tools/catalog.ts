import crypto from 'crypto';
import { pool, COMPANY_ID } from '../db';

const cid = (args: { companyId?: string }) => args.companyId ?? COMPANY_ID;

export const catalogTools = [
  // ─── Brands ──────────────────────────────────────────────────────────────────

  {
    name: 'list_brands',
    description: 'List all brands for this company with product count and total stock qty.',
    inputSchema: {
      type: 'object' as const,
      properties: {
        search: { type: 'string', description: 'Search by brand name' },
        status: { type: 'boolean', description: 'Filter by active/inactive' },
        limit: { type: 'number' },
        offset: { type: 'number' },
        companyId: { type: 'string' },
      },
    },
    handler: async (args: { search?: string; status?: boolean; limit?: number; offset?: number; companyId?: string }) => {
      const companyId = cid(args);
      const limit = args.limit ?? 50;
      const offset = args.offset ?? 0;
      const params: unknown[] = [companyId, limit, offset];
      const wheres: string[] = ['b.company_id = $1'];
      if (args.search) { params.push(`%${args.search}%`); wheres.push(`b.name ILIKE $${params.length}`); }
      if (args.status !== undefined) { params.push(args.status); wheres.push(`b.status = $${params.length}`); }
      const { rows } = await pool.query(
        `SELECT b.id, b.name, b.description, b.image, b.status, b.created_at,
                COUNT(DISTINCT p.id)::int AS product_count,
                COALESCE(SUM(i.qty), 0)::int AS total_qty
         FROM brands b
         LEFT JOIN products p ON p.brand_id = b.id AND p.company_id = b.company_id
         LEFT JOIN variants v ON v.product_id = p.id AND v.company_id = p.company_id
         LEFT JOIN items i ON i.variant_id = v.id AND i.company_id = v.company_id
         WHERE ${wheres.join(' AND ')}
         GROUP BY b.id ORDER BY b.name
         LIMIT $2 OFFSET $3`,
        params
      );
      const countParams: unknown[] = [companyId];
      const countWheres: string[] = ['b.company_id = $1'];
      if (args.search) { countParams.push(`%${args.search}%`); countWheres.push(`b.name ILIKE $${countParams.length}`); }
      if (args.status !== undefined) { countParams.push(args.status); countWheres.push(`b.status = $${countParams.length}`); }
      const { rows: countRows } = await pool.query(
        `SELECT COUNT(*)::int AS total FROM brands b WHERE ${countWheres.join(' AND ')}`,
        countParams
      );
      return { brands: rows, total: countRows[0]?.total ?? rows.length };
    },
  },

  {
    name: 'create_brand',
    description: 'Create a new brand. Name is required.',
    inputSchema: {
      type: 'object' as const,
      properties: {
        name: { type: 'string', description: 'Brand name (required)' },
        description: { type: 'string' },
        image: { type: 'string' },
        companyId: { type: 'string' },
      },
      required: ['name'],
    },
    handler: async (args: { name: string; description?: string; image?: string; companyId?: string }) => {
      const companyId = cid(args);
      // Check if brand already exists
      const { rows: existing } = await pool.query(
        `SELECT id, name FROM brands WHERE LOWER(name) = LOWER($1) AND company_id = $2 LIMIT 1`,
        [args.name, companyId]
      );
      if (existing.length) {
        return { error: `Brand "${existing[0].name}" already exists`, existingId: existing[0].id };
      }
      const id = crypto.randomUUID();
      await pool.query(
        `INSERT INTO brands (id, name, description, image, company_id, created_at, updated_at)
         VALUES ($1,$2,$3,$4,$5,now(),now())`,
        [id, args.name, args.description ?? null, args.image ?? null, companyId]
      );
      return { success: true, brandId: id, name: args.name };
    },
  },

  {
    name: 'update_brand',
    description: 'Update a brand\'s name, description, image, or status.',
    inputSchema: {
      type: 'object' as const,
      properties: {
        brandId: { type: 'string' },
        name: { type: 'string' },
        description: { type: 'string' },
        image: { type: 'string' },
        status: { type: 'boolean' },
        companyId: { type: 'string' },
      },
      required: ['brandId'],
    },
    handler: async (args: { brandId: string; name?: string; description?: string; image?: string; status?: boolean; companyId?: string }) => {
      const companyId = cid(args);
      const sets: string[] = ['updated_at = now()'];
      const params: unknown[] = [args.brandId, companyId];
      if (args.name !== undefined) { params.push(args.name); sets.push(`name = $${params.length}`); }
      if (args.description !== undefined) { params.push(args.description); sets.push(`description = $${params.length}`); }
      if (args.image !== undefined) { params.push(args.image); sets.push(`image = $${params.length}`); }
      if (args.status !== undefined) { params.push(args.status); sets.push(`status = $${params.length}`); }
      if (sets.length === 1) return { error: 'No fields to update' };
      await pool.query(`UPDATE brands SET ${sets.join(', ')} WHERE id = $1 AND company_id = $2`, params);
      const { rows } = await pool.query(`SELECT id, name, status FROM brands WHERE id = $1 AND company_id = $2`, [args.brandId, companyId]);
      return { success: true, brandId: args.brandId, ...rows[0] };
    },
  },

  {
    name: 'delete_brand',
    description: 'Delete a brand. Will fail if products are still linked to it.',
    inputSchema: {
      type: 'object' as const,
      properties: {
        brandId: { type: 'string' },
        companyId: { type: 'string' },
      },
      required: ['brandId'],
    },
    handler: async (args: { brandId: string; companyId?: string }) => {
      const companyId = cid(args);
      // Check for linked products
      const { rows: linked } = await pool.query(
        `SELECT COUNT(*)::int AS count FROM products WHERE brand_id = $1 AND company_id = $2`,
        [args.brandId, companyId]
      );
      if (linked[0].count > 0) {
        return { error: `Cannot delete brand — ${linked[0].count} product(s) are linked to it. Remove or reassign them first.` };
      }
      const { rows } = await pool.query(`SELECT name FROM brands WHERE id = $1 AND company_id = $2`, [args.brandId, companyId]);
      await pool.query(`DELETE FROM brands WHERE id = $1 AND company_id = $2`, [args.brandId, companyId]);
      return { success: true, deletedBrandId: args.brandId, name: rows[0]?.name };
    },
  },

  // ─── Categories ──────────────────────────────────────────────────────────────

  {
    name: 'list_categories',
    description: 'List all categories for this company with product count, total qty, and subcategory count.',
    inputSchema: {
      type: 'object' as const,
      properties: {
        search: { type: 'string', description: 'Search by category name' },
        status: { type: 'boolean' },
        limit: { type: 'number' },
        offset: { type: 'number' },
        companyId: { type: 'string' },
      },
    },
    handler: async (args: { search?: string; status?: boolean; limit?: number; offset?: number; companyId?: string }) => {
      const companyId = cid(args);
      const limit = args.limit ?? 50;
      const offset = args.offset ?? 0;
      const params: unknown[] = [companyId, limit, offset];
      const wheres: string[] = ['c.company_id = $1'];
      if (args.search) { params.push(`%${args.search}%`); wheres.push(`c.name ILIKE $${params.length}`); }
      if (args.status !== undefined) { params.push(args.status); wheres.push(`c.status = $${params.length}`); }
      const { rows } = await pool.query(
        `SELECT c.id, c.name, c.description, c.image, c.status, c.hsn, c.tax_type,
                c.fixed_tax, c.threshold_amount, c.tax_below_threshold, c.tax_above_threshold,
                c.margin, c.target_audience, c.created_at,
                COUNT(DISTINCT p.id)::int AS product_count,
                COALESCE(SUM(i.qty), 0)::int AS total_qty,
                COUNT(DISTINCT sc.id)::int AS subcategory_count
         FROM categories c
         LEFT JOIN products p ON p.category_id = c.id AND p.company_id = c.company_id
         LEFT JOIN variants v ON v.product_id = p.id AND v.company_id = p.company_id
         LEFT JOIN items i ON i.variant_id = v.id AND i.company_id = v.company_id
         LEFT JOIN subcategories sc ON sc.category_id = c.id AND sc.company_id = c.company_id
         WHERE ${wheres.join(' AND ')}
         GROUP BY c.id ORDER BY c.name
         LIMIT $2 OFFSET $3`,
        params
      );
      return { categories: rows };
    },
  },

  {
    name: 'create_category',
    description: 'Create a new category. Name is required. Optionally set tax config, HSN, margin, and target audience.',
    inputSchema: {
      type: 'object' as const,
      properties: {
        name: { type: 'string', description: 'Category name (required)' },
        description: { type: 'string' },
        image: { type: 'string' },
        hsn: { type: 'string', description: 'HSN code' },
        taxType: { type: 'string', enum: ['FIXED', 'VARIABLE'], description: 'Tax type (default FIXED)' },
        fixedTax: { type: 'number', description: 'Fixed tax percentage' },
        thresholdAmount: { type: 'number', description: 'Price threshold for variable tax' },
        taxBelowThreshold: { type: 'number', description: 'Tax % below threshold' },
        taxAboveThreshold: { type: 'number', description: 'Tax % above threshold' },
        margin: { type: 'number', description: 'Margin percentage' },
        targetAudience: { type: 'string', description: 'Target audience (e.g. Men, Women, Kids)' },
        companyId: { type: 'string' },
      },
      required: ['name'],
    },
    handler: async (args: {
      name: string; description?: string; image?: string; hsn?: string;
      taxType?: string; fixedTax?: number; thresholdAmount?: number;
      taxBelowThreshold?: number; taxAboveThreshold?: number;
      margin?: number; targetAudience?: string; companyId?: string;
    }) => {
      const companyId = cid(args);
      const { rows: existing } = await pool.query(
        `SELECT id, name FROM categories WHERE LOWER(name) = LOWER($1) AND company_id = $2 LIMIT 1`,
        [args.name, companyId]
      );
      if (existing.length) {
        return { error: `Category "${existing[0].name}" already exists`, existingId: existing[0].id };
      }
      const id = crypto.randomUUID();
      await pool.query(
        `INSERT INTO categories (id, name, description, image, hsn, tax_type, fixed_tax,
          threshold_amount, tax_below_threshold, tax_above_threshold, margin, target_audience,
          company_id, created_at, updated_at)
         VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,now(),now())`,
        [id, args.name, args.description ?? null, args.image ?? null, args.hsn ?? null,
         args.taxType ?? 'FIXED', args.fixedTax ?? null, args.thresholdAmount ?? null,
         args.taxBelowThreshold ?? null, args.taxAboveThreshold ?? null,
         args.margin ?? null, args.targetAudience ?? null, companyId]
      );
      return { success: true, categoryId: id, name: args.name };
    },
  },

  {
    name: 'update_category',
    description: 'Update a category\'s name, description, image, status, tax config, HSN, margin, or target audience.',
    inputSchema: {
      type: 'object' as const,
      properties: {
        categoryId: { type: 'string' },
        name: { type: 'string' },
        description: { type: 'string' },
        image: { type: 'string' },
        status: { type: 'boolean' },
        hsn: { type: 'string' },
        taxType: { type: 'string', enum: ['FIXED', 'VARIABLE'] },
        fixedTax: { type: 'number' },
        thresholdAmount: { type: 'number' },
        taxBelowThreshold: { type: 'number' },
        taxAboveThreshold: { type: 'number' },
        margin: { type: 'number' },
        targetAudience: { type: 'string' },
        companyId: { type: 'string' },
      },
      required: ['categoryId'],
    },
    handler: async (args: Record<string, unknown>) => {
      const companyId = (args.companyId as string | undefined) ?? COMPANY_ID;
      const sets: string[] = ['updated_at = now()'];
      const params: unknown[] = [args.categoryId, companyId];
      const fields: Record<string, string> = {
        name: 'name', description: 'description', image: 'image', status: 'status',
        hsn: 'hsn', taxType: 'tax_type', fixedTax: 'fixed_tax',
        thresholdAmount: 'threshold_amount', taxBelowThreshold: 'tax_below_threshold',
        taxAboveThreshold: 'tax_above_threshold', margin: 'margin', targetAudience: 'target_audience',
      };
      for (const [key, col] of Object.entries(fields)) {
        if (args[key] !== undefined) { params.push(args[key]); sets.push(`${col} = $${params.length}`); }
      }
      if (sets.length === 1) return { error: 'No fields to update' };
      await pool.query(`UPDATE categories SET ${sets.join(', ')} WHERE id = $1 AND company_id = $2`, params);
      const { rows } = await pool.query(`SELECT id, name, status FROM categories WHERE id = $1 AND company_id = $2`, [args.categoryId, companyId]);
      return { success: true, categoryId: args.categoryId as string, ...rows[0] };
    },
  },

  {
    name: 'delete_category',
    description: 'Delete a category. Will fail if products or subcategories are still linked to it.',
    inputSchema: {
      type: 'object' as const,
      properties: {
        categoryId: { type: 'string' },
        companyId: { type: 'string' },
      },
      required: ['categoryId'],
    },
    handler: async (args: { categoryId: string; companyId?: string }) => {
      const companyId = cid(args);
      const { rows: linkedProducts } = await pool.query(
        `SELECT COUNT(*)::int AS count FROM products WHERE category_id = $1 AND company_id = $2`,
        [args.categoryId, companyId]
      );
      if (linkedProducts[0].count > 0) {
        return { error: `Cannot delete category — ${linkedProducts[0].count} product(s) are linked to it.` };
      }
      const { rows: linkedSubs } = await pool.query(
        `SELECT COUNT(*)::int AS count FROM subcategories WHERE category_id = $1 AND company_id = $2`,
        [args.categoryId, companyId]
      );
      if (linkedSubs[0].count > 0) {
        return { error: `Cannot delete category — ${linkedSubs[0].count} subcategory(ies) are linked to it. Delete them first.` };
      }
      const { rows: catRows } = await pool.query(`SELECT name FROM categories WHERE id = $1 AND company_id = $2`, [args.categoryId, companyId]);
      await pool.query(`DELETE FROM categories WHERE id = $1 AND company_id = $2`, [args.categoryId, companyId]);
      return { success: true, deletedCategoryId: args.categoryId, name: catRows[0]?.name };
    },
  },

  // ─── Subcategories ───────────────────────────────────────────────────────────

  {
    name: 'list_subcategories',
    description: 'List subcategories. Optionally filter by category.',
    inputSchema: {
      type: 'object' as const,
      properties: {
        categoryId: { type: 'string', description: 'Filter by parent category UUID' },
        search: { type: 'string' },
        status: { type: 'boolean' },
        limit: { type: 'number' },
        offset: { type: 'number' },
        companyId: { type: 'string' },
      },
    },
    handler: async (args: { categoryId?: string; search?: string; status?: boolean; limit?: number; offset?: number; companyId?: string }) => {
      const companyId = cid(args);
      const limit = args.limit ?? 50;
      const offset = args.offset ?? 0;
      const params: unknown[] = [companyId, limit, offset];
      const wheres: string[] = ['sc.company_id = $1'];
      if (args.categoryId) { params.push(args.categoryId); wheres.push(`sc.category_id = $${params.length}`); }
      if (args.search) { params.push(`%${args.search}%`); wheres.push(`sc.name ILIKE $${params.length}`); }
      if (args.status !== undefined) { params.push(args.status); wheres.push(`sc.status = $${params.length}`); }
      const { rows } = await pool.query(
        `SELECT sc.id, sc.name, sc.description, sc.image, sc.status, sc.created_at,
                c.name AS category_name, sc.category_id,
                COUNT(DISTINCT p.id)::int AS product_count,
                COALESCE(SUM(i.qty), 0)::int AS total_qty
         FROM subcategories sc
         LEFT JOIN categories c ON sc.category_id = c.id
         LEFT JOIN products p ON p.subcategory_id = sc.id AND p.company_id = sc.company_id
         LEFT JOIN variants v ON v.product_id = p.id AND v.company_id = p.company_id
         LEFT JOIN items i ON i.variant_id = v.id AND i.company_id = v.company_id
         WHERE ${wheres.join(' AND ')}
         GROUP BY sc.id, c.name ORDER BY c.name, sc.name
         LIMIT $2 OFFSET $3`,
        params
      );
      return { subcategories: rows };
    },
  },

  {
    name: 'create_subcategory',
    description: 'Create a new subcategory under a category. Name and category are required. Category can be a name (looked up or created) or a UUID.',
    inputSchema: {
      type: 'object' as const,
      properties: {
        name: { type: 'string', description: 'Subcategory name (required)' },
        category: { type: 'string', description: 'Category name or UUID (required)' },
        description: { type: 'string' },
        image: { type: 'string' },
        companyId: { type: 'string' },
      },
      required: ['name', 'category'],
    },
    handler: async (args: { name: string; category: string; description?: string; image?: string; companyId?: string }) => {
      const companyId = cid(args);
      // Resolve category — try UUID first, then name
      let categoryId: string;
      const isUUID = /^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$/i.test(args.category);
      if (isUUID) {
        const { rows } = await pool.query(`SELECT id FROM categories WHERE id = $1 AND company_id = $2`, [args.category, companyId]);
        if (!rows.length) return { error: `Category UUID "${args.category}" not found` };
        categoryId = rows[0].id;
      } else {
        const { rows } = await pool.query(
          `SELECT id FROM categories WHERE LOWER(name) = LOWER($1) AND company_id = $2 LIMIT 1`,
          [args.category, companyId]
        );
        if (!rows.length) return { error: `Category "${args.category}" not found. Create it first using create_category.` };
        categoryId = rows[0].id;
      }
      // Check duplicate
      const { rows: existing } = await pool.query(
        `SELECT id, name FROM subcategories WHERE LOWER(name) = LOWER($1) AND category_id = $2 AND company_id = $3 LIMIT 1`,
        [args.name, categoryId, companyId]
      );
      if (existing.length) {
        return { error: `Subcategory "${existing[0].name}" already exists under this category`, existingId: existing[0].id };
      }
      const id = crypto.randomUUID();
      await pool.query(
        `INSERT INTO subcategories (id, name, description, image, category_id, company_id, created_at, updated_at)
         VALUES ($1,$2,$3,$4,$5,$6,now(),now())`,
        [id, args.name, args.description ?? null, args.image ?? null, categoryId, companyId]
      );
      return { success: true, subcategoryId: id, name: args.name, categoryId };
    },
  },

  {
    name: 'update_subcategory',
    description: 'Update a subcategory\'s name, description, image, status, or parent category.',
    inputSchema: {
      type: 'object' as const,
      properties: {
        subcategoryId: { type: 'string' },
        name: { type: 'string' },
        description: { type: 'string' },
        image: { type: 'string' },
        status: { type: 'boolean' },
        categoryId: { type: 'string', description: 'Move to a different category (UUID)' },
        companyId: { type: 'string' },
      },
      required: ['subcategoryId'],
    },
    handler: async (args: { subcategoryId: string; name?: string; description?: string; image?: string; status?: boolean; categoryId?: string; companyId?: string }) => {
      const companyId = cid(args);
      const sets: string[] = ['updated_at = now()'];
      const params: unknown[] = [args.subcategoryId, companyId];
      if (args.name !== undefined) { params.push(args.name); sets.push(`name = $${params.length}`); }
      if (args.description !== undefined) { params.push(args.description); sets.push(`description = $${params.length}`); }
      if (args.image !== undefined) { params.push(args.image); sets.push(`image = $${params.length}`); }
      if (args.status !== undefined) { params.push(args.status); sets.push(`status = $${params.length}`); }
      if (args.categoryId !== undefined) { params.push(args.categoryId); sets.push(`category_id = $${params.length}`); }
      if (sets.length === 1) return { error: 'No fields to update' };
      await pool.query(`UPDATE subcategories SET ${sets.join(', ')} WHERE id = $1 AND company_id = $2`, params);
      const { rows } = await pool.query(
        `SELECT sc.id, sc.name, sc.status, sc.category_id, c.name AS category_name
         FROM subcategories sc LEFT JOIN categories c ON sc.category_id = c.id
         WHERE sc.id = $1 AND sc.company_id = $2`,
        [args.subcategoryId, companyId]
      );
      return { success: true, subcategoryId: args.subcategoryId, ...rows[0] };
    },
  },

  {
    name: 'delete_subcategory',
    description: 'Delete a subcategory. Will fail if products are still linked to it.',
    inputSchema: {
      type: 'object' as const,
      properties: {
        subcategoryId: { type: 'string' },
        companyId: { type: 'string' },
      },
      required: ['subcategoryId'],
    },
    handler: async (args: { subcategoryId: string; companyId?: string }) => {
      const companyId = cid(args);
      const { rows: linked } = await pool.query(
        `SELECT COUNT(*)::int AS count FROM products WHERE subcategory_id = $1 AND company_id = $2`,
        [args.subcategoryId, companyId]
      );
      if (linked[0].count > 0) {
        return { error: `Cannot delete subcategory — ${linked[0].count} product(s) are linked to it.` };
      }
      const { rows: subRows } = await pool.query(
        `SELECT sc.name, c.name AS category_name FROM subcategories sc LEFT JOIN categories c ON sc.category_id = c.id
         WHERE sc.id = $1 AND sc.company_id = $2`,
        [args.subcategoryId, companyId]
      );
      await pool.query(`DELETE FROM subcategories WHERE id = $1 AND company_id = $2`, [args.subcategoryId, companyId]);
      return { success: true, deletedSubcategoryId: args.subcategoryId, name: subRows[0]?.name, categoryName: subRows[0]?.category_name };
    },
  },
];
