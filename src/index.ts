import { McpServer } from '@modelcontextprotocol/sdk/server/mcp.js';
import { StdioServerTransport } from '@modelcontextprotocol/sdk/server/stdio.js';
import { z } from 'zod';
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
// db is imported lazily by tools — no eager import here

const server = new McpServer({
  name: 'markit',
  version: '1.0.0',
});

// ─── Product Tools ────────────────────────────────────────────────────────────

const { getHandler } = (() => {
  const map = new Map<string, (args: unknown) => Promise<unknown>>();
  for (const t of [...productTools, ...purchaseOrderTools, ...catalogTools, ...expenseTools, ...financeTools, ...accountTools, ...memoryTools, ...distributorTools, ...statementTools, ...reportTools]) {
    map.set(t.name, t.handler as (args: unknown) => Promise<unknown>);
  }
  return { getHandler: (name: string) => map.get(name)! };
})();

async function call(name: string, args: unknown) {
  try {
    const result = await getHandler(name)(args);
    return { content: [{ type: 'text' as const, text: JSON.stringify(result, null, 2) }] };
  } catch (err: any) {
    return { content: [{ type: 'text' as const, text: `Error: ${err.message}` }], isError: true };
  }
}

server.registerTool(
  'list_products',
  {
    description: 'List products with optional search/filter. Returns paginated list with variant count.',
    inputSchema: {
      search: z.string().optional().describe('Search by product name'),
      categoryId: z.string().optional().describe('Filter by category UUID'),
      brandId: z.string().optional().describe('Filter by brand name'),
      limit: z.number().optional().describe('Results per page (default 20)'),
      offset: z.number().optional().describe('Pagination offset (default 0)'),
      companyId: z.string().optional(),
    },
  },
  async (args) => call('list_products', args)
);

server.registerTool(
  'get_product',
  {
    description: 'Get full product details with all variants and inventory items.',
    inputSchema: { productId: z.string().describe('Product UUID'), companyId: z.string().optional() },
  },
  async (args) => call('get_product', args)
);

server.registerTool(
  'create_product',
  {
    description: `Create a new product with variants and inventory under an existing purchase order.

FLOW:
1. Call create_purchase_order first to get a poId
2. Use that poId for all products in the same order
3. After creating each product, ask: "Do you want to add more products to PO <number>?"

MANDATORY FIELDS:
- name, poId, category, variants are REQUIRED
- If category is missing, do NOT proceed — ask the user
- If brand/category/subcategory name is not found in DB, do NOT auto-create — ask the user: "Brand/Category '<name>' doesn't exist. Do you want me to create it?"

SMART DEFAULTS (use past memory):
- If the user typically provides qty for items, pre-fill qty and confirm: "I'll add qty X based on your usual pattern — let me know if you want different"
- If the user usually sets a specific deliveryType, suggest it
- For missing optional fields (description, discount, dprice), suggest sensible defaults based on past products`,
    inputSchema: {
      name: z.string().describe('Product name'),
      poId: z.string().describe('Purchase order UUID — every product must belong to a PO'),
      brand: z.string().optional().describe('Brand name (looked up by name, must exist — use create_brand if not)'),
      description: z.string().optional(),
      category: z.string().describe('Category name (REQUIRED — looked up by name, must exist — use create_category if not)'),
      subcategory: z.string().optional().describe('Subcategory name (looked up by name under the category)'),
      deliveryType: z.string().optional().describe('Default: trynbuy'),
      variants: z.array(z.object({
        name: z.string(),
        code: z.string().optional(),
        sprice: z.number().describe('Selling price'),
        pprice: z.number().optional().describe('Purchase price'),
        dprice: z.number().optional().describe('Discounted price'),
        discount: z.number().optional(),
        items: z.array(z.object({
          size: z.string().nullable().optional(),
          qty: z.number(),
        })).optional().describe('Inventory items with size and qty — always confirm qty with user'),
      })).describe('Variants to create'),
      companyId: z.string().optional(),
    },
  },
  async (args) => call('create_product', args)
);

server.registerTool(
  'update_product',
  {
    description: 'Update a product\'s name, brand, description, category, or subcategory.',
    inputSchema: {
      productId: z.string().describe('Product UUID'),
      name: z.string().optional(),
      brand: z.string().optional(),
      description: z.string().optional(),
      categoryId: z.string().optional(),
      subcategoryId: z.string().optional(),
      companyId: z.string().optional(),
    },
  },
  async (args) => call('update_product', args)
);

server.registerTool(
  'update_variant',
  {
    description: "Update a variant's fields and/or upsert inventory items. Items are matched by size — existing sizes get qty updated, new sizes are added.",
    inputSchema: {
      variantId: z.string().describe('Variant UUID'),
      name: z.string().optional(),
      code: z.string().optional(),
      sprice: z.number().optional().describe('Selling price'),
      pprice: z.number().optional().describe('Purchase price'),
      dprice: z.number().optional().describe('Discounted price'),
      discount: z.number().optional(),
      deliveryType: z.string().optional(),
      status: z.boolean().optional(),
      items: z.array(z.object({
        size: z.string().nullable().optional().describe('Size label (S, M, L, XL, Free, 30, 32, etc.)'),
        qty: z.number().describe('Quantity to set'),
      })).optional().describe('Items to upsert by size'),
      companyId: z.string().optional(),
    },
  },
  async (args) => call('update_variant', args)
);

server.registerTool(
  'delete_items',
  {
    description: 'Delete specific inventory items from a variant by size or item ID.',
    inputSchema: {
      variantId: z.string().describe('Variant UUID'),
      sizes: z.array(z.string().nullable()).optional().describe('Sizes to delete'),
      itemIds: z.array(z.string()).optional().describe('Item UUIDs to delete'),
      companyId: z.string().optional(),
    },
  },
  async (args) => call('delete_items', args)
);

server.registerTool(
  'get_stock_summary',
  {
    description: 'Get aggregated stock grouped by category, brand, or distributor.',
    inputSchema: {
      groupBy: z.enum(['category', 'brand', 'distributor']).describe('How to group the summary'),
      companyId: z.string().optional(),
    },
  },
  async (args) => call('get_stock_summary', args)
);

server.registerTool(
  'search_by_barcode',
  {
    description: 'Find a product variant and item by barcode.',
    inputSchema: { barcode: z.string().describe('Item barcode'), companyId: z.string().optional() },
  },
  async (args) => call('search_by_barcode', args)
);

// ─── Distributor Tools ───────────────────────────────────────────────────────

server.registerTool(
  'list_distributors',
  {
    description: 'List all distributors linked to this company with credit/payment/due summary and opening due.',
    inputSchema: {
      search: z.string().optional().describe('Search by distributor name'),
      companyId: z.string().optional(),
    },
  },
  async (args) => call('list_distributors', args)
);

server.registerTool(
  'get_distributor',
  {
    description: 'Get full distributor details: address, bank info, payment/credit summary, opening due, and total due.',
    inputSchema: {
      distributorId: z.string().describe('Distributor UUID'),
      companyId: z.string().optional(),
    },
  },
  async (args) => call('get_distributor', args)
);

server.registerTool(
  'create_distributor',
  {
    description: 'Create a new distributor with optional address, bank details, and opening due. Creates both the distributor record and the company link.',
    inputSchema: {
      name: z.string().describe('Distributor/supplier name'),
      gstin: z.string().optional(),
      accHolderName: z.string().optional().describe('Bank account holder name'),
      ifsc: z.string().optional(),
      accountNo: z.string().optional(),
      bankName: z.string().optional(),
      upiId: z.string().optional(),
      street: z.string().optional(),
      locality: z.string().optional(),
      city: z.string().optional(),
      state: z.string().optional(),
      pincode: z.string().optional(),
      openingDue: z.number().optional().describe('Opening due amount. Positive = company owes distributor, negative = distributor owes company'),
      openingDueDate: z.string().optional().describe('Opening due date (ISO string)'),
      companyId: z.string().optional(),
    },
  },
  async (args) => call('create_distributor', args)
);

server.registerTool(
  'update_distributor',
  {
    description: "Update a distributor's details, bank info, address, or opening due.",
    inputSchema: {
      distributorId: z.string().describe('Distributor UUID'),
      name: z.string().optional(),
      gstin: z.string().optional(),
      accHolderName: z.string().optional(),
      ifsc: z.string().optional(),
      accountNo: z.string().optional(),
      bankName: z.string().optional(),
      upiId: z.string().optional(),
      status: z.boolean().optional(),
      street: z.string().optional(),
      locality: z.string().optional(),
      city: z.string().optional(),
      state: z.string().optional(),
      pincode: z.string().optional(),
      openingDue: z.number().optional().describe('Opening due amount. Positive = company owes distributor, negative = distributor owes company'),
      openingDueDate: z.string().optional().describe('Opening due date (ISO string)'),
      companyId: z.string().optional(),
    },
  },
  async (args) => call('update_distributor', args)
);

server.registerTool(
  'delete_distributor',
  {
    description: 'Remove a distributor from this company. Fails if POs, credits, or payments are linked.',
    inputSchema: {
      distributorId: z.string().describe('Distributor UUID'),
      companyId: z.string().optional(),
    },
  },
  async (args) => call('delete_distributor', args)
);

server.registerTool(
  'create_distributor_payment',
  {
    description: 'Record a payment made to a distributor. Reduces the amount due.',
    inputSchema: {
      distributorId: z.string().describe('Distributor UUID'),
      amount: z.number().describe('Payment amount'),
      paymentType: z.enum(['CASH', 'CREDIT', 'CARD', 'UPI', 'BANK', 'CHEQUE', 'RETURN']).optional().describe('Default: CASH'),
      remarks: z.string().optional(),
      billNo: z.string().optional(),
      date: z.string().optional().describe('Payment date (ISO string). Defaults to now'),
      purchaseOrderId: z.string().optional().describe('Link payment to a specific PO'),
      companyId: z.string().optional(),
    },
  },
  async (args) => call('create_distributor_payment', args)
);

server.registerTool(
  'create_distributor_credit',
  {
    description: 'Record a credit (amount owed) to a distributor. Increases the amount due.',
    inputSchema: {
      distributorId: z.string().describe('Distributor UUID'),
      amount: z.number().describe('Credit amount'),
      remarks: z.string().optional(),
      billNo: z.string().optional(),
      date: z.string().optional().describe('Credit date (ISO string). Defaults to now'),
      purchaseOrderId: z.string().optional().describe('Link credit to a specific PO'),
      companyId: z.string().optional(),
    },
  },
  async (args) => call('create_distributor_credit', args)
);

// ─── Purchase Order Tools ─────────────────────────────────────────────────────

server.registerTool(
  'list_purchase_orders',
  {
    description: 'List purchase orders with distributor info and product count.',
    inputSchema: {
      distributorId: z.string().optional().describe('Filter by distributor UUID'),
      limit: z.number().optional(),
      offset: z.number().optional(),
      companyId: z.string().optional(),
    },
  },
  async (args) => call('list_purchase_orders', args)
);

server.registerTool(
  'get_purchase_order',
  {
    description: 'Get full purchase order details including all products.',
    inputSchema: { purchaseOrderId: z.string().describe('Purchase order UUID'), companyId: z.string().optional() },
  },
  async (args) => call('get_purchase_order', args)
);

server.registerTool(
  'create_purchase_order',
  {
    description: 'Create a new purchase order. MUST be called before create_product — every product must belong to a PO. Returns a purchaseOrderId to use for all products in this order.',
    inputSchema: {
      distributorId: z.string().optional(),
      billNo: z.string().optional(),
      paymentType: z.enum(['CASH', 'ONLINE', 'CREDIT']).optional(),
      totalAmount: z.number().optional(),
      subTotalAmount: z.number().optional(),
      discount: z.number().optional(),
      tax: z.number().optional(),
      adjustment: z.number().optional(),
      companyId: z.string().optional(),
    },
  },
  async (args) => call('create_purchase_order', args)
);

server.registerTool(
  'update_purchase_order',
  {
    description: 'Update purchase order fields like amounts, bill number, or payment type.',
    inputSchema: {
      purchaseOrderId: z.string().describe('Purchase order UUID'),
      distributorId: z.string().optional(),
      billNo: z.string().optional(),
      paymentType: z.enum(['CASH', 'ONLINE', 'CREDIT']).optional(),
      totalAmount: z.number().optional(),
      subTotalAmount: z.number().optional(),
      discount: z.number().optional(),
      tax: z.number().optional(),
      adjustment: z.number().optional(),
      companyId: z.string().optional(),
    },
  },
  async (args) => call('update_purchase_order', args)
);

// ─── Catalog Tools (Brands, Categories, Subcategories) ──────────────────────

server.registerTool(
  'list_brands',
  {
    description: 'List all brands for this company with product count and total stock qty.',
    inputSchema: {
      search: z.string().optional().describe('Search by brand name'),
      status: z.boolean().optional().describe('Filter by active/inactive'),
      limit: z.number().optional(),
      offset: z.number().optional(),
      companyId: z.string().optional(),
    },
  },
  async (args) => call('list_brands', args)
);

server.registerTool(
  'create_brand',
  {
    description: 'Create a new brand. Returns error if brand already exists (with its ID).',
    inputSchema: {
      name: z.string().describe('Brand name'),
      description: z.string().optional(),
      image: z.string().optional(),
      companyId: z.string().optional(),
    },
  },
  async (args) => call('create_brand', args)
);

server.registerTool(
  'update_brand',
  {
    description: "Update a brand's name, description, image, or status.",
    inputSchema: {
      brandId: z.string().describe('Brand UUID'),
      name: z.string().optional(),
      description: z.string().optional(),
      image: z.string().optional(),
      status: z.boolean().optional(),
      companyId: z.string().optional(),
    },
  },
  async (args) => call('update_brand', args)
);

server.registerTool(
  'delete_brand',
  {
    description: 'Delete a brand. Fails if products are still linked.',
    inputSchema: {
      brandId: z.string().describe('Brand UUID'),
      companyId: z.string().optional(),
    },
  },
  async (args) => call('delete_brand', args)
);

server.registerTool(
  'list_categories',
  {
    description: 'List all categories with product count, total qty, and subcategory count.',
    inputSchema: {
      search: z.string().optional().describe('Search by category name'),
      status: z.boolean().optional(),
      limit: z.number().optional(),
      offset: z.number().optional(),
      companyId: z.string().optional(),
    },
  },
  async (args) => call('list_categories', args)
);

server.registerTool(
  'create_category',
  {
    description: 'Create a new category. Returns error if it already exists. Optionally set tax config, HSN, margin, target audience.',
    inputSchema: {
      name: z.string().describe('Category name'),
      description: z.string().optional(),
      image: z.string().optional(),
      hsn: z.string().optional().describe('HSN code'),
      taxType: z.enum(['FIXED', 'VARIABLE']).optional().describe('Default: FIXED'),
      fixedTax: z.number().optional(),
      thresholdAmount: z.number().optional(),
      taxBelowThreshold: z.number().optional(),
      taxAboveThreshold: z.number().optional(),
      margin: z.number().optional(),
      targetAudience: z.string().optional().describe('e.g. Men, Women, Kids'),
      companyId: z.string().optional(),
    },
  },
  async (args) => call('create_category', args)
);

server.registerTool(
  'update_category',
  {
    description: "Update a category's name, description, tax config, HSN, margin, status, or target audience.",
    inputSchema: {
      categoryId: z.string().describe('Category UUID'),
      name: z.string().optional(),
      description: z.string().optional(),
      image: z.string().optional(),
      status: z.boolean().optional(),
      hsn: z.string().optional(),
      taxType: z.enum(['FIXED', 'VARIABLE']).optional(),
      fixedTax: z.number().optional(),
      thresholdAmount: z.number().optional(),
      taxBelowThreshold: z.number().optional(),
      taxAboveThreshold: z.number().optional(),
      margin: z.number().optional(),
      targetAudience: z.string().optional(),
      companyId: z.string().optional(),
    },
  },
  async (args) => call('update_category', args)
);

server.registerTool(
  'delete_category',
  {
    description: 'Delete a category. Fails if products or subcategories are linked.',
    inputSchema: {
      categoryId: z.string().describe('Category UUID'),
      companyId: z.string().optional(),
    },
  },
  async (args) => call('delete_category', args)
);

server.registerTool(
  'list_subcategories',
  {
    description: 'List subcategories, optionally filtered by parent category.',
    inputSchema: {
      categoryId: z.string().optional().describe('Filter by parent category UUID'),
      search: z.string().optional(),
      status: z.boolean().optional(),
      limit: z.number().optional(),
      offset: z.number().optional(),
      companyId: z.string().optional(),
    },
  },
  async (args) => call('list_subcategories', args)
);

server.registerTool(
  'create_subcategory',
  {
    description: 'Create a new subcategory under a category. Category can be a name or UUID.',
    inputSchema: {
      name: z.string().describe('Subcategory name'),
      category: z.string().describe('Parent category name or UUID'),
      description: z.string().optional(),
      image: z.string().optional(),
      companyId: z.string().optional(),
    },
  },
  async (args) => call('create_subcategory', args)
);

server.registerTool(
  'update_subcategory',
  {
    description: "Update a subcategory's name, description, image, status, or parent category.",
    inputSchema: {
      subcategoryId: z.string().describe('Subcategory UUID'),
      name: z.string().optional(),
      description: z.string().optional(),
      image: z.string().optional(),
      status: z.boolean().optional(),
      categoryId: z.string().optional().describe('Move to different category (UUID)'),
      companyId: z.string().optional(),
    },
  },
  async (args) => call('update_subcategory', args)
);

server.registerTool(
  'delete_subcategory',
  {
    description: 'Delete a subcategory. Fails if products are linked.',
    inputSchema: {
      subcategoryId: z.string().describe('Subcategory UUID'),
      companyId: z.string().optional(),
    },
  },
  async (args) => call('delete_subcategory', args)
);

// ─── Expense Tools ───────────────────────────────────────────────────────────

server.registerTool(
  'list_expenses',
  {
    description: 'List expenses with optional filters (date range, category, user, status, payment mode). Returns paginated list.',
    inputSchema: {
      search: z.string().optional().describe('Search by note content'),
      categoryId: z.string().optional().describe('Filter by expense category UUID'),
      userId: z.string().optional().describe('Filter by user UUID'),
      status: z.string().optional().describe('Filter by status (Paid, Pending, Approved, Rejected)'),
      paymentMode: z.string().optional().describe('Filter by payment mode (CASH, BANK, UPI, CARD, CHEQUE)'),
      fromDate: z.string().optional().describe('Filter expenses from this date (ISO string)'),
      toDate: z.string().optional().describe('Filter expenses up to this date (ISO string)'),
      limit: z.number().optional(),
      offset: z.number().optional(),
      companyId: z.string().optional(),
    },
  },
  async (args) => call('list_expenses', args)
);

server.registerTool(
  'create_expense',
  {
    description: `Create a new expense record.

REQUIRED: category (name or UUID), amount
OPTIONAL: date, userId (employee), paymentMode (CASH/BANK/UPI/CARD/CHEQUE), status (Paid/Pending/Approved/Rejected), note

If the category name doesn't exist, ask the user: "Expense category '<name>' doesn't exist. Should I create it?" — then use create_expense_category.`,
    inputSchema: {
      category: z.string().describe('Expense category name or UUID (REQUIRED)'),
      amount: z.number().describe('Total expense amount (REQUIRED)'),
      date: z.string().optional().describe('Expense date (ISO string). Defaults to now'),
      userId: z.string().optional().describe('User UUID who incurred the expense'),
      paymentMode: z.string().optional().describe('CASH, BANK, UPI, CARD, CHEQUE (default: CASH)'),
      status: z.string().optional().describe('Paid, Pending, Approved, Rejected (default: Paid)'),
      note: z.string().optional().describe('Optional note/description'),
      companyId: z.string().optional(),
    },
  },
  async (args) => call('create_expense', args)
);

server.registerTool(
  'update_expense',
  {
    description: "Update an expense's category, amount, date, payment mode, status, note, or assigned user.",
    inputSchema: {
      expenseId: z.string().describe('Expense UUID'),
      category: z.string().optional().describe('Expense category name or UUID'),
      amount: z.number().optional().describe('Total expense amount'),
      date: z.string().optional().describe('Expense date (ISO string)'),
      userId: z.string().optional().describe('User UUID'),
      paymentMode: z.string().optional().describe('CASH, BANK, UPI, CARD, CHEQUE'),
      status: z.string().optional().describe('Paid, Pending, Approved, Rejected'),
      note: z.string().optional().describe('Note/description'),
      companyId: z.string().optional(),
    },
  },
  async (args) => call('update_expense', args)
);

server.registerTool(
  'delete_expense',
  {
    description: 'Delete an expense by ID.',
    inputSchema: {
      expenseId: z.string().describe('Expense UUID'),
      companyId: z.string().optional(),
    },
  },
  async (args) => call('delete_expense', args)
);

server.registerTool(
  'list_expense_categories',
  {
    description: 'List all expense categories with expense count and total amount.',
    inputSchema: {
      search: z.string().optional().describe('Search by category name'),
      companyId: z.string().optional(),
    },
  },
  async (args) => call('list_expense_categories', args)
);

server.registerTool(
  'create_expense_category',
  {
    description: 'Create a new expense category. Returns error if it already exists (with its ID).',
    inputSchema: {
      name: z.string().describe('Category name'),
      companyId: z.string().optional(),
    },
  },
  async (args) => call('create_expense_category', args)
);

// ─── Finance Tools (Investments, Money Transactions) ─────────────────────────

server.registerTool(
  'list_investments',
  {
    description: 'List investments with optional filters (direction, status, userId, paymentMode, date range). Returns paginated list with user info.',
    inputSchema: {
      direction: z.enum(['IN', 'OUT']).optional().describe('IN = invested, OUT = withdrawn'),
      status: z.enum(['COMPLETED', 'PENDING']).optional(),
      userId: z.string().optional().describe('Filter by user UUID'),
      paymentMode: z.enum(['CASH', 'BANK', 'UPI', 'CARD', 'CHEQUE']).optional(),
      fromDate: z.string().optional().describe('Filter from this date (ISO string)'),
      toDate: z.string().optional().describe('Filter up to this date (ISO string)'),
      limit: z.number().optional(),
      offset: z.number().optional(),
      companyId: z.string().optional(),
    },
  },
  async (args) => call('list_investments', args)
);

server.registerTool(
  'create_investment',
  {
    description: `Create an investment record (capital IN or drawings OUT).

REQUIRED: userId (employee/owner UUID), direction (IN or OUT), amount
OPTIONAL: paymentMode (default CASH), status (default COMPLETED), note, date`,
    inputSchema: {
      userId: z.string().describe('User UUID who invested/withdrew (REQUIRED)'),
      direction: z.enum(['IN', 'OUT']).describe('IN = capital invested, OUT = withdrawn'),
      amount: z.number().describe('Investment amount'),
      paymentMode: z.enum(['CASH', 'BANK', 'UPI', 'CARD', 'CHEQUE']).optional().describe('Default: CASH'),
      status: z.enum(['COMPLETED', 'PENDING']).optional().describe('Default: COMPLETED'),
      note: z.string().optional(),
      date: z.string().optional().describe('ISO date string. Defaults to now'),
      companyId: z.string().optional(),
    },
  },
  async (args) => call('create_investment', args)
);

server.registerTool(
  'update_investment',
  {
    description: "Update an investment's direction, amount, paymentMode, status, note, date, or user.",
    inputSchema: {
      investmentId: z.string().describe('Investment UUID'),
      userId: z.string().optional(),
      direction: z.enum(['IN', 'OUT']).optional(),
      amount: z.number().optional(),
      paymentMode: z.enum(['CASH', 'BANK', 'UPI', 'CARD', 'CHEQUE']).optional(),
      status: z.enum(['COMPLETED', 'PENDING']).optional(),
      note: z.string().optional(),
      date: z.string().optional(),
      companyId: z.string().optional(),
    },
  },
  async (args) => call('update_investment', args)
);

server.registerTool(
  'delete_investment',
  {
    description: 'Delete an investment by ID.',
    inputSchema: {
      investmentId: z.string().describe('Investment UUID'),
      companyId: z.string().optional(),
    },
  },
  async (args) => call('delete_investment', args)
);

server.registerTool(
  'list_money_transactions',
  {
    description: `List money transactions — these are external cash/bank exchanges WITH a party (customer, supplier, employee, owner, etc.).
Use this for: "money given to supplier", "received payment from customer", "paid salary to employee".
Do NOT use for internal fund movements between your own accounts — use account transfers for that.`,
    inputSchema: {
      partyType: z.enum(['CUSTOMER', 'SUPPLIER', 'EMPLOYEE', 'OWNER', 'OTHER']).optional(),
      direction: z.enum(['GIVEN', 'RECEIVED']).optional(),
      status: z.enum(['PENDING', 'PAID']).optional(),
      paymentMode: z.enum(['CASH', 'BANK', 'UPI', 'CARD', 'CHEQUE']).optional(),
      fromDate: z.string().optional().describe('Filter from this date (ISO string)'),
      toDate: z.string().optional().describe('Filter up to this date (ISO string)'),
      limit: z.number().optional(),
      offset: z.number().optional(),
      companyId: z.string().optional(),
    },
  },
  async (args) => call('list_money_transactions', args)
);

server.registerTool(
  'create_money_transaction',
  {
    description: `Create a money transaction — an external cash/bank exchange WITH a party.
Use this when the user says: "gave ₹5000 to supplier", "received ₹2000 from customer", "paid employee salary".
Do NOT use for moving money between your own CASH/BANK/INVESTMENT accounts — use create_account_transfer for that.

REQUIRED: partyType (CUSTOMER/SUPPLIER/EMPLOYEE/OWNER/OTHER), direction (GIVEN/RECEIVED), amount
OPTIONAL: paymentMode (default CASH), status (default PENDING), accountId (bank UUID when paymentMode=BANK), note, date`,
    inputSchema: {
      partyType: z.enum(['CUSTOMER', 'SUPPLIER', 'EMPLOYEE', 'OWNER', 'OTHER']).describe('Who the transaction is with'),
      direction: z.enum(['GIVEN', 'RECEIVED']).describe('GIVEN = paid out, RECEIVED = collected'),
      amount: z.number().describe('Transaction amount'),
      paymentMode: z.enum(['CASH', 'BANK', 'UPI', 'CARD', 'CHEQUE']).optional().describe('Default: CASH'),
      status: z.enum(['PENDING', 'PAID']).optional().describe('Default: PENDING'),
      accountId: z.string().optional().describe('Bank account UUID (only when paymentMode=BANK)'),
      note: z.string().optional(),
      date: z.string().optional().describe('ISO date string. Defaults to now'),
      companyId: z.string().optional(),
    },
  },
  async (args) => call('create_money_transaction', args)
);

server.registerTool(
  'update_money_transaction',
  {
    description: "Update a money transaction (external exchange with a party). Can change partyType, direction, status, amount, paymentMode, accountId, or note.",
    inputSchema: {
      transactionId: z.string().describe('Transaction UUID'),
      partyType: z.enum(['CUSTOMER', 'SUPPLIER', 'EMPLOYEE', 'OWNER', 'OTHER']).optional(),
      direction: z.enum(['GIVEN', 'RECEIVED']).optional(),
      status: z.enum(['PENDING', 'PAID']).optional(),
      amount: z.number().optional(),
      paymentMode: z.enum(['CASH', 'BANK', 'UPI', 'CARD', 'CHEQUE']).optional(),
      accountId: z.string().optional().describe('Bank account UUID (null to clear)'),
      note: z.string().optional(),
      date: z.string().optional(),
      companyId: z.string().optional(),
    },
  },
  async (args) => call('update_money_transaction', args)
);

server.registerTool(
  'delete_money_transaction',
  {
    description: 'Delete a money transaction (external exchange with a party) by ID.',
    inputSchema: {
      transactionId: z.string().describe('Transaction UUID'),
      companyId: z.string().optional(),
    },
  },
  async (args) => call('delete_money_transaction', args)
);

// ─── Account Tools (Bank Accounts, Transfers, Ledgers) ───────────────────────

server.registerTool(
  'list_bank_accounts',
  {
    description: 'List all bank accounts — returns primary bank (from company record) and secondary banks, each labeled with type.',
    inputSchema: {
      companyId: z.string().optional(),
    },
  },
  async (args) => call('list_bank_accounts', args)
);

server.registerTool(
  'create_bank_account',
  {
    description: 'Create a secondary bank account.',
    inputSchema: {
      bankName: z.string().optional().describe('Bank name'),
      accHolderName: z.string().optional().describe('Account holder name'),
      accountNo: z.string().optional().describe('Account number'),
      ifsc: z.string().optional().describe('IFSC code'),
      gstin: z.string().optional().describe('GST number'),
      upiId: z.string().optional().describe('UPI ID'),
      openingBalance: z.number().optional().describe('Opening balance (default 0)'),
      companyId: z.string().optional(),
    },
  },
  async (args) => call('create_bank_account', args)
);

server.registerTool(
  'update_bank_account',
  {
    description: "Update a secondary bank account's details.",
    inputSchema: {
      bankAccountId: z.string().describe('Bank account UUID'),
      bankName: z.string().optional(),
      accHolderName: z.string().optional(),
      accountNo: z.string().optional(),
      ifsc: z.string().optional(),
      gstin: z.string().optional(),
      upiId: z.string().optional(),
      openingBalance: z.number().optional(),
      companyId: z.string().optional(),
    },
  },
  async (args) => call('update_bank_account', args)
);

server.registerTool(
  'delete_bank_account',
  {
    description: 'Delete a secondary bank account. Fails if transactions or transfers reference it.',
    inputSchema: {
      bankAccountId: z.string().describe('Bank account UUID'),
      companyId: z.string().optional(),
    },
  },
  async (args) => call('delete_bank_account', args)
);

server.registerTool(
  'list_account_transfers',
  {
    description: `List account transfers — these are internal fund movements between your own CASH, BANK, and INVESTMENT accounts.
Use this for: "transferred ₹10000 from cash to bank", "moved funds from bank to investment".
Do NOT use for external exchanges with a party (customer, supplier, etc.) — use money transactions for that.`,
    inputSchema: {
      fromType: z.enum(['CASH', 'BANK', 'INVESTMENT']).optional().describe('Filter by source type'),
      toType: z.enum(['CASH', 'BANK', 'INVESTMENT']).optional().describe('Filter by destination type'),
      fromDate: z.string().optional().describe('Filter from this date (ISO string)'),
      toDate: z.string().optional().describe('Filter up to this date (ISO string)'),
      limit: z.number().optional(),
      offset: z.number().optional(),
      companyId: z.string().optional(),
    },
  },
  async (args) => call('list_account_transfers', args)
);

server.registerTool(
  'create_account_transfer',
  {
    description: `Create an account transfer — an internal fund movement between your own CASH, BANK, and INVESTMENT accounts.
Use this when the user says: "transfer ₹10000 from cash to bank", "move funds to investment", "deposit cash in bank".
Do NOT use for external exchanges with a party — use create_money_transaction for that.

REQUIRED: fromType (CASH/BANK/INVESTMENT), toType, amount
OPTIONAL: fromAccountId (bank UUID for secondary bank, null = primary), toAccountId (same), note, date`,
    inputSchema: {
      fromType: z.enum(['CASH', 'BANK', 'INVESTMENT']).describe('Source account type'),
      toType: z.enum(['CASH', 'BANK', 'INVESTMENT']).describe('Destination account type'),
      amount: z.number().describe('Transfer amount'),
      fromAccountId: z.string().optional().describe('Source bank UUID (null = primary bank)'),
      toAccountId: z.string().optional().describe('Destination bank UUID (null = primary bank)'),
      note: z.string().optional(),
      date: z.string().optional().describe('ISO date string. Defaults to now'),
      companyId: z.string().optional(),
    },
  },
  async (args) => call('create_account_transfer', args)
);

server.registerTool(
  'update_account_transfer',
  {
    description: "Update an account transfer (internal fund movement). Can change fromType, toType, account IDs, amount, or note.",
    inputSchema: {
      transferId: z.string().describe('Transfer UUID'),
      fromType: z.enum(['CASH', 'BANK', 'INVESTMENT']).optional(),
      toType: z.enum(['CASH', 'BANK', 'INVESTMENT']).optional(),
      amount: z.number().optional(),
      fromAccountId: z.string().optional().describe('Source bank UUID (null = primary)'),
      toAccountId: z.string().optional().describe('Destination bank UUID (null = primary)'),
      note: z.string().optional(),
      date: z.string().optional(),
      companyId: z.string().optional(),
    },
  },
  async (args) => call('update_account_transfer', args)
);

server.registerTool(
  'delete_account_transfer',
  {
    description: 'Delete an account transfer (internal fund movement) by ID.',
    inputSchema: {
      transferId: z.string().describe('Transfer UUID'),
      companyId: z.string().optional(),
    },
  },
  async (args) => call('delete_account_transfer', args)
);

server.registerTool(
  'get_cash_ledger',
  {
    description: 'Get the cash account ledger with running balance. Shows sales, expenses, transactions, and transfers affecting cash. Returns opening balance, ledger rows, and closing balance.',
    inputSchema: {
      fromDate: z.string().optional().describe('Start date (ISO string). Defaults to all time'),
      toDate: z.string().optional().describe('End date (ISO string). Defaults to now'),
      companyId: z.string().optional(),
    },
  },
  async (args) => call('get_cash_ledger', args)
);

server.registerTool(
  'get_primary_bank_ledger',
  {
    description: 'Get the primary bank ledger with running balance. Shows sales (UPI/Card), expenses, distributor payments, transactions, and transfers affecting the primary bank.',
    inputSchema: {
      fromDate: z.string().optional().describe('Start date (ISO string). Defaults to all time'),
      toDate: z.string().optional().describe('End date (ISO string). Defaults to now'),
      companyId: z.string().optional(),
    },
  },
  async (args) => call('get_primary_bank_ledger', args)
);

server.registerTool(
  'get_secondary_bank_ledger',
  {
    description: 'Get a secondary bank account ledger. Shows money transactions and transfers for that specific bank. Requires bankId.',
    inputSchema: {
      bankId: z.string().describe('Secondary bank account UUID'),
      fromDate: z.string().optional().describe('Start date (ISO string). Defaults to all time'),
      toDate: z.string().optional().describe('End date (ISO string). Defaults to now'),
      companyId: z.string().optional(),
    },
  },
  async (args) => call('get_secondary_bank_ledger', args)
);

// ─── Memory Tools ────────────────────────────────────────────────────────────

server.registerTool(
  'save_memory',
  {
    description: `Save a memory/preference/instruction from the user for future conversations.
Call this when the user says things like "always do X", "remember that I prefer Y",
"next time do Z", "don't do X without asking", or any instruction about how they want things done.
Store a concise summary, not the raw quote. Max 25 memories per user.`,
    inputSchema: {
      content: z.string().describe('Concise memory text, e.g. "Prefers CASH payment mode for all expenses"'),
      category: z.enum(['preference', 'instruction', 'workflow', 'correction']).optional().describe('Memory category'),
      companyId: z.string().optional(),
      userId: z.string().optional(),
    },
  },
  async (args) => call('save_memory', args)
);

server.registerTool(
  'list_memories',
  {
    description: 'List all saved memories/preferences for the current user. Use to check existing memories before saving duplicates.',
    inputSchema: {
      companyId: z.string().optional(),
      userId: z.string().optional(),
    },
  },
  async (args) => call('list_memories', args)
);

server.registerTool(
  'delete_memory',
  {
    description: 'Delete a saved memory by ID. Use when the user says to forget something or a memory is outdated.',
    inputSchema: {
      memoryId: z.string().describe('Memory UUID to delete'),
      companyId: z.string().optional(),
      userId: z.string().optional(),
    },
  },
  async (args) => call('delete_memory', args)
);

// ─── Statement Processing Tools ──────────────────────────────────────────────

server.registerTool(
  'save_statement_rows',
  {
    description: 'Save extracted bank statement rows to DB. Creates a batch + all rows. Returns batchId for the processing page link.',
    inputSchema: {
      rows: z.array(z.object({
        sno: z.number().describe('Serial/row number'),
        date: z.string().describe('Transaction date as shown in statement'),
        description: z.string().describe('Narration/remarks from statement'),
        debit: z.number().optional().describe('Withdrawal amount'),
        credit: z.number().optional().describe('Deposit amount'),
        balance: z.number().optional().describe('Running balance'),
      })).describe('Transaction rows extracted from the statement'),
      sourceFileName: z.string().optional().describe('Original file name'),
      chatId: z.string().optional().describe('AI chat ID for posting done message after execution'),
      companyId: z.string().optional(),
      userId: z.string().optional(),
    },
  },
  async (args) => call('save_statement_rows', args)
);

server.registerTool(
  'find_statement_mappings',
  {
    description: 'Look up known remark→operation mappings for a statement batch. Auto-assigns operations to rows that match previously saved mappings.',
    inputSchema: {
      batchId: z.string().describe('Statement batch UUID'),
      companyId: z.string().optional(),
    },
  },
  async (args) => call('find_statement_mappings', args)
);

// ─── Direct DB Query Tool ─────────────────────────────────────────────────────

import { pool, COMPANY_ID } from './db';

const BLOCKED_PATTERNS = /\b(DROP|TRUNCATE|ALTER|CREATE|GRANT|REVOKE|DELETE\s+FROM|INSERT\s+INTO|UPDATE\s+\w+\s+SET)\b/i;

server.registerTool(
  'query_db',
  {
    description: `Run a read-only SQL query against the database.

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
- distributor_payments (id, distributor_id, company_id, purchase_order_id, amount, payment_type, created_at)`,
    inputSchema: {
      sql: z.string().describe('SQL query. Use $1 for companyId. Example: SELECT * FROM products WHERE company_id = $1 LIMIT 10'),
      params: z.array(z.union([z.string(), z.number(), z.boolean()])).optional().describe('Extra params starting from $2'),
      companyId: z.string().optional(),
    },
  },
  async (args) => {
    try {
      const companyId = args.companyId || COMPANY_ID;
      const sql = args.sql.trim();

      // Block dangerous statements
      if (BLOCKED_PATTERNS.test(sql)) {
        return { content: [{ type: 'text' as const, text: JSON.stringify({ error: 'Only SELECT queries are allowed. No INSERT, UPDATE, DELETE, DROP, ALTER, or TRUNCATE.' }) }], isError: true };
      }

      // Must start with SELECT or WITH
      if (!/^\s*(SELECT|WITH)\b/i.test(sql)) {
        return { content: [{ type: 'text' as const, text: JSON.stringify({ error: 'Query must start with SELECT or WITH.' }) }], isError: true };
      }

      // Build params: $1 = companyId, $2+ = user params
      const queryParams: unknown[] = [companyId, ...(args.params ?? [])];

      // Enforce row limit
      const limited = /\bLIMIT\s+\d+/i.test(sql) ? sql : `${sql} LIMIT 100`;

      const { rows, rowCount } = await pool.query(limited, queryParams);
      return { content: [{ type: 'text' as const, text: JSON.stringify({ rows, rowCount }, null, 2) }] };
    } catch (err: any) {
      return { content: [{ type: 'text' as const, text: `Error: ${err.message}` }], isError: true };
    }
  }
);

// ─── Report Tools ───────────────────────────────────────────────────────────

server.registerTool(
  'generate_report',
  {
    description: reportTools[0].description,
    inputSchema: {
      type: z.string().describe('Report type').pipe(z.enum(['sales', 'profit', 'expense', 'stock', 'gst1', 'distributor'])),
      format: z.string().describe('Output format').pipe(z.enum(['pdf', 'excel'])),
      startDate: z.string().optional().describe('Start date (ISO string). Defaults to start of current month.'),
      endDate: z.string().optional().describe('End date (ISO string). Defaults to now.'),
      companyId: z.string().optional(),
      cleanup: z.boolean().optional().describe('If true, include bills with precedence=true. Default false.'),
      isTaxIncluded: z.boolean().optional().describe('Whether prices include tax. Default true. Only affects GST report.'),
    },
  },
  async (args) => call('generate_report', args)
);

// Prevent uncaught errors from killing the stdio pipe
process.on('uncaughtException', (err) => {
  process.stderr.write(`Uncaught exception (non-fatal): ${err.message}\n`);
});
process.on('unhandledRejection', (reason) => {
  process.stderr.write(`Unhandled rejection (non-fatal): ${reason}\n`);
});

async function main() {
  const transport = new StdioServerTransport();
  await server.connect(transport);
  process.stderr.write('Markit MCP server running on stdio\n');
}

main().catch((err) => {
  process.stderr.write(`Fatal: ${err.message}\n`);
  process.exit(1);
});
