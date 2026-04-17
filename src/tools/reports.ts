import { pool, COMPANY_ID } from '../db';
import { jsPDF } from 'jspdf';
import autoTable from 'jspdf-autotable';
import ExcelJS from 'exceljs';

const cid = (args: { companyId?: string }) => args.companyId ?? COMPANY_ID;
const rs = (n: number) => `Rs ${n.toFixed(2)}`;
const num = (v: unknown) => Number(v || 0);

// IST is UTC+5:30. Gemini may pass dates in any format ("2026-04-17", "2026-04-17T00:00:00Z", etc).
// Always extract the YYYY-MM-DD part and force IST interpretation so the date range
// covers midnight-to-midnight IST, matching how report.get.ts works with frontend dates.

function toISTStartOfDay(dateStr: string): Date {
  const match = dateStr.match(/(\d{4})-(\d{2})-(\d{2})/);
  if (match) {
    // midnight IST = previous day 18:30 UTC
    return new Date(`${match[1]}-${match[2]}-${match[3]}T00:00:00+05:30`);
  }
  return new Date(dateStr);
}

function toISTEndOfDay(dateStr: string): Date {
  const match = dateStr.match(/(\d{4})-(\d{2})-(\d{2})/);
  if (match) {
    // 23:59:59.999 IST
    return new Date(`${match[1]}-${match[2]}-${match[3]}T23:59:59.999+05:30`);
  }
  return new Date(dateStr);
}

function defaultDates(args: { startDate?: string; endDate?: string }) {
  const now = new Date();
  // Default: 1st of current month IST to now
  const y = now.getUTCFullYear();
  const m = String(now.getUTCMonth() + 1).padStart(2, '0');
  const startDate = args.startDate
    ? toISTStartOfDay(args.startDate)
    : new Date(`${y}-${m}-01T00:00:00+05:30`);
  const endDate = args.endDate
    ? toISTEndOfDay(args.endDate)
    : now;
  return { startDate, endDate };
}

// ─── Sales Report ────────────────────────────────────────────────────────────

async function salesData(companyId: string, startDate: Date, endDate: Date, cleanup: boolean) {
  const client = await pool.connect();
  try {
    // Opening balance
    const baseRes = await client.query(
      `SELECT cash, bank, opening_cash_date, opening_bank_date FROM companies WHERE id = $1`,
      [companyId]
    );
    const company = baseRes.rows[0] || {};
    let baseCash = 0, baseBank = 0;
    if (company.cash && company.opening_cash_date && new Date(company.opening_cash_date) <= startDate) baseCash = num(company.cash);
    if (company.bank && company.opening_bank_date && new Date(company.opening_bank_date) <= startDate) baseBank = num(company.bank);

    const isZeroOpening = num(company.cash) === 0 && num(company.bank) === 0;
    let openingCash = 0, openingBank = 0;

    if (!isZeroOpening) {
      // Before-period queries to compute true opening balance (mirrors report.get.ts)
      const [cashSalesBefore, cashExpBefore, cashDistBefore, cashMoneyBefore, cashTransBefore,
             bankSalesBefore, bankExpBefore, bankDistBefore, bankMoneyBefore, bankTransBefore] = await Promise.all([
        // Cash sales before
        client.query(
          `WITH split AS (
            SELECT (elem->>'amount')::numeric AS amount FROM bills b
            JOIN LATERAL jsonb_array_elements(CASE WHEN jsonb_typeof(b.split_payments::jsonb)='array' THEN b.split_payments::jsonb ELSE '[]'::jsonb END) elem ON true
            WHERE b.company_id=$1 AND b.payment_method='Split' AND (elem->>'method')='Cash' AND b.deleted=false
              AND b.payment_status IN ('PAID','PENDING') AND b.is_markit=false AND b.created_at < $2 AND ($3=true OR b.precedence IS NOT TRUE)
          )
          SELECT COALESCE(SUM(CASE WHEN payment_method='Cash' THEN grand_total ELSE 0 END),0) + COALESCE((SELECT SUM(amount) FROM split),0) AS total
          FROM bills WHERE company_id=$1 AND deleted=false AND payment_status IN ('PAID','PENDING') AND is_markit=false AND created_at < $2 AND ($3=true OR precedence IS NOT TRUE)`,
          [companyId, startDate, cleanup]
        ),
        // Cash expenses before
        client.query(`SELECT COALESCE(SUM(total_amount),0) AS total FROM expenses WHERE company_id=$1 AND payment_mode='CASH' AND UPPER(status)='PAID' AND expense_date < $2`, [companyId, startDate]),
        // Cash distributor before
        client.query(`SELECT COALESCE(SUM(amount),0) AS total FROM distributor_payments WHERE company_id=$1 AND payment_type='CASH' AND created_at < $2`, [companyId, startDate]),
        // Cash money before
        client.query(`SELECT COALESCE(SUM(CASE WHEN direction='RECEIVED' THEN amount ELSE -amount END),0) AS net FROM money_transactions WHERE company_id=$1 AND payment_mode='CASH' AND status='PAID' AND created_at < $2`, [companyId, startDate]),
        // Cash transfer before
        client.query(`SELECT COALESCE(SUM(CASE WHEN to_type='CASH' THEN amount ELSE 0 END),0) - COALESCE(SUM(CASE WHEN from_type='CASH' THEN amount ELSE 0 END),0) AS net FROM account_transfers WHERE company_id=$1 AND created_at < $2`, [companyId, startDate]),
        // Bank sales before
        client.query(
          `WITH split AS (
            SELECT (elem->>'amount')::numeric AS amount FROM bills b
            JOIN LATERAL jsonb_array_elements(CASE WHEN jsonb_typeof(b.split_payments::jsonb)='array' THEN b.split_payments::jsonb ELSE '[]'::jsonb END) elem ON true
            WHERE b.company_id=$1 AND b.payment_method='Split' AND (elem->>'method') IN ('UPI','Card') AND b.deleted=false
              AND b.payment_status IN ('PAID','PENDING') AND b.is_markit=false AND b.created_at < $2 AND ($3=true OR b.precedence IS NOT TRUE)
          )
          SELECT COALESCE(SUM(CASE WHEN payment_method IN ('UPI','Card') THEN grand_total ELSE 0 END),0) + COALESCE((SELECT SUM(amount) FROM split),0) AS total
          FROM bills WHERE company_id=$1 AND deleted=false AND payment_status IN ('PAID','PENDING') AND is_markit=false AND created_at < $2 AND ($3=true OR precedence IS NOT TRUE)`,
          [companyId, startDate, cleanup]
        ),
        // Bank expenses before
        client.query(`SELECT COALESCE(SUM(total_amount),0) AS total FROM expenses WHERE company_id=$1 AND payment_mode IN ('UPI','CARD','BANK','CHEQUE') AND UPPER(status)='PAID' AND expense_date < $2`, [companyId, startDate]),
        // Bank distributor before
        client.query(`SELECT COALESCE(SUM(amount),0) AS total FROM distributor_payments WHERE company_id=$1 AND payment_type='BANK' AND created_at < $2`, [companyId, startDate]),
        // Bank money before
        client.query(`SELECT COALESCE(SUM(CASE WHEN direction='RECEIVED' THEN amount ELSE -amount END),0) AS net FROM money_transactions WHERE company_id=$1 AND payment_mode='BANK' AND status='PAID' AND account_id IS NULL AND created_at < $2`, [companyId, startDate]),
        // Bank transfer before
        client.query(`SELECT COALESCE(SUM(CASE WHEN to_type='BANK' AND to_account_id IS NULL THEN amount ELSE 0 END),0) - COALESCE(SUM(CASE WHEN from_type='BANK' AND from_account_id IS NULL THEN amount ELSE 0 END),0) AS net FROM account_transfers WHERE company_id=$1 AND created_at < $2`, [companyId, startDate]),
      ]);

      openingCash = baseCash + num(cashSalesBefore.rows[0].total) - num(cashExpBefore.rows[0].total) - num(cashDistBefore.rows[0].total) + num(cashMoneyBefore.rows[0].net) + num(cashTransBefore.rows[0].net);
      openingBank = baseBank + num(bankSalesBefore.rows[0].total) - num(bankExpBefore.rows[0].total) - num(bankDistBefore.rows[0].total) + num(bankMoneyBefore.rows[0].net) + num(bankTransBefore.rows[0].net);
    }

    // Sales
    const salesRes = await client.query(
      `SELECT
        COALESCE(SUM(CASE WHEN b.payment_method NOT IN ('Split','Credit') THEN b.grand_total ELSE 0 END),0)
        + COALESCE(SUM(CASE WHEN sp.method != 'Credit' THEN sp.amount ELSE 0 END),0) AS total_sales,
        COALESCE(SUM(CASE WHEN b.payment_method = 'Cash' THEN b.grand_total ELSE 0 END),0)
        + COALESCE(SUM(CASE WHEN sp.method = 'Cash' THEN sp.amount ELSE 0 END),0) AS cash,
        COALESCE(SUM(CASE WHEN b.payment_method = 'UPI' THEN b.grand_total ELSE 0 END),0)
        + COALESCE(SUM(CASE WHEN sp.method = 'UPI' THEN sp.amount ELSE 0 END),0) AS upi,
        COALESCE(SUM(CASE WHEN b.payment_method = 'Card' THEN b.grand_total ELSE 0 END),0)
        + COALESCE(SUM(CASE WHEN sp.method = 'Card' THEN sp.amount ELSE 0 END),0) AS card,
        COALESCE(SUM(CASE WHEN b.payment_method = 'Credit' THEN b.grand_total ELSE 0 END),0) AS credit
      FROM bills b
      LEFT JOIN LATERAL (
        SELECT (elem->>'method') AS method, (elem->>'amount')::numeric AS amount
        FROM jsonb_array_elements(CASE WHEN jsonb_typeof(b.split_payments::jsonb) = 'array' THEN b.split_payments::jsonb ELSE '[]'::jsonb END) elem
      ) sp ON b.payment_method = 'Split'
      WHERE b.company_id = $1 AND b.deleted = false AND b.payment_status IN ('PAID','PENDING') AND b.is_markit = false
        AND b.created_at BETWEEN $2 AND $3 AND ($4 = true OR b.precedence IS NOT TRUE)`,
      [companyId, startDate, endDate, cleanup]
    );
    const sales = salesRes.rows[0];

    // Expenses
    const expenseRes = await client.query(
      `SELECT
        SUM(total_amount) AS total_expense,
        SUM(CASE WHEN payment_mode='CASH' THEN total_amount ELSE 0 END) AS cash,
        SUM(CASE WHEN payment_mode='UPI' THEN total_amount ELSE 0 END) AS upi,
        SUM(CASE WHEN payment_mode='CARD' THEN total_amount ELSE 0 END) AS card,
        SUM(CASE WHEN payment_mode='BANK' THEN total_amount ELSE 0 END) AS bank,
        SUM(CASE WHEN payment_mode='CHEQUE' THEN total_amount ELSE 0 END) AS cheque
      FROM expenses WHERE company_id=$1 AND UPPER(status)='PAID' AND expense_date BETWEEN $2 AND $3`,
      [companyId, startDate, endDate]
    );
    const expenses = expenseRes.rows[0];

    // Purchases
    const purchaseRes = await client.query(
      `SELECT
        SUM(amount) AS total_purchase,
        SUM(CASE WHEN payment_type='CASH' THEN amount ELSE 0 END) AS cash,
        SUM(CASE WHEN payment_type='UPI' THEN amount ELSE 0 END) AS upi,
        SUM(CASE WHEN payment_type='CARD' THEN amount ELSE 0 END) AS card,
        SUM(CASE WHEN payment_type='BANK' THEN amount ELSE 0 END) AS bank,
        SUM(CASE WHEN payment_type='CHEQUE' THEN amount ELSE 0 END) AS cheque
      FROM distributor_payments WHERE company_id=$1 AND created_at BETWEEN $2 AND $3`,
      [companyId, startDate, endDate]
    );
    const purchase = purchaseRes.rows[0];

    // Transfers
    const transferRes = await client.query(
      `SELECT
        SUM(CASE WHEN from_type='CASH' THEN amount ELSE 0 END) AS cash_debit,
        SUM(CASE WHEN to_type='CASH' THEN amount ELSE 0 END) AS cash_credit,
        SUM(CASE WHEN from_type!='CASH' AND (from_account_id IS NULL OR from_account_id='') THEN amount ELSE 0 END) AS bank_debit,
        SUM(CASE WHEN to_type!='CASH' AND (to_account_id IS NULL OR to_account_id='') THEN amount ELSE 0 END) AS bank_credit
      FROM account_transfers WHERE company_id=$1 AND created_at BETWEEN $2 AND $3`,
      [companyId, startDate, endDate]
    );
    const transfers = transferRes.rows[0];
    const transferCashNet = num(transfers.cash_credit) - num(transfers.cash_debit);
    const transferBankNet = num(transfers.bank_credit) - num(transfers.bank_debit);

    // Transactions
    const transactionRes = await client.query(
      `SELECT
        SUM(CASE WHEN payment_mode='CASH' AND direction='GIVEN' THEN amount ELSE 0 END) AS cash_debit,
        SUM(CASE WHEN payment_mode='CASH' AND direction='RECEIVED' THEN amount ELSE 0 END) AS cash_credit,
        SUM(CASE WHEN payment_mode!='CASH' AND direction='GIVEN' AND (account_id IS NULL OR account_id='') THEN amount ELSE 0 END) AS bank_debit,
        SUM(CASE WHEN payment_mode!='CASH' AND direction='RECEIVED' AND (account_id IS NULL OR account_id='') THEN amount ELSE 0 END) AS bank_credit
      FROM money_transactions WHERE company_id=$1 AND status='PAID' AND created_at BETWEEN $2 AND $3`,
      [companyId, startDate, endDate]
    );
    const transactions = transactionRes.rows[0];
    const transactionCashNet = num(transactions.cash_credit) - num(transactions.cash_debit);
    const transactionBankNet = num(transactions.bank_credit) - num(transactions.bank_debit);

    // Bills list
    const billsRes = await client.query(
      `SELECT invoice_number AS invoice, created_at AS date,
        COALESCE(subtotal,0) AS subtotal, COALESCE(subtotal,0) - COALESCE(grand_total,0) AS discount,
        grand_total AS total, payment_method AS payment
      FROM bills WHERE company_id = $1 AND deleted = false AND payment_method != 'Credit'
        AND payment_status != 'PENDING' AND created_at BETWEEN $2 AND $3
      ORDER BY created_at DESC`,
      [companyId, startDate, endDate]
    );

    // Expense rows
    const expenseRowsRes = await client.query(
      `SELECT e.expense_date AS date, ec.name AS category, e.payment_mode AS mode, e.note, e.total_amount AS amount
      FROM expenses e JOIN expense_categories ec ON ec.id=e.expense_category_id
      WHERE e.company_id=$1 AND e.expense_date BETWEEN $2 AND $3 ORDER BY e.expense_date DESC`,
      [companyId, startDate, endDate]
    );

    // Closing balance
    const closingCash = openingCash + num(sales.cash) - (num(expenses.cash) + num(purchase.cash)) + transactionCashNet + transferCashNet;
    const closingBank = openingBank + (num(sales.upi) + num(sales.card))
      - (num(expenses.upi) + num(expenses.card) + num(expenses.bank) + num(expenses.cheque) + num(purchase.upi) + num(purchase.card) + num(purchase.bank) + num(purchase.cheque))
      + transactionBankNet + transferBankNet;

    return {
      openingCash, openingBank, closingCash, closingBank, closingTotal: closingCash + closingBank,
      sales, expenses, purchase, transfers, transactions,
      transferCashNet, transferBankNet, transactionCashNet, transactionBankNet,
      bills: billsRes.rows, expenseRows: expenseRowsRes.rows,
    };
  } finally {
    client.release();
  }
}

function salesPdf(data: Awaited<ReturnType<typeof salesData>>, startDate: Date, endDate: Date): Buffer {
  const doc = new jsPDF({ unit: 'mm', format: 'a4' });
  const M = 14;
  let y = M;

  doc.setFont('helvetica', 'bold'); doc.setFontSize(18); doc.text('Store Summary', M, y);
  doc.setFontSize(10); doc.setFont('helvetica', 'normal'); y += 7;
  doc.text(`From: ${startDate.toLocaleDateString()}  To: ${endDate.toLocaleDateString()}`, M, y); y += 5;
  doc.text(`Generated: ${new Date().toLocaleString()}`, M, y); y += 10;

  doc.setFontSize(13); doc.text('Opening & Closing Balance', M, y); y += 4;
  autoTable(doc, { startY: y, head: [['Type', 'Opening', 'Closing']], body: [
    ['Cash', rs(data.openingCash), rs(data.closingCash)],
    ['Bank', rs(data.openingBank), rs(data.closingBank)],
    ['Total', rs(data.openingCash + data.openingBank), rs(data.closingTotal)],
  ], theme: 'grid' });
  y = (doc as any).lastAutoTable.finalY + 8;

  doc.text('Sales Breakdown', M, y); y += 4;
  autoTable(doc, { startY: y, head: [['Type', 'Amount']], body: [
    ['Total Sales', rs(num(data.sales.total_sales))], ['Cash', rs(num(data.sales.cash))],
    ['UPI', rs(num(data.sales.upi))], ['Card', rs(num(data.sales.card))], ['Credit', rs(num(data.sales.credit))],
  ], theme: 'grid' });
  y = (doc as any).lastAutoTable.finalY + 8;

  doc.text('Expense Breakdown', M, y); y += 4;
  autoTable(doc, { startY: y, head: [['Type', 'Amount']], body: [
    ['Total Expense', rs(num(data.expenses.total_expense))], ['Cash', rs(num(data.expenses.cash))],
    ['UPI', rs(num(data.expenses.upi))], ['Card', rs(num(data.expenses.card))],
    ['Bank', rs(num(data.expenses.bank))], ['Cheque', rs(num(data.expenses.cheque))],
  ], theme: 'grid' });
  y = (doc as any).lastAutoTable.finalY + 8;

  doc.text('Distributor Purchase', M, y); y += 4;
  autoTable(doc, { startY: y, head: [['Type', 'Amount']], body: [
    ['Total Purchase', rs(num(data.purchase.total_purchase))], ['Cash', rs(num(data.purchase.cash))],
    ['UPI', rs(num(data.purchase.upi))], ['Card', rs(num(data.purchase.card))],
    ['Bank', rs(num(data.purchase.bank))], ['Cheque', rs(num(data.purchase.cheque))],
  ], theme: 'grid' });
  y = (doc as any).lastAutoTable.finalY + 8;

  doc.text('Account Transfers', M, y); y += 4;
  autoTable(doc, { startY: y, head: [['Account', 'Debit', 'Credit', 'Net']], body: [
    ['Cash', rs(num(data.transfers.cash_debit)), rs(num(data.transfers.cash_credit)), rs(data.transferCashNet)],
    ['Bank', rs(num(data.transfers.bank_debit)), rs(num(data.transfers.bank_credit)), rs(data.transferBankNet)],
  ], theme: 'grid' });
  y = (doc as any).lastAutoTable.finalY + 8;

  doc.text('Money Transactions', M, y); y += 4;
  autoTable(doc, { startY: y, head: [['Account', 'Debit', 'Credit', 'Net']], body: [
    ['Cash', rs(num(data.transactions.cash_debit)), rs(num(data.transactions.cash_credit)), rs(data.transactionCashNet)],
    ['Bank', rs(num(data.transactions.bank_debit)), rs(num(data.transactions.bank_credit)), rs(data.transactionBankNet)],
  ], theme: 'grid' });
  y = (doc as any).lastAutoTable.finalY + 8;

  if (data.bills.length) {
    doc.text('Bills', M, y); y += 4;
    autoTable(doc, { startY: y, head: [['Invoice', 'Date', 'Subtotal', 'Discount', 'Total', 'Payment']],
      body: data.bills.map((b: any) => [b.invoice, new Date(b.date).toLocaleDateString(), rs(num(b.subtotal)), rs(num(b.discount)), rs(num(b.total)), b.payment]),
      theme: 'grid', styles: { fontSize: 8 } });
    y = (doc as any).lastAutoTable.finalY + 8;
  }

  if (data.expenseRows.length) {
    doc.text('Expense Details', M, y); y += 4;
    autoTable(doc, { startY: y, head: [['Date', 'Category', 'Mode', 'Note', 'Amount']],
      body: data.expenseRows.map((e: any) => [new Date(e.date).toLocaleDateString(), e.category, e.mode, e.note || '', rs(num(e.amount))]),
      theme: 'grid', styles: { fontSize: 8 } });
  }

  return Buffer.from(doc.output('arraybuffer'));
}

async function salesExcel(data: Awaited<ReturnType<typeof salesData>>, startDate: Date, endDate: Date): Promise<Buffer> {
  const wb = new ExcelJS.Workbook();
  wb.creator = 'Markit'; wb.created = new Date();
  const cw = 18;

  const s1 = wb.addWorksheet('Summary');
  s1.columns = [{ width: cw }, { width: cw }, { width: cw }];
  s1.addRow(['Type', 'Opening', 'Closing']);
  s1.addRow(['Cash', data.openingCash, data.closingCash]);
  s1.addRow(['Bank', data.openingBank, data.closingBank]);
  s1.addRow(['Total', data.openingCash + data.openingBank, data.closingTotal]);

  const s2 = wb.addWorksheet('Sales');
  s2.columns = [{ width: cw }, { width: cw }];
  s2.addRow(['Type', 'Amount']);
  s2.addRow(['Total Sales', num(data.sales.total_sales)]);
  s2.addRow(['Cash', num(data.sales.cash)]); s2.addRow(['UPI', num(data.sales.upi)]);
  s2.addRow(['Card', num(data.sales.card)]); s2.addRow(['Credit', num(data.sales.credit)]);

  const s3 = wb.addWorksheet('Expenses');
  s3.columns = [{ width: cw }, { width: cw }];
  s3.addRow(['Type', 'Amount']);
  s3.addRow(['Total Expense', num(data.expenses.total_expense)]);
  s3.addRow(['Cash', num(data.expenses.cash)]); s3.addRow(['UPI', num(data.expenses.upi)]);
  s3.addRow(['Card', num(data.expenses.card)]); s3.addRow(['Bank', num(data.expenses.bank)]);
  s3.addRow(['Cheque', num(data.expenses.cheque)]);

  const s4 = wb.addWorksheet('Purchases');
  s4.columns = [{ width: cw }, { width: cw }];
  s4.addRow(['Type', 'Amount']);
  s4.addRow(['Total Purchase', num(data.purchase.total_purchase)]);
  s4.addRow(['Cash', num(data.purchase.cash)]); s4.addRow(['UPI', num(data.purchase.upi)]);
  s4.addRow(['Card', num(data.purchase.card)]); s4.addRow(['Bank', num(data.purchase.bank)]);
  s4.addRow(['Cheque', num(data.purchase.cheque)]);

  const s5 = wb.addWorksheet('Bills');
  s5.columns = [{ width: 12 }, { width: 14 }, { width: cw }, { width: cw }, { width: cw }, { width: 14 }];
  s5.addRow(['Invoice', 'Date', 'Subtotal', 'Discount', 'Total', 'Payment']);
  for (const b of data.bills as any[]) {
    s5.addRow([b.invoice, new Date(b.date).toLocaleDateString(), num(b.subtotal), num(b.discount), num(b.total), b.payment]);
  }

  const s6 = wb.addWorksheet('Expense Details');
  s6.columns = [{ width: 14 }, { width: cw }, { width: 12 }, { width: 24 }, { width: cw }];
  s6.addRow(['Date', 'Category', 'Mode', 'Note', 'Amount']);
  for (const e of data.expenseRows as any[]) {
    s6.addRow([new Date(e.date).toLocaleDateString(), e.category, e.mode, e.note || '', num(e.amount)]);
  }

  const buf = await wb.xlsx.writeBuffer();
  return Buffer.from(buf);
}

// ─── Profit Report ───���──────────────────────────���────────────────────────────

async function profitData(companyId: string, startDate: Date, endDate: Date, cleanup: boolean) {
  const client = await pool.connect();
  try {
    const { rows } = await client.query(
      `WITH entry_calc AS (
        SELECT b.id AS bill_id, b.created_at AS bill_date, b.invoice_number,
          (CASE WHEN b.payment_method NOT IN ('Split','Credit') THEN b.grand_total ELSE 0 END
           + COALESCE(sp.non_credit_amount,0)) AS bill_sales,
          e.id AS entry_id, e.name AS entry_name, e.qty, e.rate, e.value,
          c.name AS category_name,
          COALESCE(v.p_price, e.rate * (1 - (COALESCE(c.margin, 100) / 100.0))) AS cost_price,
          e.return AS is_return
        FROM entries e
        INNER JOIN bills b ON e.bill_id = b.id
        LEFT JOIN LATERAL (
          SELECT SUM(CASE WHEN (elem->>'method') != 'Credit' THEN (elem->>'amount')::numeric ELSE 0 END) AS non_credit_amount
          FROM jsonb_array_elements(CASE WHEN jsonb_typeof(b.split_payments::jsonb)='array' THEN b.split_payments::jsonb ELSE '[]'::jsonb END) elem
        ) sp ON b.payment_method='Split'
        LEFT JOIN variants v ON e.variant_id = v.id
        LEFT JOIN categories c ON e.category_id = c.id
        WHERE b.company_id = $1 AND b.deleted = false AND b.is_markit = false
          AND b.payment_status = 'PAID' AND b.created_at BETWEEN $2 AND $3
          AND ($4 = true OR b.precedence IS NOT TRUE)
      )
      SELECT *, CASE WHEN is_return = true THEN -ABS(cost_price * qty) ELSE (cost_price * qty) END AS entry_cogs,
        CASE WHEN is_return = true THEN -ABS(value - (cost_price * qty)) ELSE (value - (cost_price * qty)) END AS entry_profit
      FROM entry_calc ORDER BY bill_date DESC`,
      [companyId, startDate, endDate, cleanup]
    );

    const expenseRes = await client.query(
      `SELECT COALESCE(SUM(e.total_amount), 0) AS total_expenses
      FROM expenses e JOIN expense_categories ec ON ec.id = e.expense_category_id
      WHERE e.company_id = $1 AND ec.name <> 'Purchase' AND e.expense_date BETWEEN $2 AND $3`,
      [companyId, startDate, endDate]
    );
    const totalExpenses = num(expenseRes.rows[0].total_expenses);

    const billMap = new Map<string, any>();
    const categoryMap = new Map<string, { sales: number; profit: number }>();

    for (const r of rows) {
      if (!billMap.has(r.bill_id)) {
        billMap.set(r.bill_id, { billId: r.bill_id, billDate: r.bill_date, invoiceNumber: r.invoice_number, billSales: num(r.bill_sales), billCOGS: 0, billProfit: 0, marginPercent: 0, entries: [] });
      }
      const bill = billMap.get(r.bill_id);
      const entryValue = num(r.value); const entryCOGS = num(r.entry_cogs); const entryProfit = num(r.entry_profit);
      bill.billCOGS += entryCOGS;
      bill.entries.push({ name: r.entry_name, qty: num(r.qty), rate: num(r.rate), value: entryValue, cogs: entryCOGS, profit: entryProfit, marginPercent: entryValue > 0 ? (entryProfit / entryValue) * 100 : 0 });

      const cat = r.category_name || 'Uncategorized';
      if (!categoryMap.has(cat)) categoryMap.set(cat, { sales: 0, profit: 0 });
      const c = categoryMap.get(cat)!;
      c.sales += entryValue; c.profit += entryProfit;
    }

    let totalSales = 0, totalCOGS = 0;
    const bills = Array.from(billMap.values()).map(b => {
      b.billProfit = b.billSales - b.billCOGS;
      b.marginPercent = b.billSales > 0 ? (b.billProfit / b.billSales) * 100 : 0;
      totalSales += b.billSales; totalCOGS += b.billCOGS;
      return b;
    });
    const totalProfit = totalSales - totalCOGS;

    const categoryProfit = Array.from(categoryMap.entries()).map(([name, v]) => ({
      name, sales: v.sales, profit: v.profit, marginPercent: v.sales > 0 ? (v.profit / v.sales) * 100 : 0,
    }));

    return { totalSales, totalCOGS, totalProfit, totalExpenses, netProfit: totalProfit - totalExpenses, overallMargin: totalSales > 0 ? (totalProfit / totalSales) * 100 : 0, bills, categoryProfit };
  } finally {
    client.release();
  }
}

function profitPdf(data: Awaited<ReturnType<typeof profitData>>, startDate: Date, endDate: Date): Buffer {
  const doc = new jsPDF({ unit: 'mm', format: 'a4' });
  const M = 14; let y = M;

  doc.setFont('helvetica', 'bold'); doc.setFontSize(18); doc.text('Profit Summary Report', M, y);
  doc.setFontSize(10); doc.setFont('helvetica', 'normal'); y += 7;
  doc.text(`From: ${startDate.toLocaleDateString()}  To: ${endDate.toLocaleDateString()}`, M, y); y += 5;
  doc.text(`Generated: ${new Date().toLocaleString()}`, M, y); y += 10;

  doc.setFontSize(13);
  autoTable(doc, { startY: y, head: [['Metric', 'Amount']], body: [
    ['Total Sales', rs(data.totalSales)], ['Total COGS', rs(data.totalCOGS)],
    ['Gross Profit', rs(data.totalProfit)], ['Total Expenses', rs(data.totalExpenses)],
    ['Net Profit', rs(data.netProfit)], ['Margin %', `${data.overallMargin.toFixed(1)}%`],
  ], theme: 'grid' });
  y = (doc as any).lastAutoTable.finalY + 8;

  if (data.categoryProfit.length) {
    doc.text('Category-wise Profit', M, y); y += 4;
    autoTable(doc, { startY: y, head: [['Category', 'Sales', 'Profit', 'Margin %']], body: data.categoryProfit.map(c => [c.name, rs(c.sales), rs(c.profit), `${c.marginPercent.toFixed(1)}%`]), theme: 'grid' });
    y = (doc as any).lastAutoTable.finalY + 8;
  }

  if (data.bills.length) {
    doc.text('Bill-wise Breakdown', M, y); y += 4;
    autoTable(doc, { startY: y, head: [['Invoice', 'Date', 'Sales', 'COGS', 'Profit', 'Margin %']],
      body: data.bills.map(b => [b.invoiceNumber, new Date(b.billDate).toLocaleDateString(), rs(b.billSales), rs(b.billCOGS), rs(b.billProfit), `${b.marginPercent.toFixed(1)}%`]),
      theme: 'grid', styles: { fontSize: 8 } });
  }

  return Buffer.from(doc.output('arraybuffer'));
}

async function profitExcel(data: Awaited<ReturnType<typeof profitData>>, startDate: Date, endDate: Date): Promise<Buffer> {
  const wb = new ExcelJS.Workbook(); wb.creator = 'Markit'; wb.created = new Date();

  const s1 = wb.addWorksheet('Summary');
  s1.columns = [{ width: 20 }, { width: 18 }];
  s1.addRow(['Metric', 'Amount']);
  s1.addRow(['Total Sales', data.totalSales]); s1.addRow(['Total COGS', data.totalCOGS]);
  s1.addRow(['Gross Profit', data.totalProfit]); s1.addRow(['Total Expenses', data.totalExpenses]);
  s1.addRow(['Net Profit', data.netProfit]); s1.addRow(['Margin %', data.overallMargin]);

  const s2 = wb.addWorksheet('Category Profit');
  s2.columns = [{ width: 20 }, { width: 18 }, { width: 18 }, { width: 14 }];
  s2.addRow(['Category', 'Sales', 'Profit', 'Margin %']);
  for (const c of data.categoryProfit) s2.addRow([c.name, c.sales, c.profit, c.marginPercent]);

  const s3 = wb.addWorksheet('Bills');
  s3.columns = [{ width: 12 }, { width: 14 }, { width: 18 }, { width: 18 }, { width: 18 }, { width: 14 }];
  s3.addRow(['Invoice', 'Date', 'Sales', 'COGS', 'Profit', 'Margin %']);
  for (const b of data.bills) s3.addRow([b.invoiceNumber, new Date(b.billDate).toLocaleDateString(), b.billSales, b.billCOGS, b.billProfit, b.marginPercent]);

  return Buffer.from(await wb.xlsx.writeBuffer());
}

// ─── Expense Report ─────────���────────────────────────────────────────────────

async function expenseData(companyId: string, startDate: Date, endDate: Date) {
  const { rows } = await pool.query(
    `SELECT e.expense_date AS date, ec.name AS category, e.payment_mode AS mode, e.note, e.total_amount AS amount, e.status
    FROM expenses e LEFT JOIN expense_categories ec ON ec.id=e.expense_category_id
    WHERE e.company_id=$1 AND e.expense_date BETWEEN $2 AND $3 ORDER BY e.expense_date DESC`,
    [companyId, startDate, endDate]
  );

  const byCategory: Record<string, number> = {};
  const byMode: Record<string, number> = {};
  let total = 0;
  for (const r of rows) {
    const amt = num(r.amount);
    total += amt;
    byCategory[r.category || 'Uncategorized'] = (byCategory[r.category || 'Uncategorized'] || 0) + amt;
    byMode[r.mode || 'Unknown'] = (byMode[r.mode || 'Unknown'] || 0) + amt;
  }
  return { rows, byCategory, byMode, total };
}

function expensePdf(data: Awaited<ReturnType<typeof expenseData>>, startDate: Date, endDate: Date): Buffer {
  const doc = new jsPDF({ unit: 'mm', format: 'a4' }); const M = 14; let y = M;
  doc.setFont('helvetica', 'bold'); doc.setFontSize(18); doc.text('Expense Report', M, y);
  doc.setFontSize(10); doc.setFont('helvetica', 'normal'); y += 7;
  doc.text(`From: ${startDate.toLocaleDateString()}  To: ${endDate.toLocaleDateString()}`, M, y); y += 5;
  doc.text(`Total: ${rs(data.total)}`, M, y); y += 10;

  doc.setFontSize(13); doc.text('By Category', M, y); y += 4;
  autoTable(doc, { startY: y, head: [['Category', 'Amount']], body: Object.entries(data.byCategory).map(([k, v]) => [k, rs(v)]), theme: 'grid' });
  y = (doc as any).lastAutoTable.finalY + 8;

  doc.text('By Payment Mode', M, y); y += 4;
  autoTable(doc, { startY: y, head: [['Mode', 'Amount']], body: Object.entries(data.byMode).map(([k, v]) => [k, rs(v)]), theme: 'grid' });
  y = (doc as any).lastAutoTable.finalY + 8;

  if (data.rows.length) {
    doc.text('Expense Details', M, y); y += 4;
    autoTable(doc, { startY: y, head: [['Date', 'Category', 'Mode', 'Note', 'Amount']],
      body: data.rows.map((r: any) => [new Date(r.date).toLocaleDateString(), r.category || '', r.mode, r.note || '', rs(num(r.amount))]),
      theme: 'grid', styles: { fontSize: 8 } });
  }
  return Buffer.from(doc.output('arraybuffer'));
}

async function expenseExcel(data: Awaited<ReturnType<typeof expenseData>>, startDate: Date, endDate: Date): Promise<Buffer> {
  const wb = new ExcelJS.Workbook(); wb.creator = 'Markit'; wb.created = new Date();

  const s1 = wb.addWorksheet('By Category');
  s1.columns = [{ width: 22 }, { width: 18 }];
  s1.addRow(['Category', 'Amount']);
  for (const [k, v] of Object.entries(data.byCategory)) s1.addRow([k, v]);
  s1.addRow(['Total', data.total]);

  const s2 = wb.addWorksheet('Expense Details');
  s2.columns = [{ width: 14 }, { width: 20 }, { width: 12 }, { width: 28 }, { width: 16 }];
  s2.addRow(['Date', 'Category', 'Mode', 'Note', 'Amount']);
  for (const r of data.rows as any[]) s2.addRow([new Date(r.date).toLocaleDateString(), r.category || '', r.mode, r.note || '', num(r.amount)]);

  return Buffer.from(await wb.xlsx.writeBuffer());
}

// ─── Stock Report ────────────────────────────────────────────────────────────

async function stockData(companyId: string) {
  const { rows } = await pool.query(
    `SELECT p.name AS product, c.name AS category, b.name AS brand,
      v.name AS variant, v.s_price, v.p_price,
      i.size, i.qty, i.initial_qty, i.sold_qty, i.barcode
    FROM items i
    JOIN variants v ON v.id = i.variant_id
    JOIN products p ON p.id = v.product_id
    LEFT JOIN categories c ON c.id = p.category_id
    LEFT JOIN brands b ON b.id = p.brand_id
    WHERE i.company_id = $1 AND v.status = true AND p.status = true
    ORDER BY p.name, v.name, i.size`,
    [companyId]
  );

  let totalQty = 0, totalValue = 0;
  const byCategory: Record<string, { qty: number; value: number }> = {};
  const byBrand: Record<string, { qty: number; value: number }> = {};

  for (const r of rows) {
    const qty = num(r.qty); const pprice = num(r.p_price);
    const value = qty * pprice;
    totalQty += qty; totalValue += value;

    const cat = r.category || 'Uncategorized';
    if (!byCategory[cat]) byCategory[cat] = { qty: 0, value: 0 };
    byCategory[cat].qty += qty; byCategory[cat].value += value;

    const brand = r.brand || 'No Brand';
    if (!byBrand[brand]) byBrand[brand] = { qty: 0, value: 0 };
    byBrand[brand].qty += qty; byBrand[brand].value += value;
  }

  return { rows, totalQty, totalValue, byCategory, byBrand };
}

function stockPdf(data: Awaited<ReturnType<typeof stockData>>): Buffer {
  const doc = new jsPDF({ unit: 'mm', format: 'a4' }); const M = 14; let y = M;
  doc.setFont('helvetica', 'bold'); doc.setFontSize(18); doc.text('Stock Report', M, y);
  doc.setFontSize(10); doc.setFont('helvetica', 'normal'); y += 7;
  doc.text(`Total Qty: ${data.totalQty}  |  Total Value: ${rs(data.totalValue)}`, M, y); y += 5;
  doc.text(`Generated: ${new Date().toLocaleString()}`, M, y); y += 10;

  doc.setFontSize(13); doc.text('By Category', M, y); y += 4;
  autoTable(doc, { startY: y, head: [['Category', 'Qty', 'Value']], body: Object.entries(data.byCategory).map(([k, v]) => [k, v.qty, rs(v.value)]), theme: 'grid' });
  y = (doc as any).lastAutoTable.finalY + 8;

  doc.text('By Brand', M, y); y += 4;
  autoTable(doc, { startY: y, head: [['Brand', 'Qty', 'Value']], body: Object.entries(data.byBrand).map(([k, v]) => [k, v.qty, rs(v.value)]), theme: 'grid' });
  y = (doc as any).lastAutoTable.finalY + 8;

  doc.text('Item Details', M, y); y += 4;
  autoTable(doc, { startY: y, head: [['Product', 'Variant', 'Size', 'Qty', 'P.Price', 'S.Price', 'Value']],
    body: data.rows.map((r: any) => [r.product, r.variant, r.size || '-', num(r.qty), rs(num(r.p_price)), rs(num(r.s_price)), rs(num(r.qty) * num(r.p_price))]),
    theme: 'grid', styles: { fontSize: 7 } });

  return Buffer.from(doc.output('arraybuffer'));
}

async function stockExcel(data: Awaited<ReturnType<typeof stockData>>): Promise<Buffer> {
  const wb = new ExcelJS.Workbook(); wb.creator = 'Markit'; wb.created = new Date();

  const s1 = wb.addWorksheet('Summary');
  s1.columns = [{ width: 22 }, { width: 14 }, { width: 18 }];
  s1.addRow(['Category', 'Qty', 'Value']);
  for (const [k, v] of Object.entries(data.byCategory)) s1.addRow([k, v.qty, v.value]);
  s1.addRow([]); s1.addRow(['Total', data.totalQty, data.totalValue]);

  const s2 = wb.addWorksheet('Items');
  s2.columns = [{ width: 22 }, { width: 16 }, { width: 10 }, { width: 10 }, { width: 14 }, { width: 14 }, { width: 14 }, { width: 18 }];
  s2.addRow(['Product', 'Variant', 'Size', 'Qty', 'P.Price', 'S.Price', 'Value', 'Barcode']);
  for (const r of data.rows as any[]) {
    s2.addRow([r.product, r.variant, r.size || '', num(r.qty), num(r.p_price), num(r.s_price), num(r.qty) * num(r.p_price), r.barcode || '']);
  }

  return Buffer.from(await wb.xlsx.writeBuffer());
}

// ─── GST Report (GSTR-1) ────────────���───────────────────────────────────────

async function gstData(companyId: string, startDate: Date, endDate: Date, cleanup: boolean, isTaxIncluded: boolean) {
  const client = await pool.connect();
  try {
    const [kpiRes, rateRes, hsnRes] = await Promise.all([
      client.query(
        `SELECT COUNT(DISTINCT b.id) AS bill_count,
          CASE WHEN $4 = true THEN COALESCE(SUM(e.value * 100.0 / (100.0 + NULLIF(e.tax, 0))), 0) ELSE COALESCE(SUM(e.value), 0) END AS total_taxable_value,
          CASE WHEN $4 = true THEN COALESCE(SUM(e.value * COALESCE(e.tax, 0) / (100.0 + NULLIF(e.tax, 0))), 0) ELSE COALESCE(SUM(e.value * COALESCE(e.tax, 0) / 100.0), 0) END AS total_tax,
          COALESCE(SUM(b.grand_total), 0) AS total_invoice_value
        FROM entries e JOIN bills b ON b.id = e.bill_id
        WHERE b.company_id = $1 AND b.deleted = false AND b.payment_status IN ('PAID', 'PENDING') AND b.is_markit = false
          AND b.created_at BETWEEN $2 AND $3 AND ($5 = true OR b.precedence IS NOT TRUE)`,
        [companyId, startDate, endDate, isTaxIncluded, cleanup]
      ),
      client.query(
        `SELECT COALESCE(e.tax, 0) AS tax_rate,
          CASE WHEN $4 = true THEN COALESCE(SUM(e.value * 100.0 / (100.0 + NULLIF(e.tax, 0))), 0) ELSE COALESCE(SUM(e.value), 0) END AS taxable_value,
          CASE WHEN $4 = true THEN COALESCE(SUM(e.value * COALESCE(e.tax, 0) / (100.0 + NULLIF(e.tax, 0))), 0) ELSE COALESCE(SUM(e.value * COALESCE(e.tax, 0) / 100.0), 0) END AS total_tax
        FROM entries e JOIN bills b ON b.id = e.bill_id
        WHERE b.company_id = $1 AND b.deleted = false AND b.payment_status IN ('PAID', 'PENDING') AND b.is_markit = false
          AND b.created_at BETWEEN $2 AND $3 AND ($5 = true OR b.precedence IS NOT TRUE)
        GROUP BY COALESCE(e.tax, 0) ORDER BY COALESCE(e.tax, 0)`,
        [companyId, startDate, endDate, isTaxIncluded, cleanup]
      ),
      client.query(
        `SELECT COALESCE(c.hsn, 'N/A') AS hsn_code, COALESCE(c.name, 'Uncategorized') AS category_name,
          COALESCE(e.tax, 0) AS tax_rate, COALESCE(SUM(e.qty), 0) AS total_qty,
          CASE WHEN $4 = true THEN COALESCE(SUM(e.value * 100.0 / (100.0 + NULLIF(e.tax, 0))), 0) ELSE COALESCE(SUM(e.value), 0) END AS taxable_value,
          CASE WHEN $4 = true THEN COALESCE(SUM(e.value * COALESCE(e.tax, 0) / (100.0 + NULLIF(e.tax, 0))), 0) ELSE COALESCE(SUM(e.value * COALESCE(e.tax, 0) / 100.0), 0) END AS total_tax
        FROM entries e JOIN bills b ON b.id = e.bill_id LEFT JOIN categories c ON c.id = e.category_id
        WHERE b.company_id = $1 AND b.deleted = false AND b.payment_status IN ('PAID', 'PENDING') AND b.is_markit = false
          AND b.created_at BETWEEN $2 AND $3 AND ($5 = true OR b.precedence IS NOT TRUE)
        GROUP BY c.hsn, c.name, COALESCE(e.tax, 0) ORDER BY c.name, COALESCE(e.tax, 0)`,
        [companyId, startDate, endDate, isTaxIncluded, cleanup]
      ),
    ]);

    const kpi = kpiRes.rows[0];
    return {
      kpi: { billCount: num(kpi.bill_count), totalTaxableValue: num(kpi.total_taxable_value), totalTax: num(kpi.total_tax), totalInvoiceValue: num(kpi.total_invoice_value) },
      rateSummary: rateRes.rows.map((r: any) => { const tax = num(r.total_tax); return { taxRate: num(r.tax_rate), taxableValue: num(r.taxable_value), cgst: tax / 2, sgst: tax / 2, igst: 0, totalTax: tax }; }),
      hsnSummary: hsnRes.rows.map((r: any) => { const tax = num(r.total_tax); return { hsnCode: r.hsn_code, description: r.category_name, totalQty: num(r.total_qty), taxableValue: num(r.taxable_value), taxRate: num(r.tax_rate), cgst: tax / 2, sgst: tax / 2, totalTax: tax }; }),
    };
  } finally {
    client.release();
  }
}

function gstPdf(data: Awaited<ReturnType<typeof gstData>>, startDate: Date, endDate: Date): Buffer {
  const doc = new jsPDF({ unit: 'mm', format: 'a4' }); const M = 14; let y = M;
  doc.setFont('helvetica', 'bold'); doc.setFontSize(18); doc.text('GSTR-1 Report', M, y);
  doc.setFontSize(10); doc.setFont('helvetica', 'normal'); y += 7;
  doc.text(`From: ${startDate.toLocaleDateString()}  To: ${endDate.toLocaleDateString()}`, M, y); y += 10;

  doc.setFontSize(13); doc.text('Summary', M, y); y += 4;
  autoTable(doc, { startY: y, head: [['Metric', 'Value']], body: [
    ['Bill Count', String(data.kpi.billCount)], ['Taxable Value', rs(data.kpi.totalTaxableValue)],
    ['Total Tax', rs(data.kpi.totalTax)], ['Invoice Value', rs(data.kpi.totalInvoiceValue)],
  ], theme: 'grid' });
  y = (doc as any).lastAutoTable.finalY + 8;

  if (data.rateSummary.length) {
    doc.text('Rate-wise Summary', M, y); y += 4;
    autoTable(doc, { startY: y, head: [['Tax Rate', 'Taxable Value', 'CGST', 'SGST', 'Total Tax']],
      body: data.rateSummary.map(r => [`${r.taxRate}%`, rs(r.taxableValue), rs(r.cgst), rs(r.sgst), rs(r.totalTax)]), theme: 'grid' });
    y = (doc as any).lastAutoTable.finalY + 8;
  }

  if (data.hsnSummary.length) {
    doc.text('HSN Summary', M, y); y += 4;
    autoTable(doc, { startY: y, head: [['HSN', 'Description', 'Qty', 'Taxable', 'Rate', 'CGST', 'SGST', 'Total Tax']],
      body: data.hsnSummary.map(r => [r.hsnCode, r.description, r.totalQty, rs(r.taxableValue), `${r.taxRate}%`, rs(r.cgst), rs(r.sgst), rs(r.totalTax)]),
      theme: 'grid', styles: { fontSize: 8 } });
  }
  return Buffer.from(doc.output('arraybuffer'));
}

async function gstExcel(data: Awaited<ReturnType<typeof gstData>>): Promise<Buffer> {
  const wb = new ExcelJS.Workbook(); wb.creator = 'Markit'; wb.created = new Date();

  const s1 = wb.addWorksheet('Summary');
  s1.columns = [{ width: 22 }, { width: 18 }];
  s1.addRow(['Metric', 'Value']);
  s1.addRow(['Bill Count', data.kpi.billCount]); s1.addRow(['Taxable Value', data.kpi.totalTaxableValue]);
  s1.addRow(['Total Tax', data.kpi.totalTax]); s1.addRow(['Invoice Value', data.kpi.totalInvoiceValue]);

  const s2 = wb.addWorksheet('Rate-wise');
  s2.columns = [{ width: 12 }, { width: 18 }, { width: 14 }, { width: 14 }, { width: 10 }, { width: 14 }];
  s2.addRow(['Tax Rate', 'Taxable Value', 'CGST', 'SGST', 'IGST', 'Total Tax']);
  for (const r of data.rateSummary) s2.addRow([r.taxRate, r.taxableValue, r.cgst, r.sgst, r.igst, r.totalTax]);

  const s3 = wb.addWorksheet('HSN');
  s3.columns = [{ width: 12 }, { width: 22 }, { width: 10 }, { width: 18 }, { width: 10 }, { width: 14 }, { width: 14 }, { width: 14 }];
  s3.addRow(['HSN', 'Description', 'Qty', 'Taxable', 'Rate', 'CGST', 'SGST', 'Total Tax']);
  for (const r of data.hsnSummary) s3.addRow([r.hsnCode, r.description, r.totalQty, r.taxableValue, r.taxRate, r.cgst, r.sgst, r.totalTax]);

  return Buffer.from(await wb.xlsx.writeBuffer());
}

// ─── Distributor Report ─────────��────────────────────────────────────────────

async function distributorData(companyId: string, startDate: Date, endDate: Date) {
  const client = await pool.connect();
  try {
    const { rows } = await client.query(
      `SELECT d.id, d.name,
        COALESCE(dc.opening_due, 0) AS opening_due,
        COALESCE(pay.total_paid, 0) AS total_paid,
        COALESCE(cred.total_credit, 0) AS total_credit,
        COALESCE(po.total_purchase, 0) AS total_purchase,
        COALESCE(po.po_count, 0) AS po_count
      FROM distributors d
      JOIN distributor_companies dc ON dc.distributor_id = d.id AND dc.company_id = $1
      LEFT JOIN LATERAL (
        SELECT SUM(amount) AS total_paid FROM distributor_payments WHERE distributor_id = d.id AND company_id = $1 AND created_at BETWEEN $2 AND $3
      ) pay ON true
      LEFT JOIN LATERAL (
        SELECT SUM(amount) AS total_credit FROM distributor_credits WHERE distributor_id = d.id AND company_id = $1 AND created_at BETWEEN $2 AND $3
      ) cred ON true
      LEFT JOIN LATERAL (
        SELECT SUM(total_amount) AS total_purchase, COUNT(*)::int AS po_count FROM purchase_orders WHERE distributor_id = d.id AND company_id = $1 AND created_at BETWEEN $2 AND $3
      ) po ON true
      ORDER BY d.name`,
      [companyId, startDate, endDate]
    );

    let totalPurchase = 0, totalPaid = 0, totalCredit = 0;
    const distributors = rows.map((r: any) => {
      const purchase = num(r.total_purchase); const paid = num(r.total_paid); const credit = num(r.total_credit);
      const opening = num(r.opening_due);
      const outstanding = opening + purchase - paid - credit;
      totalPurchase += purchase; totalPaid += paid; totalCredit += credit;
      return { name: r.name, openingDue: opening, purchase, paid, credit, outstanding, poCount: num(r.po_count) };
    });
    return { distributors, totalPurchase, totalPaid, totalCredit, totalOutstanding: distributors.reduce((s, d) => s + d.outstanding, 0) };
  } finally {
    client.release();
  }
}

function distributorPdf(data: Awaited<ReturnType<typeof distributorData>>, startDate: Date, endDate: Date): Buffer {
  const doc = new jsPDF({ unit: 'mm', format: 'a4' }); const M = 14; let y = M;
  doc.setFont('helvetica', 'bold'); doc.setFontSize(18); doc.text('Distributor Report', M, y);
  doc.setFontSize(10); doc.setFont('helvetica', 'normal'); y += 7;
  doc.text(`From: ${startDate.toLocaleDateString()}  To: ${endDate.toLocaleDateString()}`, M, y); y += 10;

  doc.setFontSize(13); doc.text('Summary', M, y); y += 4;
  autoTable(doc, { startY: y, head: [['Metric', 'Amount']], body: [
    ['Total Purchase', rs(data.totalPurchase)], ['Total Paid', rs(data.totalPaid)],
    ['Total Credit', rs(data.totalCredit)], ['Total Outstanding', rs(data.totalOutstanding)],
  ], theme: 'grid' });
  y = (doc as any).lastAutoTable.finalY + 8;

  if (data.distributors.length) {
    doc.text('Distributor-wise', M, y); y += 4;
    autoTable(doc, { startY: y, head: [['Distributor', 'Opening', 'Purchase', 'Paid', 'Credit', 'Outstanding', 'POs']],
      body: data.distributors.map(d => [d.name, rs(d.openingDue), rs(d.purchase), rs(d.paid), rs(d.credit), rs(d.outstanding), d.poCount]),
      theme: 'grid', styles: { fontSize: 8 } });
  }
  return Buffer.from(doc.output('arraybuffer'));
}

async function distributorExcel(data: Awaited<ReturnType<typeof distributorData>>): Promise<Buffer> {
  const wb = new ExcelJS.Workbook(); wb.creator = 'Markit'; wb.created = new Date();
  const s1 = wb.addWorksheet('Distributors');
  s1.columns = [{ width: 22 }, { width: 16 }, { width: 16 }, { width: 16 }, { width: 16 }, { width: 16 }, { width: 10 }];
  s1.addRow(['Distributor', 'Opening Due', 'Purchase', 'Paid', 'Credit', 'Outstanding', 'POs']);
  for (const d of data.distributors) s1.addRow([d.name, d.openingDue, d.purchase, d.paid, d.credit, d.outstanding, d.poCount]);
  s1.addRow(['Total', '', data.totalPurchase, data.totalPaid, data.totalCredit, data.totalOutstanding, '']);
  return Buffer.from(await wb.xlsx.writeBuffer());
}

// ─── Main Tool ─────────────────────────────────────���─────────────────────────

export const reportTools = [
  {
    name: 'generate_report',
    description: `Generate a business report as a downloadable PDF or Excel file. Available report types:
- sales: Revenue, payment breakdown, bills, expenses, opening/closing balance
- profit: P&L with COGS, margins per bill and category
- expense: Expenses by category and payment mode
- stock: Inventory levels and value by category/brand
- gst1: GSTR-1 outward supplies, rate-wise and HSN-wise
- distributor: Purchase history, payments, outstanding dues per distributor

Returns a file that the user can download. Always ask the user which report type and format (PDF/Excel) they want before calling this tool.`,
    inputSchema: {
      type: 'object' as const,
      properties: {
        type: { type: 'string', enum: ['sales', 'profit', 'expense', 'stock', 'gst1', 'distributor'], description: 'Report type' },
        format: { type: 'string', enum: ['pdf', 'excel'], description: 'Output format' },
        startDate: { type: 'string', description: 'Start date (ISO string). Defaults to start of current month.' },
        endDate: { type: 'string', description: 'End date (ISO string). Defaults to now.' },
        companyId: { type: 'string' },
        cleanup: { type: 'boolean', description: 'If true, include bills with precedence=true. Default false.' },
        isTaxIncluded: { type: 'boolean', description: 'Whether prices include tax. Default true. Only affects GST report.' },
      },
      required: ['type', 'format'],
    },
    handler: async (args: {
      type: string; format: string; startDate?: string; endDate?: string;
      companyId?: string; cleanup?: boolean; isTaxIncluded?: boolean;
    }) => {
      const companyId = cid(args);
      const { startDate, endDate } = defaultDates(args);
      const cleanup = args.cleanup ?? false;
      const isPdf = args.format === 'pdf';
      const ext = isPdf ? '.pdf' : '.xlsx';
      const mimeType = isPdf ? 'application/pdf' : 'application/vnd.openxmlformats-officedocument.spreadsheetml.sheet';

      let buffer: Buffer;
      let summary: string;

      switch (args.type) {
        case 'sales': {
          const data = await salesData(companyId, startDate, endDate, cleanup);
          buffer = isPdf ? salesPdf(data, startDate, endDate) : await salesExcel(data, startDate, endDate);
          summary = `Sales Report (${startDate.toLocaleDateString()} - ${endDate.toLocaleDateString()}): Total Sales ${rs(num(data.sales.total_sales))}, Expenses ${rs(num(data.expenses.total_expense))}, Closing Cash ${rs(data.closingCash)}, Closing Bank ${rs(data.closingBank)}`;
          break;
        }
        case 'profit': {
          const data = await profitData(companyId, startDate, endDate, cleanup);
          buffer = isPdf ? profitPdf(data, startDate, endDate) : await profitExcel(data, startDate, endDate);
          summary = `Profit Report (${startDate.toLocaleDateString()} - ${endDate.toLocaleDateString()}): Sales ${rs(data.totalSales)}, COGS ${rs(data.totalCOGS)}, Net Profit ${rs(data.netProfit)}, Margin ${data.overallMargin.toFixed(1)}%`;
          break;
        }
        case 'expense': {
          const data = await expenseData(companyId, startDate, endDate);
          buffer = isPdf ? expensePdf(data, startDate, endDate) : await expenseExcel(data, startDate, endDate);
          summary = `Expense Report (${startDate.toLocaleDateString()} - ${endDate.toLocaleDateString()}): Total ${rs(data.total)}, ${data.rows.length} entries across ${Object.keys(data.byCategory).length} categories`;
          break;
        }
        case 'stock': {
          const data = await stockData(companyId);
          buffer = isPdf ? stockPdf(data) : await stockExcel(data);
          summary = `Stock Report: ${data.totalQty} units, Value ${rs(data.totalValue)}, across ${Object.keys(data.byCategory).length} categories`;
          break;
        }
        case 'gst1': {
          const isTaxIncluded = args.isTaxIncluded ?? true;
          const data = await gstData(companyId, startDate, endDate, cleanup, isTaxIncluded);
          buffer = isPdf ? gstPdf(data, startDate, endDate) : await gstExcel(data);
          summary = `GSTR-1 Report (${startDate.toLocaleDateString()} - ${endDate.toLocaleDateString()}): ${data.kpi.billCount} bills, Taxable ${rs(data.kpi.totalTaxableValue)}, Tax ${rs(data.kpi.totalTax)}`;
          break;
        }
        case 'distributor': {
          const data = await distributorData(companyId, startDate, endDate);
          buffer = isPdf ? distributorPdf(data, startDate, endDate) : await distributorExcel(data);
          summary = `Distributor Report (${startDate.toLocaleDateString()} - ${endDate.toLocaleDateString()}): ${data.distributors.length} distributors, Total Purchase ${rs(data.totalPurchase)}, Outstanding ${rs(data.totalOutstanding)}`;
          break;
        }
        default:
          return { error: `Unknown report type: ${args.type}. Available: sales, profit, expense, stock, gst1, distributor` };
      }

      return {
        _reportFile: true,
        base64: buffer.toString('base64'),
        filename: `${args.type}-report${ext}`,
        mimeType,
        summary,
      };
    },
  },
];
