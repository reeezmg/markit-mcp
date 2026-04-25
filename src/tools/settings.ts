import crypto from 'crypto';
import { pool, COMPANY_ID } from '../db';

const cid = (args: { companyId?: string }) => args.companyId ?? COMPANY_ID;

// Helper: build dynamic UPDATE SET clauses from a field map
function buildSets(fieldMap: Record<string, any>, params: any[]): string[] {
  const sets: string[] = [];
  for (const [col, val] of Object.entries(fieldMap)) {
    if (val !== undefined) {
      params.push(val);
      sets.push(`"${col}" = $${params.length}`);
    }
  }
  return sets;
}

export const settingsTools = [
  // ─── Get Settings ────────────────────────────────────────────────────────────

  {
    name: 'get_settings',
    description: `Get all current company settings in one call. Returns store identity, policies, billing config, delivery config, business hours, bank details, opening balance, feature toggles, address, product inputs, and variant inputs.`,
    inputSchema: {
      type: 'object' as const,
      properties: {
        companyId: { type: 'string' },
      },
    },
    handler: async (args: { companyId?: string }) => {
      const companyId = cid(args);

      const { rows: companyRows } = await pool.query(
        `SELECT
           c.name, c.store_unique_name, c.phone, c.logo, c.category, c.description,
           c.thank_you_note, c.refund_policy, c.return_policy,
           c.is_tax_included, c.is_cost_included, c.points_value,
           c.delivery_type, c.delivery_mode, c.fund_delivery_fees,
           c.delivery_radius, c.delivery_fees_per_km, c.waiting_time,
           c.waiting_charges_per_min, c.min_delivery_charges,
           c.delivery_discount_threshold, c.delivery_discount_amount,
           c.open_time, c.close_time,
           c.acc_holder_name, c.ifsc, c.account_no, c.bank_name, c.upi_id, c.gstin,
           c.cash, c.bank, c.opening_cash_date, c.opening_bank_date,
           c.is_ai_image, c.is_usertrack_included,
           c.printer_label_size
         FROM companies c
         WHERE c.id = $1`,
        [companyId]
      );
      if (!companyRows.length) return { error: 'Company not found' };

      const c = companyRows[0];

      const { rows: addrRows } = await pool.query(
        `SELECT name, street, landmark, city, state, pincode,
                formatted_address, place_id, lat, lng
         FROM addresses WHERE company_id = $1`,
        [companyId]
      );

      const { rows: piRows } = await pool.query(
        `SELECT name, brand, category, subcategory, description
         FROM product_inputs WHERE company_id = $1`,
        [companyId]
      );

      const { rows: viRows } = await pool.query(
        `SELECT name, code, sprice, pprice, dprice, discount, qty, sizes, images, button
         FROM variant_inputs WHERE company_id = $1`,
        [companyId]
      );

      return {
        storeIdentity: {
          name: c.name,
          storeUniqueName: c.store_unique_name,
          phone: c.phone,
          logo: c.logo,
          category: c.category,
        },
        policies: {
          description: c.description,
          thankYouNote: c.thank_you_note,
          refundPolicy: c.refund_policy,
          returnPolicy: c.return_policy,
        },
        billing: {
          isTaxIncluded: c.is_tax_included,
          isCostIncluded: c.is_cost_included,
          pointsValue: c.points_value,
        },
        delivery: {
          deliveryType: c.delivery_type,
          deliveryMode: c.delivery_mode,
          fundDeliveryFees: c.fund_delivery_fees,
          deliveryRadius: c.delivery_radius,
          deliveryFeesPerKm: c.delivery_fees_per_km,
          waitingTime: c.waiting_time,
          waitingChargesPerMin: c.waiting_charges_per_min,
          minDeliveryCharges: c.min_delivery_charges,
          deliveryDiscountThreshold: c.delivery_discount_threshold,
          deliveryDiscountAmount: c.delivery_discount_amount,
        },
        businessHours: {
          openTime: c.open_time,
          closeTime: c.close_time,
        },
        bankDetails: {
          accHolderName: c.acc_holder_name,
          ifsc: c.ifsc,
          accountNo: c.account_no,
          bankName: c.bank_name,
          upiId: c.upi_id,
          gstin: c.gstin,
        },
        openingBalance: {
          cash: c.cash,
          bank: c.bank,
          openingCashDate: c.opening_cash_date,
          openingBankDate: c.opening_bank_date,
        },
        featureToggles: {
          isAiImage: c.is_ai_image,
          isUserTrackIncluded: c.is_usertrack_included,
        },
        printerSettings: {
          printerLabelSize: c.printer_label_size,
        },
        address: addrRows[0] ?? null,
        productInputs: piRows[0] ?? null,
        variantInputs: viRows[0] ?? null,
      };
    },
  },

  // ─── Update Store Identity ───────────────────────────────────────────────────

  {
    name: 'update_store_identity',
    description: 'Update store identity fields: unique name (URL slug), phone number, logo key, and categories.',
    inputSchema: {
      type: 'object' as const,
      properties: {
        storeUniqueName: { type: 'string', description: 'Unique store URL slug' },
        phone: { type: 'string', description: 'Store phone number' },
        logo: { type: 'string', description: 'Logo image key (S3 UUID)' },
        category: { type: 'array', items: { type: 'string' }, description: 'Store categories (e.g. ["Men","Women"])' },
        companyId: { type: 'string' },
      },
    },
    handler: async (args: { storeUniqueName?: string; phone?: string; logo?: string; category?: string[]; companyId?: string }) => {
      const companyId = cid(args);
      const params: any[] = [companyId];
      const sets = buildSets({
        store_unique_name: args.storeUniqueName,
        phone: args.phone,
        logo: args.logo,
        category: args.category,
      }, params);

      if (!sets.length) return { error: 'No fields provided to update' };

      sets.push(`updated_at = now()`);
      await pool.query(`UPDATE companies SET ${sets.join(', ')} WHERE id = $1`, params);
      return { success: true, updated: { storeUniqueName: args.storeUniqueName, phone: args.phone, logo: args.logo, category: args.category } };
    },
  },

  // ─── Update Store Policies ───────────────────────────────────────────────────

  {
    name: 'update_store_policies',
    description: 'Update store description, thank you note, refund policy, and return policy.',
    inputSchema: {
      type: 'object' as const,
      properties: {
        description: { type: 'string', description: 'Store description (max 40 chars)' },
        thankYouNote: { type: 'string', description: 'Thank you note shown after order completion' },
        refundPolicy: { type: 'string', description: 'Refund policy text' },
        returnPolicy: { type: 'string', description: 'Return policy text' },
        companyId: { type: 'string' },
      },
    },
    handler: async (args: { description?: string; thankYouNote?: string; refundPolicy?: string; returnPolicy?: string; companyId?: string }) => {
      const companyId = cid(args);
      const params: any[] = [companyId];
      const sets = buildSets({
        description: args.description,
        thank_you_note: args.thankYouNote,
        refund_policy: args.refundPolicy,
        return_policy: args.returnPolicy,
      }, params);

      if (!sets.length) return { error: 'No fields provided to update' };

      sets.push(`updated_at = now()`);
      await pool.query(`UPDATE companies SET ${sets.join(', ')} WHERE id = $1`, params);
      return { success: true, updated: { description: args.description, thankYouNote: args.thankYouNote, refundPolicy: args.refundPolicy, returnPolicy: args.returnPolicy } };
    },
  },

  // ─── Update Billing Settings ─────────────────────────────────────────────────

  {
    name: 'update_billing_settings',
    description: 'Update billing configuration: tax inclusion, cost visibility in bills, and loyalty points value.',
    inputSchema: {
      type: 'object' as const,
      properties: {
        isTaxIncluded: { type: 'boolean', description: 'Whether prices include tax' },
        isCostIncluded: { type: 'boolean', description: 'Whether cost is shown in bills' },
        pointsValue: { type: 'number', description: 'Value of 1 loyalty point in currency' },
        companyId: { type: 'string' },
      },
    },
    handler: async (args: { isTaxIncluded?: boolean; isCostIncluded?: boolean; pointsValue?: number; companyId?: string }) => {
      const companyId = cid(args);
      const params: any[] = [companyId];
      const sets = buildSets({
        is_tax_included: args.isTaxIncluded,
        is_cost_included: args.isCostIncluded,
        points_value: args.pointsValue,
      }, params);

      if (!sets.length) return { error: 'No fields provided to update' };

      sets.push(`updated_at = now()`);
      await pool.query(`UPDATE companies SET ${sets.join(', ')} WHERE id = $1`, params);
      return { success: true, updated: { isTaxIncluded: args.isTaxIncluded, isCostIncluded: args.isCostIncluded, pointsValue: args.pointsValue } };
    },
  },

  // ─── Update Delivery Settings ────────────────────────────────────────────────

  {
    name: 'update_delivery_settings',
    description: `Update delivery configuration: delivery types, modes, radius, fees, waiting charges, and discount thresholds.
Delivery types: trynbuy, booking, delivery (array).
Delivery modes: markit, self (array).`,
    inputSchema: {
      type: 'object' as const,
      properties: {
        deliveryType: { type: 'array', items: { type: 'string' }, description: 'Delivery types: trynbuy, booking, delivery' },
        deliveryMode: { type: 'array', items: { type: 'string' }, description: 'Delivery modes: markit, self' },
        fundDeliveryFees: { type: 'boolean', description: 'Whether store pays customer delivery fees (markit mode)' },
        deliveryRadius: { type: 'number', description: 'Delivery radius in km' },
        deliveryFeesPerKm: { type: 'number', description: 'Delivery fee per km (self mode)' },
        waitingTime: { type: 'number', description: 'Free waiting time in minutes (self mode)' },
        waitingChargesPerMin: { type: 'number', description: 'Charge per minute after free waiting (self mode)' },
        minDeliveryCharges: { type: 'number', description: 'Minimum delivery charge (self mode)' },
        deliveryDiscountThreshold: { type: 'number', description: 'Order amount threshold for delivery discount' },
        deliveryDiscountAmount: { type: 'number', description: 'Discount amount per threshold' },
        companyId: { type: 'string' },
      },
    },
    handler: async (args: {
      deliveryType?: string[]; deliveryMode?: string[]; fundDeliveryFees?: boolean;
      deliveryRadius?: number; deliveryFeesPerKm?: number; waitingTime?: number;
      waitingChargesPerMin?: number; minDeliveryCharges?: number;
      deliveryDiscountThreshold?: number; deliveryDiscountAmount?: number;
      companyId?: string;
    }) => {
      const companyId = cid(args);
      const params: any[] = [companyId];
      const sets = buildSets({
        delivery_type: args.deliveryType,
        delivery_mode: args.deliveryMode,
        fund_delivery_fees: args.fundDeliveryFees,
        delivery_radius: args.deliveryRadius,
        delivery_fees_per_km: args.deliveryFeesPerKm,
        waiting_time: args.waitingTime,
        waiting_charges_per_min: args.waitingChargesPerMin,
        min_delivery_charges: args.minDeliveryCharges,
        delivery_discount_threshold: args.deliveryDiscountThreshold,
        delivery_discount_amount: args.deliveryDiscountAmount,
      }, params);

      if (!sets.length) return { error: 'No fields provided to update' };

      sets.push(`updated_at = now()`);
      await pool.query(`UPDATE companies SET ${sets.join(', ')} WHERE id = $1`, params);
      return { success: true, updated: args };
    },
  },

  // ─── Update Business Hours ───────────────────────────────────────────────────

  {
    name: 'update_business_hours',
    description: 'Update store opening and closing times.',
    inputSchema: {
      type: 'object' as const,
      properties: {
        openTime: { type: 'string', description: 'Opening time (e.g. "9:30")' },
        closeTime: { type: 'string', description: 'Closing time (e.g. "21:00")' },
        companyId: { type: 'string' },
      },
    },
    handler: async (args: { openTime?: string; closeTime?: string; companyId?: string }) => {
      const companyId = cid(args);
      const params: any[] = [companyId];
      const sets = buildSets({
        open_time: args.openTime,
        close_time: args.closeTime,
      }, params);

      if (!sets.length) return { error: 'No fields provided to update' };

      sets.push(`updated_at = now()`);
      await pool.query(`UPDATE companies SET ${sets.join(', ')} WHERE id = $1`, params);
      return { success: true, updated: { openTime: args.openTime, closeTime: args.closeTime } };
    },
  },

  // ─── Update Bank Details ─────────────────────────────────────────────────────

  {
    name: 'update_bank_details',
    description: 'Update company bank account details, UPI ID, and GSTIN for payment settlements.',
    inputSchema: {
      type: 'object' as const,
      properties: {
        accHolderName: { type: 'string', description: 'Bank account holder name' },
        ifsc: { type: 'string', description: 'IFSC code' },
        accountNo: { type: 'string', description: 'Bank account number' },
        bankName: { type: 'string', description: 'Bank name' },
        upiId: { type: 'string', description: 'UPI ID' },
        gstin: { type: 'string', description: 'GSTIN number' },
        companyId: { type: 'string' },
      },
    },
    handler: async (args: { accHolderName?: string; ifsc?: string; accountNo?: string; bankName?: string; upiId?: string; gstin?: string; companyId?: string }) => {
      const companyId = cid(args);
      const params: any[] = [companyId];
      const sets = buildSets({
        acc_holder_name: args.accHolderName,
        ifsc: args.ifsc,
        account_no: args.accountNo,
        bank_name: args.bankName,
        upi_id: args.upiId,
        gstin: args.gstin,
      }, params);

      if (!sets.length) return { error: 'No fields provided to update' };

      sets.push(`updated_at = now()`);
      await pool.query(`UPDATE companies SET ${sets.join(', ')} WHERE id = $1`, params);
      return { success: true, updated: { accHolderName: args.accHolderName, ifsc: args.ifsc, accountNo: args.accountNo, bankName: args.bankName, upiId: args.upiId, gstin: args.gstin } };
    },
  },

  // ─── Update Opening Balance ──────────────────────────────────────────────────

  {
    name: 'update_opening_balance',
    description: 'Update store opening balance for cash and bank.',
    inputSchema: {
      type: 'object' as const,
      properties: {
        cash: { type: 'number', description: 'Opening cash balance' },
        bank: { type: 'number', description: 'Opening bank balance' },
        companyId: { type: 'string' },
      },
    },
    handler: async (args: { cash?: number; bank?: number; companyId?: string }) => {
      const companyId = cid(args);
      const params: any[] = [companyId];
      const sets = buildSets({
        cash: args.cash,
        bank: args.bank,
      }, params);

      if (!sets.length) return { error: 'No fields provided to update' };

      sets.push(`updated_at = now()`);
      await pool.query(`UPDATE companies SET ${sets.join(', ')} WHERE id = $1`, params);
      return { success: true, updated: { cash: args.cash, bank: args.bank } };
    },
  },

  // ─── Update Store Address ────────────────────────────────────────────────────

  {
    name: 'update_store_address',
    description: 'Create or update the store address. Uses upsert — if an address exists for this company it updates, otherwise it creates one.',
    inputSchema: {
      type: 'object' as const,
      properties: {
        name: { type: 'string', description: 'Location name' },
        street: { type: 'string', description: 'Street address' },
        landmark: { type: 'string', description: 'Landmark' },
        city: { type: 'string', description: 'City' },
        state: { type: 'string', description: 'State' },
        pincode: { type: 'string', description: 'Pincode' },
        lat: { type: 'number', description: 'Latitude' },
        lng: { type: 'number', description: 'Longitude' },
        placeId: { type: 'string', description: 'Google Place ID' },
        formattedAddress: { type: 'string', description: 'Full formatted address' },
        companyId: { type: 'string' },
      },
    },
    handler: async (args: {
      name?: string; street?: string; landmark?: string; city?: string; state?: string;
      pincode?: string; lat?: number; lng?: number; placeId?: string; formattedAddress?: string;
      companyId?: string;
    }) => {
      const companyId = cid(args);
      const id = crypto.randomUUID();

      // Check if address exists
      const { rows: existing } = await pool.query(
        `SELECT id FROM addresses WHERE company_id = $1`,
        [companyId]
      );

      if (existing.length) {
        // Update existing
        const params: any[] = [companyId];
        const sets = buildSets({
          name: args.name,
          street: args.street,
          landmark: args.landmark,
          city: args.city,
          state: args.state,
          pincode: args.pincode,
          lat: args.lat,
          lng: args.lng,
          place_id: args.placeId,
          formatted_address: args.formattedAddress,
        }, params);

        if (!sets.length) return { error: 'No fields provided to update' };

        sets.push(`updated_at = now()`);
        await pool.query(`UPDATE addresses SET ${sets.join(', ')} WHERE company_id = $1`, params);
        return { success: true, action: 'updated', addressId: existing[0].id };
      } else {
        // Insert new
        await pool.query(
          `INSERT INTO addresses (id, company_id, name, street, landmark, city, state, pincode, lat, lng, place_id, formatted_address, created_at, updated_at)
           VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, now(), now())`,
          [id, companyId, args.name ?? null, args.street ?? null, args.landmark ?? null,
           args.city ?? null, args.state ?? null, args.pincode ?? null,
           args.lat ?? null, args.lng ?? null, args.placeId ?? null, args.formattedAddress ?? null]
        );
        return { success: true, action: 'created', addressId: id };
      }
    },
  },

  // ─── Update Feature Toggles ──────────────────────────────────────────────────

  {
    name: 'update_feature_toggles',
    description: 'Toggle optional store features: AI image generation and user sales tracking.',
    inputSchema: {
      type: 'object' as const,
      properties: {
        isAiImage: { type: 'boolean', description: 'Enable AI image generation' },
        isUserTrackIncluded: { type: 'boolean', description: 'Enable user sales tracking' },
        companyId: { type: 'string' },
      },
    },
    handler: async (args: { isAiImage?: boolean; isUserTrackIncluded?: boolean; companyId?: string }) => {
      const companyId = cid(args);
      const params: any[] = [companyId];
      const sets = buildSets({
        is_ai_image: args.isAiImage,
        is_usertrack_included: args.isUserTrackIncluded,
      }, params);

      if (!sets.length) return { error: 'No fields provided to update' };

      sets.push(`updated_at = now()`);
      await pool.query(`UPDATE companies SET ${sets.join(', ')} WHERE id = $1`, params);
      return { success: true, updated: { isAiImage: args.isAiImage, isUserTrackIncluded: args.isUserTrackIncluded } };
    },
  },

  // ─── Update Product Inputs ───────────────────────────────────────────────────

  {
    name: 'update_product_inputs',
    description: 'Set which product fields are visible when adding products. All fields are booleans.',
    inputSchema: {
      type: 'object' as const,
      properties: {
        name: { type: 'boolean', description: 'Show product name field' },
        brand: { type: 'boolean', description: 'Show brand field' },
        category: { type: 'boolean', description: 'Show category field' },
        subcategory: { type: 'boolean', description: 'Show subcategory field' },
        description: { type: 'boolean', description: 'Show description field' },
        companyId: { type: 'string' },
      },
    },
    handler: async (args: { name?: boolean; brand?: boolean; category?: boolean; subcategory?: boolean; description?: boolean; companyId?: string }) => {
      const companyId = cid(args);
      const params: any[] = [companyId];
      const sets = buildSets({
        name: args.name,
        brand: args.brand,
        category: args.category,
        subcategory: args.subcategory,
        description: args.description,
      }, params);

      if (!sets.length) return { error: 'No fields provided to update' };

      await pool.query(`UPDATE product_inputs SET ${sets.join(', ')} WHERE company_id = $1`, params);
      return { success: true, updated: { name: args.name, brand: args.brand, category: args.category, subcategory: args.subcategory, description: args.description } };
    },
  },

  // ─── Update Variant Inputs ───────────────────────────────────────────────────

  {
    name: 'update_variant_inputs',
    description: 'Set which variant fields are visible when adding variants. All fields are booleans.',
    inputSchema: {
      type: 'object' as const,
      properties: {
        name: { type: 'boolean', description: 'Show variant name field' },
        code: { type: 'boolean', description: 'Show code field' },
        sprice: { type: 'boolean', description: 'Show selling price field' },
        pprice: { type: 'boolean', description: 'Show purchase price field' },
        dprice: { type: 'boolean', description: 'Show discount price field' },
        discount: { type: 'boolean', description: 'Show discount field' },
        qty: { type: 'boolean', description: 'Show quantity field' },
        sizes: { type: 'boolean', description: 'Show sizes field' },
        images: { type: 'boolean', description: 'Show images field' },
        button: { type: 'boolean', description: 'Show button field' },
        companyId: { type: 'string' },
      },
    },
    handler: async (args: {
      name?: boolean; code?: boolean; sprice?: boolean; pprice?: boolean; dprice?: boolean;
      discount?: boolean; qty?: boolean; sizes?: boolean; images?: boolean; button?: boolean;
      companyId?: string;
    }) => {
      const companyId = cid(args);
      const params: any[] = [companyId];
      const sets = buildSets({
        name: args.name,
        code: args.code,
        sprice: args.sprice,
        pprice: args.pprice,
        dprice: args.dprice,
        discount: args.discount,
        qty: args.qty,
        sizes: args.sizes,
        images: args.images,
        button: args.button,
      }, params);

      if (!sets.length) return { error: 'No fields provided to update' };

      await pool.query(`UPDATE variant_inputs SET ${sets.join(', ')} WHERE company_id = $1`, params);
      return { success: true, updated: args };
    },
  },

  // ─── Update Printer Settings ─────────────────────────────────────────────────

  {
    name: 'update_printer_settings',
    description: 'Update printer label size. Options: "50x25mm", "50x38 mm".',
    inputSchema: {
      type: 'object' as const,
      properties: {
        printerLabelSize: { type: 'string', description: 'Label size: "50x25mm" or "50x38 mm"' },
        companyId: { type: 'string' },
      },
      required: ['printerLabelSize'],
    },
    handler: async (args: { printerLabelSize: string; companyId?: string }) => {
      const companyId = cid(args);
      await pool.query(
        `UPDATE companies SET printer_label_size = $2, updated_at = now() WHERE id = $1`,
        [companyId, args.printerLabelSize]
      );
      return { success: true, updated: { printerLabelSize: args.printerLabelSize } };
    },
  },
];
