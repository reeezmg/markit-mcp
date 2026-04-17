export interface VariantInput {
  name: string;
  code?: string;
  sprice: number;
  pprice?: number;
  dprice?: number;
  discount?: number;
  items?: { size: string | null; qty: number }[];
}

export interface ProductRow {
  id: string;
  name: string;
  brand?: string;
  category_id?: string;
  subcategory_id?: string;
  purchaseorder_id?: string;
  created_at: Date;
}

export interface VariantRow {
  id: string;
  name: string;
  code?: string;
  s_price: number;
  p_price: number;
  d_price: number;
  discount: number;
  tax: number;
  images: string[];
}

export interface ItemRow {
  id: string;
  size?: string;
  qty: number;
  barcode?: string;
}
