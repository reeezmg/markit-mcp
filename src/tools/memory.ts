import crypto from 'crypto';
import { pool, COMPANY_ID } from '../db';

const cid = (args: { companyId?: string }) => args.companyId ?? COMPANY_ID;

const MAX_MEMORIES = 25;

export const memoryTools = [
  {
    name: 'save_memory',
    description: `Save a memory/preference/instruction from the user for future conversations.
Call this when the user says things like "always do X", "remember that I prefer Y",
"next time do Z", "don't do X without asking", or any instruction about how they want things done.
Store a concise summary, not the raw quote.
Max ${MAX_MEMORIES} memories per user — oldest is auto-deleted when limit is reached.`,
    inputSchema: {
      type: 'object' as const,
      properties: {
        content: { type: 'string', description: 'Concise memory text, e.g. "Prefers CASH payment mode for all expenses"' },
        category: { type: 'string', enum: ['preference', 'instruction', 'workflow', 'correction'], description: 'Memory category' },
        companyId: { type: 'string' },
        userId: { type: 'string' },
      },
      required: ['content'],
    },
    handler: async (args: { content: string; category?: string; companyId?: string; userId?: string }) => {
      const companyId = cid(args);
      const userId = args.userId;
      if (!userId) return { error: 'userId is required' };

      // Check for duplicate/similar content
      const { rows: existing } = await pool.query(
        `SELECT id, content FROM ai_memories WHERE company_id = $1 AND user_id = $2`,
        [companyId, userId]
      );

      // Enforce cap — delete oldest if at limit
      if (existing.length >= MAX_MEMORIES) {
        await pool.query(
          `DELETE FROM ai_memories WHERE id = (
            SELECT id FROM ai_memories WHERE company_id = $1 AND user_id = $2
            ORDER BY created_at ASC LIMIT 1
          )`,
          [companyId, userId]
        );
      }

      const id = crypto.randomUUID();
      await pool.query(
        `INSERT INTO ai_memories (id, company_id, user_id, content, category, created_at)
         VALUES ($1, $2, $3, $4, $5, NOW())`,
        [id, companyId, userId, args.content, args.category ?? 'preference']
      );

      return { success: true, id, content: args.content, category: args.category ?? 'preference' };
    },
  },

  {
    name: 'list_memories',
    description: 'List all saved memories/preferences for the current user. Use this to check what you already know about the user before saving duplicates.',
    inputSchema: {
      type: 'object' as const,
      properties: {
        companyId: { type: 'string' },
        userId: { type: 'string' },
      },
    },
    handler: async (args: { companyId?: string; userId?: string }) => {
      const companyId = cid(args);
      const userId = args.userId;
      if (!userId) return { error: 'userId is required' };

      const { rows } = await pool.query(
        `SELECT id, content, category, created_at FROM ai_memories
         WHERE company_id = $1 AND user_id = $2
         ORDER BY created_at DESC`,
        [companyId, userId]
      );

      return { memories: rows, total: rows.length };
    },
  },

  {
    name: 'delete_memory',
    description: 'Delete a saved memory by ID. Use when the user says to forget something or when a memory is outdated.',
    inputSchema: {
      type: 'object' as const,
      properties: {
        memoryId: { type: 'string', description: 'Memory UUID to delete' },
        companyId: { type: 'string' },
        userId: { type: 'string' },
      },
      required: ['memoryId'],
    },
    handler: async (args: { memoryId: string; companyId?: string; userId?: string }) => {
      const companyId = cid(args);
      const userId = args.userId;
      if (!userId) return { error: 'userId is required' };

      const { rowCount } = await pool.query(
        `DELETE FROM ai_memories WHERE id = $1 AND company_id = $2 AND user_id = $3`,
        [args.memoryId, companyId, userId]
      );

      return { success: true, deleted: rowCount ?? 0 };
    },
  },
];
