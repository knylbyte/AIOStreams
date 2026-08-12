import { getDb } from '../db.js';
import { sql } from '../sql.js';

export interface SettingRow {
  key: string;
  value: string;
  updated_at: string | Date;
  updated_by: string | null;
  [k: string]: unknown;
}

export interface SettingBatchSet {
  readonly key: string;
  readonly value: unknown;
}

export interface SettingsBatch {
  readonly sets: readonly SettingBatchSet[];
  readonly deletes: readonly string[];
  readonly expectedVersion: number;
  readonly updatedBy?: string;
}

export class SettingsRepository {
  static async getAll(): Promise<SettingRow[]> {
    return getDb().query<SettingRow>(
      sql`SELECT key, value, updated_at, updated_by FROM settings`
    );
  }

  static async getVersion(): Promise<number> {
    const row = await getDb().maybeOne<{ version: number | string }>(
      sql`SELECT version FROM settings_version WHERE id = ${1}`
    );
    return Number(row?.version ?? 0);
  }

  static async set(
    key: string,
    value: unknown,
    updatedBy?: string
  ): Promise<void> {
    const encoded = JSON.stringify(value);
    const db = getDb();
    await db.tx(async (tx) => {
      await tx.exec(
        sql`UPDATE settings_version SET version = version + 1 WHERE id = ${1}`
      );
      if (tx.dialect === 'postgres') {
        await tx.exec(
          sql`INSERT INTO settings (key, value, updated_at, updated_by)
              VALUES (${key}, ${encoded}, CURRENT_TIMESTAMP, ${updatedBy ?? null})
              ON CONFLICT(key) DO UPDATE SET
                value = EXCLUDED.value,
                updated_at = CURRENT_TIMESTAMP,
                updated_by = EXCLUDED.updated_by`
        );
      } else {
        await tx.exec(
          sql`INSERT INTO settings (key, value, updated_at, updated_by)
              VALUES (${key}, ${encoded}, CURRENT_TIMESTAMP, ${updatedBy ?? null})
              ON CONFLICT(key) DO UPDATE SET
                value = excluded.value,
                updated_at = CURRENT_TIMESTAMP,
                updated_by = excluded.updated_by`
        );
      }
    });
  }

  /**
   * Apply a settings mutation in one transaction, guarded by the version of
   * the snapshot from which its candidate was built. Returns `false` without
   * writing when another transaction has already advanced that version.
   */
  static async applyBatch(batch: SettingsBatch): Promise<boolean> {
    if (batch.sets.length === 0 && batch.deletes.length === 0) return true;
    if (
      !Number.isSafeInteger(batch.expectedVersion) ||
      batch.expectedVersion < 0
    ) {
      throw new Error(
        'Settings batch expectedVersion must be a non-negative safe integer'
      );
    }

    const encodedSets = batch.sets.map(({ key, value }) => {
      const encoded = JSON.stringify(value);
      if (encoded === undefined) {
        throw new Error(`Setting ${key} cannot be encoded as JSON`);
      }
      return { key, encoded };
    });
    const db = getDb();

    return db.tx(async (tx) => {
      const versionUpdate = await tx.exec(
        sql`UPDATE settings_version
            SET version = version + 1
            WHERE id = ${1} AND version = ${batch.expectedVersion}`
      );
      if (versionUpdate.rowCount !== 1) return false;

      for (const { key, encoded } of encodedSets) {
        if (tx.dialect === 'postgres') {
          await tx.exec(
            sql`INSERT INTO settings (key, value, updated_at, updated_by)
                VALUES (${key}, ${encoded}, CURRENT_TIMESTAMP, ${batch.updatedBy ?? null})
                ON CONFLICT(key) DO UPDATE SET
                  value = EXCLUDED.value,
                  updated_at = CURRENT_TIMESTAMP,
                  updated_by = EXCLUDED.updated_by`
          );
        } else {
          await tx.exec(
            sql`INSERT INTO settings (key, value, updated_at, updated_by)
                VALUES (${key}, ${encoded}, CURRENT_TIMESTAMP, ${batch.updatedBy ?? null})
                ON CONFLICT(key) DO UPDATE SET
                  value = excluded.value,
                  updated_at = CURRENT_TIMESTAMP,
                  updated_by = excluded.updated_by`
          );
        }
      }
      for (const key of batch.deletes) {
        await tx.exec(sql`DELETE FROM settings WHERE key = ${key}`);
      }
      return true;
    });
  }

  static async delete(key: string): Promise<void> {
    const db = getDb();
    await db.tx(async (tx) => {
      await tx.exec(
        sql`UPDATE settings_version SET version = version + 1 WHERE id = ${1}`
      );
      await tx.exec(sql`DELETE FROM settings WHERE key = ${key}`);
    });
  }
}
