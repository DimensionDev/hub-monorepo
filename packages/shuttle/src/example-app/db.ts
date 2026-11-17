import { ColumnType, Kysely, Migrator } from "kysely";
import { Logger } from "./log";
import { err, ok, Result } from "neverthrow";
import { HubTables } from "../shuttle";
import * as messages from "./migrations/001_initial_migration";
import * as onchainEvents from "./migrations/003_onchain_events";
import * as blocks from "./migrations/005_blocks";

const createMigrator = async (db: Kysely<HubTables>, dbSchema: string, log: Logger) => {
  const migrator = new Migrator({
    db,
    migrationTableSchema: dbSchema,
    provider: {
      async getMigrations() {
        return {
          "001_initial_migration": messages,
          "003_onchain_events": onchainEvents,
          "005_blocks": { up: (db) => blocks.up(db, dbSchema) },
        };
      },
    },
  });

  return migrator;
};

export const migrateToLatest = async (
  db: Kysely<HubTables>,
  dbSchema: string,
  log: Logger,
): Promise<Result<void, unknown>> => {
  const migrator = await createMigrator(db, dbSchema, log);

  const { error, results } = await migrator.migrateToLatest();

  results?.forEach((it) => {
    if (it.status === "Success") {
      log.info(`Migration "${it.migrationName}" was executed successfully`);
    } else if (it.status === "Error") {
      log.error(`failed to execute migration "${it.migrationName}"`);
    }
  });

  if (error) {
    log.error("Failed to apply all database migrations");
    log.error(error);
    return err(error);
  }

  log.info("Migrations up to date");
  return ok(undefined);
};

export interface AppTables extends HubTables {
  blocks: {
    id: string;
    createdAt: ColumnType<Date, string, string>;
    updatedAt: ColumnType<Date, string, string>;
    timestamp: ColumnType<Date, string, string>;
    deletedAt: ColumnType<Date | null, string | null, string | null>;
    blockerFid: number;
    blockedFid: number;
  };
  shuttleBlockMessageState: {
    fid: number;
    hash: Uint8Array;
    shardIndex: number;
    eventId: number;
    operation: string;
    eventTimestamp: Date;
  };
}

export type AppDb = Kysely<AppTables>;
