import { ColumnType, Kysely, Migrator } from "kysely";
import { Logger } from "./log";
import { err, ok, Result } from "neverthrow";
import { HubTables } from "../shuttle";
import * as messages from "./migrations/001_initial_migration";
import * as onchainEvents from "./migrations/003_onchain_events";
import * as blocks from "./migrations/005_blocks";
import * as projectionFramework from "./migrations/006_projection_framework";
import * as projectionRuns from "./migrations/007_projection_runs";
import * as referenceTables from "./migrations/008_reference_tables";

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
          "006_projection_framework": { up: (db) => projectionFramework.up(db, dbSchema) },
          "007_projection_runs": { up: (db) => projectionRuns.up(db, dbSchema) },
          "008_reference_tables": { up: (db) => referenceTables.up(db, dbSchema) },
        };
      },
    },
  });

  return migrator;
};

/** Names of registered migrations not yet applied (commands that must not migrate check this). */
export const pendingMigrations = async (db: Kysely<HubTables>, dbSchema: string, log: Logger): Promise<string[]> => {
  const migrator = await createMigrator(db, dbSchema, log);
  return (await migrator.getMigrations()).filter((m) => !m.executedAt).map((m) => m.name);
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
  fids: {
    id: string;
    createdAt: ColumnType<Date, string, string>;
    updatedAt: ColumnType<Date, string, string>;
    fid: number;
    custodyAddress: Uint8Array;
    timestamp: ColumnType<Date, string, string>;
    recoveryAddress: Uint8Array | null;
    registeredAt: ColumnType<Date | null, string | null, string | null>;
  };
  shuttleMessageState: {
    fid: number;
    hash: Uint8Array;
    shardIndex: number | null;
    eventId: number | null;
    operation: string;
    eventTimestamp: Date;
  };
  shuttleProjectionIssues: {
    id: ColumnType<string, never, never>;
    projection: string;
    fid: number | null;
    hash: Uint8Array;
    reason: string;
    payload: unknown;
    createdAt: ColumnType<Date, never, never>;
  };
  shuttleProjectionRuns: {
    id: ColumnType<string, never, never>;
    kind: "rebuild" | "backfill";
    projections: string[];
    fids: ColumnType<string[] | null, number[] | null, number[] | null>;
    trigger: "cli" | "dashboard";
    status: "running" | "succeeded" | "failed";
    total: number | null;
    done: ColumnType<number, number | undefined, number>;
    failedCount: ColumnType<number, number | undefined, number>;
    lastFid: ColumnType<string | null, number | null | undefined, number | null>;
    details: ColumnType<unknown, string | null | undefined, string | null>;
    error: string | null;
    startedAt: ColumnType<Date, never, never>;
    updatedAt: ColumnType<Date, never, Date>;
    finishedAt: ColumnType<Date | null, never, Date | null>;
  };
}

export type AppDb = Kysely<AppTables>;
