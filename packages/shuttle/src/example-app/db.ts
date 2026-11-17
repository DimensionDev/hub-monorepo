import { Kysely, Migrator } from "kysely";
import { Logger } from "./log";
import { err, ok, Result } from "neverthrow";
import { HubTables } from "../shuttle";
import * as messages from "./migrations/001_initial_migration";
import * as onchainEvents from "./migrations/003_onchain_events";

const createMigrator = async (db: Kysely<HubTables>, dbSchema: string, log: Logger) => {
  const migrator = new Migrator({
    db,
    migrationTableSchema: dbSchema,
    provider: {
      async getMigrations() {
        // Business projections are added separately when their handlers are ready.
        return {
          "001_initial_migration": messages,
          "003_onchain_events": onchainEvents,
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

export type AppDb = Kysely<HubTables>;
