/**
 * Local projection dashboard. Loads its own env file before any app module reads `process.env`,
 * so it can point at a remote database (e.g. through an SSH tunnel) without touching `.env`.
 *
 *   pnpm dashboard [--env-file .env.dashboard] [--addr 127.0.0.1:8790]
 */
import { existsSync } from "node:fs";
import { parseArgs } from "node:util";
import dotenv from "dotenv";

const { values } = parseArgs({
  options: {
    "env-file": { type: "string", default: ".env.dashboard" },
    addr: { type: "string", default: "127.0.0.1:8790" },
  },
});

const envFile = values["env-file"] as string;
if (!existsSync(envFile)) {
  console.error(`找不到 env 文件 ${envFile}（用 --env-file 指定，可参考 .env.dashboard.example）`);
  process.exit(1);
}
// The process environment takes precedence over the file.
dotenv.config({ path: envFile });
// env.ts runs `dotenv/config`; point it at the same file so a `.env` in the working directory
// (e.g. production settings) can never fill a connection this file leaves out.
process.env["DOTENV_CONFIG_PATH"] = envFile;
if (!process.env["POSTGRES_URL"]) {
  console.error(`${envFile} 里没有 POSTGRES_URL`);
  process.exit(1);
}

// Imported only after the env file is loaded: env.ts reads process.env at import time.
// (No top-level await: tsx runs this package as CommonJS.)
import("./server")
  .then(async ({ parseLoopbackAddr, startDashboard }) => {
    await startDashboard({ ...parseLoopbackAddr(values.addr as string), envFile });
  })
  .catch((error) => {
    console.error(error instanceof Error ? error.message : error);
    process.exit(1);
  });
