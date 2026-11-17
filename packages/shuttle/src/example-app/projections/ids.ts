import { createHash } from "node:crypto";

const DNS_NAMESPACE = "6ba7b8109dad11d180b400c04fd430c8";
const URL_NAMESPACE = "6ba7b8119dad11d180b400c04fd430c8";

/** RFC 4122 UUID v5 (SHA-1), byte-identical to Go's uuid.NewSHA1. */
export function uuidV5(namespaceHex: string, name: string): string {
  const hash = createHash("sha1").update(Buffer.from(namespaceHex, "hex")).update(name).digest();
  hash[6] = ((hash[6] ?? 0) & 0x0f) | 0x50;
  hash[8] = ((hash[8] ?? 0) & 0x3f) | 0x80;
  const hex = hash.subarray(0, 16).toString("hex");
  return `${hex.slice(0, 8)}-${hex.slice(8, 12)}-${hex.slice(12, 16)}-${hex.slice(16, 20)}-${hex.slice(20)}`;
}

/** blocks ids, matching the Go initializer (DNS namespace). */
export function blockId(fid: number, target: number): string {
  return uuidV5(DNS_NAMESPACE, `farcaster:mainnet:block:${fid}:${target}`);
}

/** Id scheme used by every other Go converter table (URL namespace). */
export function conversionId(table: string, key: string): string {
  return uuidV5(URL_NAMESPACE, `farcaster-db-conversion:${table}:${key}`);
}
