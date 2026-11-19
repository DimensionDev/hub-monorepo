/** ABI-encoded SignedKeyRequestMetadata (one dynamic tuple) with a 65-byte signature. */
export function signedKeyMetadata(requestFid: number | bigint): Buffer {
  const word = (n: number | bigint) => Buffer.from(BigInt(n).toString(16).padStart(64, "0"), "hex");
  const signature = Buffer.concat([Buffer.alloc(65, 7), Buffer.alloc(31)]);
  return Buffer.concat([
    word(32), // tuple offset
    word(requestFid),
    Buffer.concat([Buffer.alloc(12), Buffer.alloc(20, 5)]), // requestSigner
    word(128), // signature offset within the tuple
    word(1_900_000_000), // deadline
    word(65),
    signature,
  ]);
}
