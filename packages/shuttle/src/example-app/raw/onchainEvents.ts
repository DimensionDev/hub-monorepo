import {
  getStorageUnitExpiry,
  HubEvent,
  isIdRegisterOnChainEvent,
  isMergeOnChainHubEvent,
  isSignerMigratedOnChainEvent,
  isSignerOnChainEvent,
  isStorageRentOnChainEvent,
  isTierPurchaseOnChainEvent,
  OnChainEvent,
} from "@dimensiondev/hub-nodejs";
import type { DB, OnChainEventBodyJson } from "../../shuttle";
import { bytesToHex } from "../../utils";
import type { AppDb } from "../db";
import { log } from "../log";

export function onChainEventBody(onChainEvent: OnChainEvent): OnChainEventBodyJson | undefined {
  if (isIdRegisterOnChainEvent(onChainEvent)) {
    return {
      eventType: onChainEvent.idRegisterEventBody.eventType,
      from: bytesToHex(onChainEvent.idRegisterEventBody.from),
      to: bytesToHex(onChainEvent.idRegisterEventBody.to),
      recoveryAddress: bytesToHex(onChainEvent.idRegisterEventBody.recoveryAddress),
    };
  }
  if (isSignerOnChainEvent(onChainEvent)) {
    return {
      eventType: onChainEvent.signerEventBody.eventType,
      key: bytesToHex(onChainEvent.signerEventBody.key),
      keyType: onChainEvent.signerEventBody.keyType,
      metadata: bytesToHex(onChainEvent.signerEventBody.metadata),
      metadataType: onChainEvent.signerEventBody.metadataType,
    };
  }
  if (isStorageRentOnChainEvent(onChainEvent)) {
    return {
      payer: bytesToHex(onChainEvent.storageRentEventBody.payer),
      units: onChainEvent.storageRentEventBody.units,
      expiry: getStorageUnitExpiry(onChainEvent),
    };
  }
  if (isSignerMigratedOnChainEvent(onChainEvent)) {
    return { migratedAt: onChainEvent.signerMigratedEventBody.migratedAt };
  }
  if (isTierPurchaseOnChainEvent(onChainEvent)) {
    // `payer` is bytes on the protobuf; store hex like every other on-chain body.
    return {
      tierType: onChainEvent.tierPurchaseEventBody.tierType,
      forDays: onChainEvent.tierPurchaseEventBody.forDays,
      payer: bytesToHex(onChainEvent.tierPurchaseEventBody.payer),
    };
  }
  return undefined;
}

/**
 * Stores a MERGE_ON_CHAIN_EVENT idempotently, keyed on (chain_id, block_number, log_index).
 * Errors propagate: in Postgres a failed statement aborts the transaction anyway.
 */
export async function storeOnChainEvent(event: HubEvent, txn: DB): Promise<boolean> {
  if (!isMergeOnChainHubEvent(event)) return false;
  return storeOnChainEventRow(event.mergeOnChainEventBody.onChainEvent, txn);
}

/** Stores one on-chain event idempotently. Returns false for event types without a body mapping. */
export async function storeOnChainEventRow(onChainEvent: OnChainEvent, txn: DB): Promise<boolean> {
  const body = onChainEventBody(onChainEvent);
  if (body === undefined) return false;
  const inserted = await (txn as unknown as AppDb)
    .insertInto("onchain_events")
    .values({
      chainId: onChainEvent.chainId,
      fid: onChainEvent.fid,
      blockTimestamp: new Date(onChainEvent.blockTimestamp * 1000),
      blockNumber: onChainEvent.blockNumber,
      logIndex: onChainEvent.logIndex,
      txHash: onChainEvent.transactionHash,
      type: onChainEvent.type,
      body,
    })
    .onConflict((oc) => oc.columns(["chainId", "blockNumber", "logIndex"]).doNothing())
    .returning("id")
    .executeTakeFirst();
  if (inserted) log.info(`Recorded OnchainEvent ${onChainEvent.type} for fid ${onChainEvent.fid}`);
  return true;
}
