/**
 * Business-table structure copied from the reference database (Neynar replicator, PostgreSQL 17.9),
 * read from its catalogs on 2026-11-17. Do not edit by hand: re-read the reference instead.
 *
 * Index definitions are `pg_indexes.indexdef` with the table schema written as `{schema}`.
 * `blocks` (migration 005) and `parquet_import_tracking` (Neynar import bookkeeping) are not listed.
 */

/** [name, format_type, NOT NULL, default expression] in column order. */
export type ColumnSpec = readonly [name: string, type: string, notNull: boolean, defaultValue: string | null];

export type TableSpec = {
  name: string;
  columns: readonly ColumnSpec[];
  /** Index name to definition; includes the primary key's `<table>_pkey` index. */
  indexes: Readonly<Record<string, string>>;
  /** Target-only columns that may exist on an adopted table (never created here). */
  extraColumns?: readonly ColumnSpec[];
};

const neynarBackfilled: ColumnSpec = ["neynar_backfilled", "boolean", true, "false"];
const extras: Record<string, readonly ColumnSpec[]> = { fids: [neynarBackfilled], signers: [neynarBackfilled] };

export const referenceTables: readonly TableSpec[] = [
  {
    name: "casts",
    columns: [
      ["id", "uuid", true, null],
      ["created_at", "timestamp without time zone", true, null],
      ["updated_at", "timestamp without time zone", true, null],
      ["deleted_at", "timestamp without time zone", false, null],
      ["timestamp", "timestamp without time zone", true, null],
      ["fid", "bigint", true, null],
      ["hash", "bytea", true, null],
      ["text", "text", true, null],
      ["parent_hash", "bytea", false, null],
      ["parent_fid", "bigint", false, null],
      ["parent_url", "text", false, null],
      ["root_parent_hash", "bytea", true, null],
      ["root_parent_url", "text", false, null],
      ["embedded_urls", "text[]", false, null],
      ["embedded_casts", "bytea[]", false, null],
      ["mentions", "bigint[]", false, null],
      ["mentions_positions", "smallint[]", false, null],
      ["ticker_mentions", "text[]", false, null],
      ["ticker_mentions_positions", "smallint[]", false, null],
      ["channel_mentions", "text[]", false, null],
      ["channel_mentions_positions", "smallint[]", false, null],
      ["embedded_casts_fids", "bigint[]", false, null],
      ["embeds", "jsonb", false, null],
      ["creator_app_fid", "bigint", false, null],
      ["deleter_app_fid", "bigint", false, null],
    ],
    indexes: {
      casts_channel_timeline_active_idx:
        'CREATE INDEX casts_channel_timeline_active_idx ON {schema}.casts USING btree (root_parent_url, "timestamp" DESC, id DESC) WHERE (deleted_at IS NULL)',
      casts_embedded_casts_active_gin_idx:
        "CREATE INDEX casts_embedded_casts_active_gin_idx ON {schema}.casts USING gin (embedded_casts) WHERE (deleted_at IS NULL)",
      casts_fid_root_timeline_not_deleted:
        'CREATE INDEX casts_fid_root_timeline_not_deleted ON {schema}.casts USING btree (fid, "timestamp" DESC, id DESC) INCLUDE (hash, root_parent_url) WHERE ((deleted_at IS NULL) AND (parent_hash IS NULL))',
      casts_fid_timeline_not_deleted:
        'CREATE INDEX casts_fid_timeline_not_deleted ON {schema}.casts USING btree (fid, "timestamp" DESC, id DESC) INCLUDE (hash, root_parent_url) WHERE (deleted_at IS NULL)',
      casts_hash: "CREATE INDEX casts_hash ON {schema}.casts USING btree (hash)",
      casts_parent_hash: "CREATE INDEX casts_parent_hash ON {schema}.casts USING btree (parent_hash)",
      casts_parent_hash_deleted:
        "CREATE INDEX casts_parent_hash_deleted ON {schema}.casts USING btree (parent_hash) WHERE (deleted_at IS NULL)",
      casts_parent_timeline_not_deleted:
        'CREATE INDEX casts_parent_timeline_not_deleted ON {schema}.casts USING btree (parent_hash, "timestamp", id) WHERE (deleted_at IS NULL)',
      casts_pkey: "CREATE UNIQUE INDEX casts_pkey ON {schema}.casts USING btree (id)",
      casts_root_parent_hash: "CREATE INDEX casts_root_parent_hash ON {schema}.casts USING btree (root_parent_hash)",
      casts_text_active_trgm_idx:
        "CREATE INDEX casts_text_active_trgm_idx ON {schema}.casts USING gin (text gin_trgm_ops) WHERE (deleted_at IS NULL)",
      idx_casts_timestamp: 'CREATE INDEX idx_casts_timestamp ON {schema}.casts USING btree ("timestamp")',
      idx_casts_upsert: "CREATE INDEX idx_casts_upsert ON {schema}.casts USING btree (id, updated_at)",
    },
  },
  {
    name: "channel_follows",
    columns: [
      ["id", "uuid", true, null],
      ["created_at", "timestamp without time zone", true, null],
      ["updated_at", "timestamp without time zone", true, null],
      ["deleted_at", "timestamp without time zone", false, null],
      ["fid", "bigint", true, null],
      ["channel_id", "text", true, null],
      ["timestamp", "timestamp without time zone", true, null],
    ],
    indexes: {
      channel_follows_channel_id:
        "CREATE INDEX channel_follows_channel_id ON {schema}.channel_follows USING btree (channel_id)",
      channel_follows_fid: "CREATE INDEX channel_follows_fid ON {schema}.channel_follows USING btree (fid)",
      channel_follows_pkey: "CREATE UNIQUE INDEX channel_follows_pkey ON {schema}.channel_follows USING btree (id)",
      channel_follows_timestamp_not_deleted:
        'CREATE INDEX channel_follows_timestamp_not_deleted ON {schema}.channel_follows USING btree ("timestamp") WHERE (deleted_at IS NULL)',
    },
  },
  {
    name: "channel_members",
    columns: [
      ["id", "uuid", true, null],
      ["created_at", "timestamp without time zone", true, null],
      ["updated_at", "timestamp without time zone", true, null],
      ["deleted_at", "timestamp without time zone", false, null],
      ["fid", "bigint", true, null],
      ["channel_id", "text", true, null],
      ["member_at", "timestamp without time zone", true, null],
      ["role", "smallint", true, null],
    ],
    indexes: {
      channel_members_channel_id_deleted_at_fid_undeleted_idx:
        "CREATE INDEX channel_members_channel_id_deleted_at_fid_undeleted_idx ON {schema}.channel_members USING btree (channel_id, deleted_at, fid) WHERE (deleted_at IS NULL)",
      channel_members_channel_id_fid_undeleted_idx:
        "CREATE INDEX channel_members_channel_id_fid_undeleted_idx ON {schema}.channel_members USING btree (channel_id, fid) WHERE (deleted_at IS NULL)",
      channel_members_fid_channel_id_undeleted_idx:
        "CREATE INDEX channel_members_fid_channel_id_undeleted_idx ON {schema}.channel_members USING btree (fid, channel_id) WHERE (deleted_at IS NULL)",
      channel_members_fid_deleted_at_member_at_undeleted_idx:
        "CREATE INDEX channel_members_fid_deleted_at_member_at_undeleted_idx ON {schema}.channel_members USING btree (fid, deleted_at, member_at DESC) WHERE (deleted_at IS NULL)",
      channel_members_pkey: "CREATE UNIQUE INDEX channel_members_pkey ON {schema}.channel_members USING btree (id)",
      channel_members_updated_at_idx:
        "CREATE INDEX channel_members_updated_at_idx ON {schema}.channel_members USING btree (updated_at)",
    },
  },
  {
    name: "channels",
    columns: [
      ["id", "uuid", true, null],
      ["created_at", "timestamp without time zone", true, null],
      ["updated_at", "timestamp without time zone", true, null],
      ["deleted_at", "timestamp without time zone", false, null],
      ["channel_id", "text", true, null],
      ["url", "text", true, null],
      ["description", "text", true, null],
      ["image_url", "text", true, null],
      ["lead_fid", "bigint", true, null],
      ["moderator_fids", "bigint[]", true, null],
      ["follower_count", "integer", true, null],
      ["timestamp", "timestamp without time zone", true, null],
      ["name", "text", true, null],
      ["description_mentions", "bigint[]", true, null],
      ["description_mentions_positions", "integer[]", true, null],
      ["member_count", "integer", true, null],
      ["public_casting", "boolean", true, null],
      ["external_link_title", "text", false, null],
      ["external_link_url", "text", false, null],
      ["header_image_url", "text", false, null],
      ["pinned_cast_hash", "bytea", false, null],
    ],
    indexes: {
      channels_channel_id_pattern_idx:
        "CREATE INDEX channels_channel_id_pattern_idx ON {schema}.channels USING btree (channel_id varchar_pattern_ops)",
      channels_dehydrated_cover_idx:
        "CREATE INDEX channels_dehydrated_cover_idx ON {schema}.channels USING btree (channel_id) INCLUDE (name, image_url)",
      channels_id_deleted:
        "CREATE INDEX channels_id_deleted ON {schema}.channels USING btree (channel_id) WHERE (deleted_at IS NULL)",
      channels_id_to_url_idx:
        "CREATE INDEX channels_id_to_url_idx ON {schema}.channels USING btree (channel_id) INCLUDE (url)",
      channels_lead_fid_idx: "CREATE INDEX channels_lead_fid_idx ON {schema}.channels USING btree (lead_fid)",
      channels_moderator_fids_idx:
        "CREATE INDEX channels_moderator_fids_idx ON {schema}.channels USING gin (moderator_fids)",
      channels_name_lower_pattern_idx:
        "CREATE INDEX channels_name_lower_pattern_idx ON {schema}.channels USING btree (lower(name) varchar_pattern_ops)",
      channels_pkey: "CREATE UNIQUE INDEX channels_pkey ON {schema}.channels USING btree (id)",
      channels_updated_at_idx: "CREATE INDEX channels_updated_at_idx ON {schema}.channels USING btree (updated_at)",
      channels_url_deleted:
        "CREATE INDEX channels_url_deleted ON {schema}.channels USING btree (url) WHERE (deleted_at IS NULL)",
      channels_url_to_id_idx:
        "CREATE INDEX channels_url_to_id_idx ON {schema}.channels USING btree (url) INCLUDE (channel_id)",
    },
  },
  {
    name: "fids",
    columns: [
      ["id", "uuid", true, null],
      ["created_at", "timestamp without time zone", true, null],
      ["updated_at", "timestamp without time zone", true, null],
      ["fid", "integer", true, null],
      ["custody_address", "bytea", true, null],
      ["timestamp", "timestamp without time zone", true, null],
      ["recovery_address", "bytea", false, null],
      ["registered_at", "timestamp without time zone", false, null],
    ],
    indexes: {
      fids_custody_address_idx: "CREATE INDEX fids_custody_address_idx ON {schema}.fids USING btree (custody_address)",
      fids_custody_address_to_fid_idx:
        "CREATE INDEX fids_custody_address_to_fid_idx ON {schema}.fids USING btree (custody_address) INCLUDE (fid)",
      fids_fid_idx: "CREATE INDEX fids_fid_idx ON {schema}.fids USING btree (fid)",
      fids_fid_to_custody_address_idx:
        "CREATE INDEX fids_fid_to_custody_address_idx ON {schema}.fids USING btree (fid) INCLUDE (custody_address)",
      fids_fid_updated_at_id_desc_idx:
        "CREATE INDEX fids_fid_updated_at_id_desc_idx ON {schema}.fids USING btree (fid, updated_at DESC, id DESC) INCLUDE (registered_at)",
      fids_pkey: "CREATE UNIQUE INDEX fids_pkey ON {schema}.fids USING btree (id)",
      fids_registered_at_idx: "CREATE INDEX fids_registered_at_idx ON {schema}.fids USING btree (registered_at)",
      fids_timestamp_idx: 'CREATE INDEX fids_timestamp_idx ON {schema}.fids USING btree ("timestamp" DESC)',
      fids_updated_at_idx: "CREATE INDEX fids_updated_at_idx ON {schema}.fids USING btree (updated_at)",
    },
    extraColumns: extras["fids"],
  },
  {
    name: "follow_counts",
    columns: [
      ["id", "uuid", true, null],
      ["fid", "bigint", true, null],
      ["created_at", "timestamp without time zone", true, "CURRENT_TIMESTAMP"],
      ["updated_at", "timestamp without time zone", true, "CURRENT_TIMESTAMP"],
      ["follower_count", "bigint", true, "0"],
      ["following_count", "bigint", true, "0"],
      ["filtered_following_count", "bigint", false, null],
      ["filtered_follower_count", "bigint", false, null],
    ],
    indexes: {
      follow_counts_fid: "CREATE INDEX follow_counts_fid ON {schema}.follow_counts USING btree (fid)",
      follow_counts_pkey: "CREATE UNIQUE INDEX follow_counts_pkey ON {schema}.follow_counts USING btree (id)",
      idx_follow_counts_upsert:
        "CREATE INDEX idx_follow_counts_upsert ON {schema}.follow_counts USING btree (id, updated_at)",
    },
  },
  {
    name: "follows",
    columns: [
      ["id", "uuid", true, null],
      ["created_at", "timestamp without time zone", true, "CURRENT_TIMESTAMP"],
      ["updated_at", "timestamp without time zone", true, "CURRENT_TIMESTAMP"],
      ["deleted_at", "timestamp without time zone", false, null],
      ["timestamp", "timestamp without time zone", true, null],
      ["fid", "bigint", true, null],
      ["target_fid", "bigint", true, null],
      ["display_timestamp", "timestamp without time zone", false, null],
      ["creator_app_fid", "bigint", false, null],
      ["deleter_app_fid", "bigint", false, null],
    ],
    indexes: {
      follows_fid: "CREATE INDEX follows_fid ON {schema}.follows USING btree (fid)",
      follows_fid_target_deleted:
        "CREATE INDEX follows_fid_target_deleted ON {schema}.follows USING btree (fid, target_fid) WHERE (deleted_at IS NULL)",
      follows_fid_ts_deleted:
        'CREATE INDEX follows_fid_ts_deleted ON {schema}.follows USING btree (fid, "timestamp" DESC) WHERE (deleted_at IS NULL)',
      follows_pkey: "CREATE UNIQUE INDEX follows_pkey ON {schema}.follows USING btree (id)",
      follows_target_fid: "CREATE INDEX follows_target_fid ON {schema}.follows USING btree (target_fid)",
      follows_target_fid_deleted:
        "CREATE INDEX follows_target_fid_deleted ON {schema}.follows USING btree (target_fid, fid) WHERE (deleted_at IS NULL)",
      follows_timestamp_not_deleted:
        'CREATE INDEX follows_timestamp_not_deleted ON {schema}.follows USING btree ("timestamp") WHERE (deleted_at IS NULL)',
      idx_follows_upsert: "CREATE INDEX idx_follows_upsert ON {schema}.follows USING btree (id, updated_at)",
    },
  },
  {
    name: "neynar_user_scores",
    columns: [
      ["id", "uuid", true, null],
      ["fid", "bigint", true, null],
      ["created_at", "timestamp without time zone", true, "CURRENT_TIMESTAMP"],
      ["updated_at", "timestamp without time zone", true, "CURRENT_TIMESTAMP"],
      ["deleted_at", "timestamp without time zone", false, null],
      ["score", "real", true, null],
    ],
    indexes: {
      idx_neynar_user_scores_upsert:
        "CREATE INDEX idx_neynar_user_scores_upsert ON {schema}.neynar_user_scores USING btree (id, updated_at)",
      neynar_user_scores_fid: "CREATE INDEX neynar_user_scores_fid ON {schema}.neynar_user_scores USING btree (fid)",
      neynar_user_scores_pkey:
        "CREATE UNIQUE INDEX neynar_user_scores_pkey ON {schema}.neynar_user_scores USING btree (id)",
    },
  },
  {
    name: "profile_external_accounts",
    columns: [
      ["id", "uuid", true, null],
      ["created_at", "timestamp without time zone", true, null],
      ["updated_at", "timestamp without time zone", true, null],
      ["deleted_at", "timestamp without time zone", false, null],
      ["timestamp", "timestamp without time zone", true, null],
      ["fid", "bigint", true, null],
      ["app_fid", "bigint", true, null],
      ["type", "smallint", true, null],
      ["account", "text", true, null],
    ],
    indexes: {
      profile_external_accounts_account_type_idx:
        "CREATE INDEX profile_external_accounts_account_type_idx ON {schema}.profile_external_accounts USING btree (account, type) WHERE (deleted_at IS NULL)",
      profile_external_accounts_fid_index:
        "CREATE INDEX profile_external_accounts_fid_index ON {schema}.profile_external_accounts USING btree (fid) WHERE (deleted_at IS NULL)",
      profile_external_accounts_pkey:
        "CREATE UNIQUE INDEX profile_external_accounts_pkey ON {schema}.profile_external_accounts USING btree (id)",
      profile_external_accounts_type_account_index:
        "CREATE INDEX profile_external_accounts_type_account_index ON {schema}.profile_external_accounts USING btree (type, account) WHERE (deleted_at IS NULL)",
      profile_external_accounts_updated_at_idx:
        "CREATE INDEX profile_external_accounts_updated_at_idx ON {schema}.profile_external_accounts USING btree (updated_at)",
    },
  },
  {
    name: "profiles",
    columns: [
      ["id", "uuid", true, null],
      ["created_at", "timestamp without time zone", true, "CURRENT_TIMESTAMP"],
      ["updated_at", "timestamp without time zone", true, "CURRENT_TIMESTAMP"],
      ["deleted_at", "timestamp without time zone", false, null],
      ["fid", "bigint", true, null],
      ["bio", "text", false, null],
      ["pfp_url", "text", false, null],
      ["url", "text", false, null],
      ["username", "text", false, null],
      ["display_name", "text", false, null],
      ["location", "text", false, null],
      ["latitude", "real", false, null],
      ["longitude", "real", false, null],
      ["primary_eth_address", "bytea", false, null],
      ["primary_sol_address", "bytea", false, null],
      ["banner_url", "text", false, null],
      ["token_uri", "text", false, null],
    ],
    indexes: {
      idx_profiles_active_username_lower_fid:
        "CREATE INDEX idx_profiles_active_username_lower_fid ON {schema}.profiles USING btree (lower(username)) INCLUDE (fid) WHERE (deleted_at IS NULL)",
      idx_profiles_upsert: "CREATE INDEX idx_profiles_upsert ON {schema}.profiles USING btree (id, updated_at)",
      profiles_fid: "CREATE INDEX profiles_fid ON {schema}.profiles USING btree (fid)",
      profiles_pkey: "CREATE UNIQUE INDEX profiles_pkey ON {schema}.profiles USING btree (id)",
    },
  },
  {
    name: "reactions",
    columns: [
      ["id", "uuid", true, null],
      ["created_at", "timestamp without time zone", true, null],
      ["updated_at", "timestamp without time zone", true, null],
      ["deleted_at", "timestamp without time zone", false, null],
      ["timestamp", "timestamp without time zone", true, null],
      ["reaction_type", "smallint", true, null],
      ["fid", "bigint", true, null],
      ["target_hash", "bytea", false, null],
      ["target_fid", "bigint", false, null],
      ["target_url", "text", false, null],
      ["creator_app_fid", "bigint", false, null],
      ["deleter_app_fid", "bigint", false, null],
    ],
    indexes: {
      idx_reactions_timestamp: 'CREATE INDEX idx_reactions_timestamp ON {schema}.reactions USING btree ("timestamp")',
      idx_reactions_upsert: "CREATE INDEX idx_reactions_upsert ON {schema}.reactions USING btree (id, updated_at)",
      reactions_fid: "CREATE INDEX reactions_fid ON {schema}.reactions USING btree (fid)",
      reactions_fid_like_timeline_not_deleted:
        'CREATE INDEX reactions_fid_like_timeline_not_deleted ON {schema}.reactions USING btree (fid, "timestamp" DESC, id DESC) INCLUDE (target_hash) WHERE ((deleted_at IS NULL) AND (reaction_type = 1))',
      reactions_fid_recast_timeline_not_deleted:
        'CREATE INDEX reactions_fid_recast_timeline_not_deleted ON {schema}.reactions USING btree (fid, "timestamp" DESC, id DESC) INCLUDE (target_hash) WHERE ((deleted_at IS NULL) AND (reaction_type = 2))',
      reactions_fid_target_deleted:
        "CREATE INDEX reactions_fid_target_deleted ON {schema}.reactions USING btree (fid, target_hash) WHERE ((deleted_at IS NULL) AND (reaction_type = ANY (ARRAY[1, 2])))",
      reactions_pkey: "CREATE UNIQUE INDEX reactions_pkey ON {schema}.reactions USING btree (id)",
      reactions_target_fid: "CREATE INDEX reactions_target_fid ON {schema}.reactions USING btree (target_fid)",
      reactions_target_hash: "CREATE INDEX reactions_target_hash ON {schema}.reactions USING btree (target_hash)",
      reactions_target_type_active_idx:
        "CREATE INDEX reactions_target_type_active_idx ON {schema}.reactions USING btree (target_hash, reaction_type) WHERE (deleted_at IS NULL)",
      reactions_timestamp_not_deleted:
        'CREATE INDEX reactions_timestamp_not_deleted ON {schema}.reactions USING btree ("timestamp") WHERE (deleted_at IS NULL)',
    },
  },
  {
    name: "signers",
    columns: [
      ["id", "uuid", true, null],
      ["created_at", "timestamp without time zone", true, null],
      ["updated_at", "timestamp without time zone", true, null],
      ["deleted_at", "timestamp without time zone", false, null],
      ["timestamp", "timestamp without time zone", true, null],
      ["fid", "bigint", true, null],
      ["app_fid", "bigint", true, null],
      ["signer", "bytea", true, null],
    ],
    indexes: {
      signers_fid_idx: "CREATE INDEX signers_fid_idx ON {schema}.signers USING btree (fid)",
      signers_pkey: "CREATE UNIQUE INDEX signers_pkey ON {schema}.signers USING btree (id)",
      signers_signer_fid_idx: "CREATE INDEX signers_signer_fid_idx ON {schema}.signers USING btree (signer, fid)",
      signers_updated_at_idx: "CREATE INDEX signers_updated_at_idx ON {schema}.signers USING btree (updated_at)",
    },
    extraColumns: extras["signers"],
  },
  {
    name: "storage_rentals",
    columns: [
      ["id", "uuid", true, null],
      ["created_at", "timestamp without time zone", true, null],
      ["updated_at", "timestamp without time zone", true, null],
      ["fid", "integer", true, null],
      ["units", "integer", true, null],
      ["expiry", "timestamp without time zone", true, null],
      ["timestamp", "timestamp without time zone", true, null],
      ["transaction_hash", "bytea", false, null],
      ["log_index", "integer", false, null],
    ],
    indexes: {
      storage_rentals_fid_expiry_idx:
        "CREATE INDEX storage_rentals_fid_expiry_idx ON {schema}.storage_rentals USING btree (fid, expiry DESC)",
      storage_rentals_fid_idx: "CREATE INDEX storage_rentals_fid_idx ON {schema}.storage_rentals USING btree (fid)",
      storage_rentals_pkey: "CREATE UNIQUE INDEX storage_rentals_pkey ON {schema}.storage_rentals USING btree (id)",
      storage_rentals_updated_at_idx:
        "CREATE INDEX storage_rentals_updated_at_idx ON {schema}.storage_rentals USING btree (updated_at)",
    },
  },
  {
    name: "tier_purchases",
    columns: [
      ["id", "uuid", true, null],
      ["created_at", "timestamp without time zone", true, null],
      ["updated_at", "timestamp without time zone", true, null],
      ["fid", "bigint", true, null],
      ["tier_type", "integer", true, null],
      ["duration_in_days", "bigint", true, null],
      ["payer", "bytea", true, null],
      ["timestamp", "timestamp without time zone", true, null],
      ["transaction_hash", "bytea", true, null],
      ["log_index", "integer", true, null],
    ],
    indexes: {
      tier_purchases_fid_type:
        "CREATE INDEX tier_purchases_fid_type ON {schema}.tier_purchases USING btree (fid, tier_type)",
      tier_purchases_pkey: "CREATE UNIQUE INDEX tier_purchases_pkey ON {schema}.tier_purchases USING btree (id)",
      tier_purchases_pro_fid_idx:
        'CREATE INDEX tier_purchases_pro_fid_idx ON {schema}.tier_purchases USING btree (fid) INCLUDE ("timestamp", duration_in_days) WHERE (tier_type >= 1)',
    },
  },
  {
    name: "user_labels",
    columns: [
      ["id", "uuid", true, null],
      ["created_at", "timestamp without time zone", true, null],
      ["updated_at", "timestamp without time zone", true, null],
      ["deleted_at", "timestamp without time zone", false, null],
      ["source", "text", true, null],
      ["provider_fid", "bigint", true, null],
      ["target_fid", "bigint", true, null],
      ["label_type", "text", true, null],
      ["label_value", "text", true, null],
      ["timestamp", "timestamp without time zone", true, null],
    ],
    indexes: {
      user_labels_pkey: "CREATE UNIQUE INDEX user_labels_pkey ON {schema}.user_labels USING btree (id)",
      user_labels_timestamp_idx:
        "CREATE INDEX user_labels_timestamp_idx ON {schema}.user_labels USING btree (\"timestamp\") WITH (fillfactor='100', deduplicate_items='true')",
      user_labels_updated_at_idx:
        "CREATE INDEX user_labels_updated_at_idx ON {schema}.user_labels USING btree (updated_at) WITH (fillfactor='100', deduplicate_items='true')",
    },
  },
  {
    name: "usernames",
    columns: [
      ["id", "uuid", true, null],
      ["created_at", "timestamp without time zone", true, null],
      ["updated_at", "timestamp without time zone", true, null],
      ["deleted_at", "timestamp without time zone", false, null],
      ["username", "text", true, null],
      ["fid", "integer", true, null],
      ["custody_address", "bytea", false, null],
      ["proof_timestamp", "timestamp without time zone", true, null],
      ["type", "smallint", true, null],
    ],
    indexes: {
      idx_usernames_upsert: "CREATE INDEX idx_usernames_upsert ON {schema}.usernames USING btree (id, updated_at)",
      usernames_fid: "CREATE INDEX usernames_fid ON {schema}.usernames USING btree (fid)",
      usernames_pkey: "CREATE UNIQUE INDEX usernames_pkey ON {schema}.usernames USING btree (id)",
    },
  },
  {
    name: "verifications",
    columns: [
      ["id", "uuid", true, null],
      ["created_at", "timestamp without time zone", true, "CURRENT_TIMESTAMP"],
      ["updated_at", "timestamp without time zone", true, "CURRENT_TIMESTAMP"],
      ["deleted_at", "timestamp without time zone", false, null],
      ["timestamp", "timestamp without time zone", true, null],
      ["address", "bytea", true, null],
      ["fid", "bigint", true, null],
      ["protocol", "smallint", true, null],
      ["app_fid", "bigint", false, null],
    ],
    indexes: {
      idx_verifications_upsert:
        "CREATE INDEX idx_verifications_upsert ON {schema}.verifications USING btree (id, updated_at)",
      verifications_address: "CREATE INDEX verifications_address ON {schema}.verifications USING btree (address)",
      verifications_address_protocol_fid_not_deleted:
        "CREATE INDEX verifications_address_protocol_fid_not_deleted ON {schema}.verifications USING btree (address, protocol) INCLUDE (fid) WHERE (deleted_at IS NULL)",
      verifications_app_fid: "CREATE INDEX verifications_app_fid ON {schema}.verifications USING btree (app_fid)",
      verifications_fid: "CREATE INDEX verifications_fid ON {schema}.verifications USING btree (fid)",
      verifications_pkey: "CREATE UNIQUE INDEX verifications_pkey ON {schema}.verifications USING btree (id)",
      verifications_timestamp_not_deleted:
        'CREATE INDEX verifications_timestamp_not_deleted ON {schema}.verifications USING btree ("timestamp") WHERE (deleted_at IS NULL)',
    },
  },
];
