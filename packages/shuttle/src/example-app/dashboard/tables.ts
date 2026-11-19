/**
 * Every business table the Farcaster DB has to fill (the 19 reference tables in
 * docs/farcaster-db/000003 and 000007) with where each one stands. A table counts as done only
 * when a registered projection writes it; `stage` is the planning state for the others.
 * Update this list when a plan is written or a table's feasibility changes.
 */
export type TableStage = "planned" | "uncertain";
export type TableGroup = "done" | TableStage;

export type BusinessTable = {
  table: string;
  title: string;
  /** Where the rows come from. */
  source: string;
  /** Plan for a planned table; the open question for an uncertain one. */
  note: string;
  /** What has to exist first (other work, not just other tables). */
  prerequisites?: string[];
  /** File names under docs/farcaster-db. */
  docs: string[];
  stage: TableStage;
};

export const BUSINESS_TABLES: BusinessTable[] = [
  {
    table: "blocks",
    title: "屏蔽关系",
    source: "LINK_ADD / LINK_REMOVE / LINK_COMPACT_STATE（type=block）",
    note: "实时投影与只补投影 backfill 均已上线。",
    docs: ["000005_blocks-转换说明.md", "100_shuttle-inline-projection.md"],
    stage: "planned",
  },
  {
    table: "fids",
    title: "账号",
    source: "onchain_events：ID_REGISTER（注册、转移、恢复地址变更）",
    note: "按 000008 的字段口径从链上账号事件生成，不能只从 messages 里 DISTINCT fid（缺 custody_address 等必填字段）。",
    prerequisites: ["dispatcher 支持 onchain 变更", "接入 OnChainEvent reconciliation 补历史事件"],
    docs: ["000008_fids-分析与实现计划.md", "000014_Neynar补齐FID.md"],
    stage: "planned",
  },
  {
    table: "signers",
    title: "签名密钥",
    source: "onchain_events：SIGNER（KEY 注册、撤销及元数据）；KEY_ADD / KEY_REMOVE 消息",
    note: "实时投影与 SIGNER 只补投影 backfill 已上线：按 (fid, 公钥) 从链上事件生成，app_fid 取 SignedKeyRequest 元数据的 requestFid。与 reference 一致只收 keyType 1；gasless KEY_ADD 路径不在 reference 中，未接入。",
    docs: [
      "000010_signers-分析与实现计划.md",
      "000015_Neynar补齐signers.md",
      "000016_signers和custody_address的区别.md",
    ],
    stage: "planned",
  },
  {
    table: "usernames",
    title: "用户名",
    source: "用户名证明（USERNAME_PROOF 消息与 hub 用户名证明）",
    note: "按 000011 / 000017 的规则转换并与 reference 校验。",
    prerequisites: [
      "用户名证明原始落库（迁移 004 未注册，MERGE_USERNAME_PROOF 未处理）",
      "dispatcher 新变更类型（fname 转移需锁两个 fid）",
      "接入 UsernameProof reconciliation",
    ],
    docs: ["000011_usernames-分析与实现计划.md", "000017_usernames转换与reference校验.md"],
    stage: "planned",
  },
  {
    table: "storage_rentals",
    title: "存储租赁",
    source: "onchain_events：STORAGE_RENT（type=4）",
    note: "实时投影与 STORAGE_RENT 只补投影 backfill 已上线，按 Go 版口径按收据（chain、交易、log）生成。LEND_STORAGE 消息不算一次链上租赁。",
    docs: ["000012_storage_rentals-分析与实现计划.md", "100_shuttle-inline-projection.md"],
    stage: "planned",
  },
  {
    table: "tier_purchases",
    title: "会员购买",
    source: "onchain_events：TIER_PURCHASE（type=5，Base 链）",
    note: "实时投影与 TIER_PURCHASE 只补投影 backfill 已上线，与 storage_rentals 共用收据逻辑（Base 链）；当前源库还没有 type=5 事件可供核验。",
    docs: ["000013_tier_purchases-分析与实现计划.md"],
    stage: "planned",
  },
  {
    table: "casts",
    title: "帖子",
    source: "CAST_ADD / CAST_REMOVE 及 raw",
    note: "有转换草稿：正文、引用、删除状态直接取自消息；根帖要追溯祖先帖；ticker、频道提及的提取规则待确认。",
    prerequisites: ["signers（creator_app_fid / deleter_app_fid，可先留空后补）"],
    docs: ["000006_casts-转换草稿.md", "000007_表依赖关系.md"],
    stage: "planned",
  },
  {
    table: "follows",
    title: "关注关系",
    source: "LINK_ADD / LINK_REMOVE / LINK_COMPACT_STATE（type=follow）",
    note: "与 blocks 同一模式，按 fid 从 messages 重算；backfill 已经会拉取 link 消息族。",
    prerequisites: ["signers（创建、删除应用字段，可先留空后补）"],
    docs: ["000007_表依赖关系.md"],
    stage: "planned",
  },
  {
    table: "reactions",
    title: "点赞与转发",
    source: "REACTION_ADD / REACTION_REMOVE",
    note: "目标帖 hash、fid 或 URL 由消息提供，不必等 casts；之后可与 casts 做关联核对。",
    prerequisites: ["signers（创建、删除应用字段，可先留空后补）"],
    docs: ["000007_表依赖关系.md"],
    stage: "planned",
  },
  {
    table: "verifications",
    title: "地址验证",
    source: "VERIFICATION_ADD / VERIFICATION_REMOVE",
    note: "实时投影与只补投影 backfill 已上线：按 (fid, 协议, 地址) 一行，按 hub CRDT 顺序取胜出消息，移除软删除；app_fid 取签名公钥在 signers 中的应用，未知时为空，签名者到达后补上。",
    docs: ["000007_表依赖关系.md"],
    stage: "planned",
  },
  {
    table: "profile_external_accounts",
    title: "外部账户",
    source: "USER_DATA_ADD（Twitter、GitHub 等）",
    note: "实时投影与只补投影 backfill 已上线：按 (fid, 类型) 一行（Twitter、GitHub），空值软删除；app_fid 必填，签名者未知时记缺口、不写行。reference 里另有约 4,700 条非空却已删除的行，规则未知，未复刻。",
    docs: ["000007_表依赖关系.md"],
    stage: "planned",
  },
  {
    table: "profiles",
    title: "用户资料",
    source: "USER_DATA_ADD（头像、昵称、简介、URL、用户名、位置、主地址）",
    note: "实时投影与只补投影 backfill 已上线：每个字段取最新的有效 UserData，未设置为 NULL、显式空串保留；location 原样保存并解析 geo: 坐标，主地址转字节（Solana 为 base58）。与 reference 抽查逐字段一致。",
    docs: ["000007_表依赖关系.md"],
    stage: "planned",
  },
  {
    table: "follow_counts",
    title: "关注计数",
    source: "有效 follows 关系的汇总",
    note: "普通计数在 follows 完成后可做；filtered 计数的过滤口径（是否用评分、标签）未确定。",
    prerequisites: ["follows", "filtered 口径"],
    docs: ["000007_表依赖关系.md"],
    stage: "uncertain",
  },
  {
    table: "channels",
    title: "频道",
    source: "无独立 source；候选：频道接口 / 历史快照，或新协议 CHANNEL_* 消息与频道注册事件",
    note: "Shuttle 目前不转换 CHANNEL_UPDATE 等新消息；新协议的 32 字节 channelId 与 reference 的文本 channel_id 如何对应未验证。",
    docs: ["000007_表依赖关系.md"],
    stage: "uncertain",
  },
  {
    table: "channel_members",
    title: "频道成员",
    source: "候选：频道成员接口 / 历史数据，或 CHANNEL_MEMBER 消息",
    note: "不能从发帖行为推断成员；历史成员能否取回未确认。",
    prerequisites: ["channels 的标识映射"],
    docs: ["000007_表依赖关系.md"],
    stage: "uncertain",
  },
  {
    table: "channel_follows",
    title: "频道关注",
    source: "协议来源未确认",
    note: "不能从用户 follows 或频道发帖推断；需要先找到可用的数据来源。",
    prerequisites: ["channels 的标识映射"],
    docs: ["000007_表依赖关系.md"],
    stage: "uncertain",
  },
  {
    table: "neynar_user_scores",
    title: "Neynar 用户评分",
    source: "Neynar 评分接口（外部）",
    note: "评分由 Neynar 计算，不能用链上数据复刻；是否接入、如何同步未定。",
    docs: ["000007_表依赖关系.md"],
    stage: "uncertain",
  },
  {
    table: "user_labels",
    title: "用户标签",
    source: "标签提供方（外部，已见 warpcast_spam_labels 样本）",
    note: "不能由 block 或评分替代；提供方和接入方式未确认。",
    docs: ["000007_表依赖关系.md"],
    stage: "uncertain",
  },
  {
    table: "parquet_import_tracking",
    title: "Parquet 导入记录",
    source: "Parquet 导入程序的运行记录",
    note: "工具记录表；当前不做 Parquet 导入，可能不需要，也不应伪造历史记录。",
    docs: ["000007_表依赖关系.md"],
    stage: "uncertain",
  },
];

export type BusinessTableStatus = Omit<BusinessTable, "stage"> & { group: TableGroup; projection: string | null };

/** Group each table: done when a registered projection writes it, otherwise its planning stage. */
export function businessTables(projections: { name: string; table?: string }[]): BusinessTableStatus[] {
  return BUSINESS_TABLES.map(({ stage, ...table }) => {
    const projection = projections.find((p) => (p.table ?? p.name) === table.table);
    return { ...table, group: projection ? "done" : stage, projection: projection?.name ?? null };
  });
}
