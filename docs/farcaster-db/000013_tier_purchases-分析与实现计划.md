---
tags:
  - 类型/操作指南
  - 主题/数据同步
---

# tier_purchases 分析与实现计划

> [!NOTE]
> **部分取代**
> 业务规则章节仍有效。snapshot/init/restart/resume/CLI 相关章节已被 shuttle 内联投影取代，见 [100_shuttle-inline-projection](100_shuttle-inline-projection.md)。

Status: ready-for-agent

## 数据与依赖

先于编码保存本计划。reference已只读保存10列、3索引，fid/duration_in_days为bigint，tier_type/log_index为integer，payer/hash必填bytea。没有声明外键；不要求先写fids。结构仍通过通用schema命令从实际reference读取，不靠手写DDL覆盖真实表。

来源是source.onchain_events.type=5。Shuttle的TierPurchaseEventBody持久化为tierType、forDays、payer（0x十六进制）。TierRegistry在Base mainnet，chain_id=8453，不能沿用storage_rentals的Optimism链10。当前source查询type IN(4,5)只有type4，type5为0；来源空时创建空任务并明确报告，不复制reference购买记录。

## 字段

- fid：事件fid，非负bigint。tier_type=body.tierType，非负且适配integer；保留原值，不用它推导会员是否有效。
- duration_in_days=body.forDays，非负bigint，不自行增加期限。
- payer=body.payer，严格0x前缀20字节地址；不能用当前custody_address替代付款者。
- timestamp=block_timestamp，UTC；transaction_hash=tx_hash32字节；log_index原日志序号。
- id按表名+链+交易哈希+日志序号稳定生成；同FID多次购买保留多行。已有ID/created_at保留，未变化updated_at不变。

## 实施和验证

> [!WARNING]
> **已被 shuttle 内联投影取代（见 [100](100_shuttle-inline-projection.md)）**
> 本节描述 Go 一次性 snapshot/init（restart/resume/CLI）的做法，仅作存档；增量现在由 shuttle 在同一事务里内联投影。业务规则以本文其他章节为准。

复用收据初始化模块，扩充独立tier配置和解码。preview只读，apply必须restart/resume，独立运行/快照/事务游标/锁/依据。缺少必填或身份字段按run_id/source_id/fid/交易hash/log_index、missing_fields、reason记录到conversion_state.tier_purchases_issues；不写不完整行。过旧位置不能覆盖新依据。隔离CLI测试先行：10列3索引（含INCLUDE及部分条件）、大于int4的FID/天数、真实20字节付款人、多次购买、缺字段、幂等及断点恢复复用收据引擎。reference连接恢复后实际复核结构；当前无真实type5数据，100条抽查待数据齐备。

## 协议证据

- https://github.com/DimensionDev/hub-monorepo/blob/main/packages/shuttle/src/example-app/app.ts
- https://github.com/DimensionDev/hub-monorepo/blob/main/packages/core/src/protobufs/generated/onchain_event.ts
- https://github.com/farcasterxyz/contracts （Base mainnet TierRegistry部署）

## 当前结果

> [!WARNING]
> **已被 shuttle 内联投影取代（见 [100](100_shuttle-inline-projection.md)）**
> 本节描述 Go 一次性 snapshot/init（restart/resume/CLI）的做法，仅作存档；增量现在由 shuttle 在同一事务里内联投影。业务规则以本文其他章节为准。

代码已实现，完整隔离Go race测试和vet通过。验证了Base链、FID/天数不截断、多次购买、付款人字节、缺字段与错误链审计、幂等restart和离线完成resume。当前source无type5事件；真实建表、真实样本核对待reference恢复及源数据齐备。

两轴审查完成，无新增明确规格偏差。与storage_rentals共用收据快照和事务恢复逻辑，但链、事件类型、字段宽度和付款人解码使用独立配置。
