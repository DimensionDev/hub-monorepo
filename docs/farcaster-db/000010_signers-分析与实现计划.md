---
title: signers 分析与实现计划
tags:
  - 主题/数据同步
  - 技术/架构设计
updated: 2026-11-16
---

# signers 分析与实现计划

> [!NOTE]
> **部分取代**
> 业务规则章节仍有效。snapshot/init/restart/resume/CLI 相关章节已被 shuttle 内联投影取代，见 [100_shuttle-inline-projection](100_shuttle-inline-projection.md)。

Status: 已实现为 shuttle 内联投影（`example-app/projections/signers.ts`，见 [100](100_shuttle-inline-projection.md#signers-投影与-app_fid-归属)）。与 reference 一致只收 keyType 1。

## 已完成的分析

reference.signers 有 8 列、4 个索引，id 主键，没有 (fid,signer) 唯一约束。timestamp、fid、app_fid、signer 必填，deleted_at 可空。业务主键按 (fid,signer) 识别，保留已有 UUID。

本次 source 的链上 signer 事件有 717 条、582 个 FID、717 个不同公钥：699 条 ADD 的 metadataType=1，均能按 ABI 解出 requestFid；18 条只有 REMOVE，缺少对应 ADD。ADD 中有 461 条 keyType=1、238 条 keyType=2，key 字节长度都是 32。统计是检查时的快照，之后会变化。

应用归属取 SignedKeyRequestMetadata.requestFid，不取用户 FID，也不硬编码 9152。metadata 是动态 ABI tuple，首个 word 是 tuple 偏移，不是 requestFid；还要检查签名偏移与长度。注册信息来自 source，reference 只用于结构、索引和对照。

## 范围与缺口

先实现现有 onchain_events(type=1,chain_id=10) 可以完整证明的链上 signers。每把公钥的注册、撤销、ADMIN_RESET 都需要处理；已有 ADD 的撤销记录保留为软删除行。

新式 KEY_ADD/KEY_REMOVE 属于独立协议路径，当前 Shuttle backfill 没有这些类型，body 也只存占位。raw 虽有潜在信息，但滑动 TTL、权限和有效状态还需分析，不能与链上事件直接混排后宣称支持。第一阶段明确记录 gasless 路径尚未实现、全量覆盖未知，不把这个范围外的数据悄悄视作没有 signer。

缺少 ADD 的 REMOVE/ADMIN_RESET，无法确定 app_fid 和注册 timestamp，保存逐 (fid,signer) 的 pending、missing_fields 和原因，不猜值、不创建不完整目标行。metadataType 不支持或 ABI 无法取得合法应用 FID 时，也记录 app_fid 缺口。

## 字段与事件规则

- fid、signer：事件用户和原始 key 字节，保留字节表示，不把 keyType=2 任意裁成 20 字节。
- app_fid：ADD 元数据的 requestFid，必须在正数 bigint 范围内。
- timestamp：ADD 的区块时间；撤销不改注册时间。
- deleted_at：REMOVE 区块时间；ADMIN_RESET 在有原始 ADD 时恢复未删除状态。
- 同一公钥按 block_number、log_index 处理；入库 UUID 只确定快照扫描顺序。重复链上位置冲突必须在预览与应用中拒绝。
- 同一公钥被 REMOVE 后，后到的 ADD 不能直接把它恢复；按 Snapchain 规则处理，ADMIN_RESET 通过最早捕获的原 ADD 恢复，并允许后续合法 ADD 更新应用归属与注册时间，保持目标 UUID。异常 ADD 历史不能靠 UUID 决定。
- id：稳定 UUID；已有行保留 id、created_at；业务数据不变时保留 updated_at。
- 来源快照比已经处理的链上位置更旧时，不覆盖较新的 target 结果。

## 实施步骤

> [!WARNING]
> **已被 shuttle 内联投影取代（见 [100](100_shuttle-inline-projection.md)）**
> 本节描述 Go 一次性 snapshot/init（restart/resume/CLI）的做法，仅作存档；增量现在由 shuttle 在同一事务里内联投影。业务规则以本文其他章节为准。

1. 先提取新初始化器共用的快照任务生命周期，保持 fids 的已有状态表、版本、统计和断点兼容。blocks 暂保留已有稳定实现，后续迁移另行验证。
2. 通过 CLI + 独立测试数据库先验证一个 ADD 的应用归属和结构复制；然后逐步加入 REMOVE、ADMIN_RESET、缺失 ADD、非法元数据、乱序、重跑及失败恢复。
3. 实现 schema signers、init signers 的只读预览、--apply --restart 和 --resume。任务范围、锁与进度按业务表隔离。
4. 按批读取完整 FID 历史与目标数据，流水线写入，避免每把公钥多次串行远程查询。
5. 保存逐公钥的已处理依据和字段缺口；待补全清单与快照进度、业务写入同事务提交。
6. 只读预览真实 source，报告可生成与待补全；创建与 reference 完全一致的 target 表与4个索引后初始化。随机核对100把公钥和全部 pending，验证 restart/resume 幂等。
7. 审查、修复、记录覆盖限制，验证通过后提交。

## 验证与记录

沿用用户已接受的 Go 命令边界和隔离数据库测试。真实 source/reference 只读；不把 source 数据少或 source 尚无某类事件写成网络不存在该数据。无法实现的路径、字段、业务主键写入缺口文档及运行状态，便于补齐来源后 restart。

依赖：不要求 target.fids 先有所有用户记录。应用身份的 FID 可以直接保存，但不能借 fids 去猜应用归属。

依据：Shuttle 的数据库类型、消息 body 转换和 MessageReconciliation；Snapchain onchain_event_store 的 signer 索引规则；SignedKeyRequestMetadata ABI；reports/signers-source-events.json 与 signers-analysis-summary.json。

## 当前实施结果

> [!WARNING]
> **已被 shuttle 内联投影取代（见 [100](100_shuttle-inline-projection.md)）**
> 本节描述 Go 一次性 snapshot/init（restart/resume/CLI）的做法，仅作存档；增量现在由 shuttle 在同一事务里内联投影。业务规则以本文其他章节为准。

链上子集已实现。首轮任务 e447c38e-062e-453f-a337-37c01d2b8649 捕获 719 条事件，涉及 583 个 FID，写入 701 把公钥；18 把只有 REMOVE，已在 conversion_state.signers_issues 逐公钥保存 app_fid、timestamp 的缺口。分析时的 699 条 ADD 到执行时增加到 701，源库仍在采集。

随机核对100把公钥的应用归属、注册时间和删除状态全部通过；完成任务 resume 后全部701行的精确摘要不变；restart 新任务 e51a4a49-b2f8-403b-8ddf-e0dca67a87c1 产生0新增、0更新、701未变化。

共用快照生命周期与批次读取已经提取到 internal/initrun，fids 的旧状态名称、版本及统计格式保持兼容，并已对真实旧 fids 完成任务执行 resume 验证。blocks 保留原实现。Go全部隔离集成测试（race）和vet通过。代码审查已完成，发现的RESET重新注册问题已修正并通过复审。

运行时报告始终标明 scope=onchain_only，gasless=not_processed。这个阶段不表示已完成所有 gasless signer 支持或全网历史覆盖。

审查补充依据：https://github.com/farcasterxyz/contracts/blob/main/src/KeyRegistry.sol，ADMIN_RESET 将合约 key 状态重置为 NULL，允许再次注册，不能把 RESET 后 ADD 当作重复活跃注册。

审查修正：ADD → ADMIN_RESET → ADD 可合法重新注册；再遇ADMIN_RESET按Snapchain恢复最早匹配ADD。两种序列已通过命令级隔离测试，目标UUID稳定。统一了所有初始化器的捕获失败restart与处理失败resume提示。

## 2026-11-16：补齐ADD历史

按先分析再实施的计划完成Neynar补齐。18把公钥全部找到ADD与REMOVE，只写target并保留撤销状态，新增neynar_backfilled来源标记及审计。按(fid,signer)分页和保存断点，旧FID任务兼容。真实18条逐字段核对、幂等及隔离数据库测试通过；source历史仍不完整，gasless范围不变。详见[000015_Neynar补齐signers](000015_Neynar%E8%A1%A5%E9%BD%90signers.md)。
