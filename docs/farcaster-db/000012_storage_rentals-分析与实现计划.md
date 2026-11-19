---
title: storage_rentals 分析与实现计划
tags:
  - 主题/数据同步
  - 技术/架构设计
updated: 2026-11-16
---

# storage_rentals 分析与实现计划

> [!NOTE]
> **部分取代**
> 业务规则章节仍有效。snapshot/init/restart/resume/CLI 相关章节已被 shuttle 内联投影取代，见 [100_shuttle-inline-projection](100_shuttle-inline-projection.md)。

Status: 已实现为 shuttle 内联投影（`example-app/projections/storageRentals.ts`，见 [100](100_shuttle-inline-projection.md#storage_rentals-投影)）。

## 已确认的来源与结构

reference结构已于2026-11-16只读检查并保存：9列、4个索引，只有id主键，fid/units为integer，expiry/timestamp必填，transaction_hash/log_index可空。2026-11-17重新读取的结构由迁移008建表并校验，与此一致。2026-11-18只读抽查（小样本，非100条抽查）：source最早3条租赁（2026年，1年期）在reference中的fid、units、expiry、timestamp、transaction_hash、log_index与本文口径一致；另查reference中fid 2、3、194等的历史租赁，expiry−timestamp在2024-08-29前为3年、2025-07-16前为2年，与Shuttle `getStorageUnitExpiry`的三段规则一致，说明expiry直接取body.expiry即可。reference有6条fid=0的租赁；dispatcher按fid加锁并拒绝fid 0，内联投影不接这类事件（仍走通用原始落库），属于已知缺口。

source来自onchain_events.type=4的存储租赁事件，当前已有数百条，chain_id=10。它记录一次租赁收据，不是账户余额；过期记录仍保留，不因当前时间过期而删除。messages.type=15的LEND_STORAGE不是新的链上租赁，不能混入。

## 字段与身份

- 业务身份：(chain_id,tx_hash,log_index)，不能按fid覆盖；同用户多次租赁要多行。
- fid：原事件FID，非负且必须适配目标integer；不要求先存在target.fids。
- units：body.units，非负整数且适配integer，缺失或越界记录字段缺口，不截断。
- expiry：body.expiry已是Shuttle算出的Unix秒，直接转UTC，不再加一年。
- timestamp：block_timestamp，UTC。
- transaction_hash、log_index：原事件交易哈希与日志序号。虽然目标允许空，但本轮要可靠识别收据，因此源缺少这些身份信息时pending，不猜值。
- id：按表名及收据身份生成稳定UUID；已有行保留id与created_at；业务字段不变时保留updated_at。

链上位置和完整输入保存在辅助状态中。较旧位置不覆盖已知较新投影，同一位置源字段修正可重新计算。相同收据重复内容合并；相同身份内容冲突不以UUID决定，明确报告。

内联投影不再需要辅助状态：原始onchain_events只增不改，每次按fid从全部type=4事件重算，结果与顺序无关。

## 缺口记录

每个无法生成的收据保存run_id、source_id、fid、已知的交易和log_index、missing_fields、reason（内联投影中记在shuttle_projection_issues：subject为交易哈希，reason含fid、log_index和缺失字段，payload含chain、区块和log_index）。必填内容或身份缺失不创建不完整收据。source没有提供的历史租赁无法补出，记录来源时间范围及数量，不复制reference记录。

## 实施步骤

> [!WARNING]
> **已被 shuttle 内联投影取代（见 [100](100_shuttle-inline-projection.md)）**
> 本节描述 Go 一次性 snapshot/init（restart/resume/CLI）的做法，仅作存档；增量现在由 shuttle 在同一事务里内联投影。业务规则以本文其他章节为准。

1. 先保存本计划，再用CLI+隔离数据库测试一个租赁事件的字段映射和结构复制。
2. 复用initrun的持久快照、独立锁、事务游标和restart/resume；同FID完整历史中每张收据独立写入。
3. 实现schema storage_rentals与init storage_rentals，预览默认只读，apply明确选择restart/resume。
4. 批量读取目标和依据，流水线写入，覆盖同FID多笔、过期保留、重复运行、缺字段、旧水位与失败恢复。
5. reference恢复连接后核对真实结构/索引，再应用和独立抽查100条；若连接或数据不足，明确记录待完成项。
6. 审查修复后提交，更新缺口清单和实施进度。

## 当前结果

> [!WARNING]
> **已被 shuttle 内联投影取代（见 [100](100_shuttle-inline-projection.md)）**
> 本节描述 Go 一次性 snapshot/init（restart/resume/CLI）的做法，仅作存档；增量现在由 shuttle 在同一事务里内联投影。业务规则以本文其他章节为准。

代码及隔离数据库验证完成。验证包含同用户多笔、过期保留、缺到期时间记录、重复执行、旧位置保护、事务回滚和离线断点恢复。真实source此前查到394条租赁事件，但reference仍连接超时，真实target尚未建表和执行；真实抽查明确待完成。

审查已完成并修复两个边界：同一交易收据的FID修正保留原ID；缺units和明确units=0不再当成相同输入。预览按批次保留数据，跨FID冲突在只读快照中通过SQL检查。已有大目标表按交易哈希补查可能较慢，因为reference没有该列索引；当前没有额外增加业务索引。
