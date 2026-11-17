# Farcaster DB 设计文档

这个目录收录 Farcaster 原始数据（shuttle 写入的 `messages` / `onchain_events`）转换成业务表的设计资料。

- **现行设计**：[100_shuttle-inline-projection](100_shuttle-inline-projection.md)。shuttle 消费现有 Redis Stream，在同一个 Postgres 事务里写原始表和业务表，不用 Kafka。
- 000001–000017 是从 Obsidian 迁来的原始笔记，文件编号不变，已脱敏（凭据、密钥等替换为 `<redacted>`）。原方案是 CKafka CDC + Go 服务，实际只实现了 Go 一次性 init。笔记里的**业务规则**（字段口径、有效性、冲突与缺口处理）仍然有效；snapshot/init/restart/resume/CLI 章节已标注“已被 shuttle 内联投影取代”。

状态说明：**现行**表示内容仍然有效；**部分取代**表示业务规则有效，运行方式已被 100 取代；**存档**表示仅留作历史记录。

| 文档 | 内容 | 状态 |
|---|---|---|
| [100_shuttle-inline-projection](100_shuttle-inline-projection.md) | 新设计：投影框架、生命周期守卫、advisory lock、issues 表、加新表步骤、cutover、已知限制、下一期清单 | 现行 |
| [000001_CKafka-连接器（DataHub）](000001_CKafka-%E8%BF%9E%E6%8E%A5%E5%99%A8%EF%BC%88DataHub%EF%BC%89.md) | 用腾讯云 CKafka 连接器把 PG 变更 CDC 到 Kafka | 存档 |
| [000002_farcaster_db_convertion.excalidraw](000002_farcaster_db_convertion.excalidraw.md) | 旧架构 Excalidraw 草图（CKafka + Go service） | 存档 |
| [000003_表数据处理](000003_%E8%A1%A8%E6%95%B0%E6%8D%AE%E5%A4%84%E7%90%86.md) | 19 张业务表清单、旧 init 完成情况、新实现位置 | 现行 |
| [000004_构建测试环境](000004_%E6%9E%84%E5%BB%BA%E6%B5%8B%E8%AF%95%E7%8E%AF%E5%A2%83.md) | 旧方案的测试环境搭建步骤（数据库、Kafka、DataHub、Go 服务） | 存档 |
| [000005_blocks-转换说明](000005_blocks-%E8%BD%AC%E6%8D%A2%E8%AF%B4%E6%98%8E.md) | blocks 的消息有效性、多消息裁决和字段规则；旧 init 运行方式 | 部分取代 |
| [000006_casts-转换草稿](000006_casts-%E8%BD%AC%E6%8D%A2%E8%8D%89%E7%A8%BF.md) | casts 的字段与转换规则草稿 | 现行 |
| [000007_表依赖关系](000007_%E8%A1%A8%E4%BE%9D%E8%B5%96%E5%85%B3%E7%B3%BB.md) | 19 张表的来源、依赖、实现顺序和源覆盖缺口 | 现行 |
| [000008_fids-分析与实现计划](000008_fids-%E5%88%86%E6%9E%90%E4%B8%8E%E5%AE%9E%E7%8E%B0%E8%AE%A1%E5%88%92.md) | fids 的字段取值、缺失与冲突规则；旧 init 命令与结果 | 部分取代 |
| [000009_未完成与数据缺口](000009_%E6%9C%AA%E5%AE%8C%E6%88%90%E4%B8%8E%E6%95%B0%E6%8D%AE%E7%BC%BA%E5%8F%A3.md) | 各表未完成项和源数据缺口清单 | 现行 |
| [000010_signers-分析与实现计划](000010_signers-%E5%88%86%E6%9E%90%E4%B8%8E%E5%AE%9E%E7%8E%B0%E8%AE%A1%E5%88%92.md) | signers 的 app_fid 解码、事件规则、gasless 范围；旧 init 步骤与结果 | 部分取代 |
| [000011_usernames-分析与实现计划](000011_usernames-%E5%88%86%E6%9E%90%E4%B8%8E%E5%AE%9E%E7%8E%B0%E8%AE%A1%E5%88%92.md) | usernames 的来源、业务主键、冲突处理、Neynar 可行性；旧 init 步骤与结果 | 部分取代 |
| [000012_storage_rentals-分析与实现计划](000012_storage_rentals-%E5%88%86%E6%9E%90%E4%B8%8E%E5%AE%9E%E7%8E%B0%E8%AE%A1%E5%88%92.md) | storage_rentals 的收据身份、字段与缺口规则；旧 init 步骤与结果 | 部分取代 |
| [000013_tier_purchases-分析与实现计划](000013_tier_purchases-%E5%88%86%E6%9E%90%E4%B8%8E%E5%AE%9E%E7%8E%B0%E8%AE%A1%E5%88%92.md) | tier_purchases 的字段与协议依据（Base 链）；旧 init 实施与结果 | 部分取代 |
| [000014_Neynar补齐FID](000014_Neynar%E8%A1%A5%E9%BD%90FID.md) | 用 Neynar 补齐 source 缺注册事件的 4 个 FID | 现行 |
| [000015_Neynar补齐signers](000015_Neynar%E8%A1%A5%E9%BD%90signers.md) | 用 Neynar 补齐只有 REMOVE 的 18 把公钥 | 现行 |
| [000016_signers和custody_address的区别](000016_signers%E5%92%8Ccustody_address%E7%9A%84%E5%8C%BA%E5%88%AB.md) | signer 与 custody 地址的概念区别 | 现行 |
| [000017_usernames转换与reference校验](000017_usernames%E8%BD%AC%E6%8D%A2%E4%B8%8Ereference%E6%A0%A1%E9%AA%8C.md) | usernames 字段规则和与 reference 的对比方法；旧运行命令 | 部分取代 |
