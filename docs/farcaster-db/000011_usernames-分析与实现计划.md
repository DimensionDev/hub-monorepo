---
title: usernames 分析与实现计划
tags:
  - 主题/数据同步
  - 技术/架构设计
updated: 2026-11-16
---

# usernames 分析与实现计划

> [!NOTE]
> **部分取代**
> 业务规则章节仍有效。snapshot/init/restart/resume/CLI 相关章节已被 shuttle 内联投影取代，见 [100_shuttle-inline-projection](100_shuttle-inline-projection.md)。

Status: implemented-real-validation-pending

## 已确认的来源与结构

reference.usernames 有9列、3个索引，只有id主键，没有username或fid唯一约束。fid为integer，type为smallint，custody_address与deleted_at可空，proof_timestamp必填。

同一FID可以有多条有效证明。例如reference的FID 3同时保存danromero.eth与dwr.eth两条ENS证明，以及dwr的FNAME证明。因此不能按每个FID只留一个username，也不能用profiles的首选username替代证明表。

读取两个来源：source.usernames的结构化证明，以及messages.type=12的用户名证明。Shuttle body.name、owner是0x十六进制，name应按UTF-8解码；body.timestamp已经是Unix秒。协议校验要求消息中的fid和证明fid相同，证明时间与已转换的messages.timestamp相同；gossiped消息只支持ENS、Basename，FNAME来自独立证明来源。

当前source.usernames为空；抽查FID 3、347、9152、3358486未见type=12消息，不能由此断言整个messages没有证明。初始化会按实际快照报告两个来源的数量，零结果表示采集覆盖不足，不表示网络没有用户名。

## 业务主键与字段

按(fid,type,username)维护一条目标记录；同一用户的多个ENS名字互不覆盖。source UUID只决定捕获顺序，归并用proof_timestamp与来源状态证据。

- username：证明中的原始名称，消息来源从UTF-8字节解码，不存十六进制，不随意改大小写。
- fid、type：证明的归属用户与类型，检查目标列范围；目前支持1 FNAME、2 ENS、3 Basename。
- custody_address：证明的owner或source.usernames.custody_address，代表证明当时的地址；不能用fids里的当前custody覆盖。
- proof_timestamp：证明时间UTC，不能用消息入库时间替代，也不能重复加Farcaster epoch。
- deleted_at：来源明确给出的删除/失效时间。消息deleted_at、pruned_at、revoked_at取最早失效时间。
- id：稳定UUID，已有目标记录保留id和created_at；业务字段不变时不更新updated_at。

## 缺失和冲突处理

缺少或非法的必填字段，记录source_id、fid、可用的名称/类型和missing_fields，不创建假证明。缺少owner时可以写nullable custody_address=NULL，但要记录字段缺口；明确零地址保留原字节。

同一证明时间有多个来源时，内容一致可合并；同来源的状态修正使用source.updated_at。跨来源的owner或删除状态互相矛盾且无法确定权威状态时，记录冲突字段并暂缓该记录，不用UUID或随意来源优先级解决。

同一FNAME名称在快照中有多个有效归属FID时，记录冲突，不推测旧owner的删除时间；FID=0的清除/控制记录单独记录，缺少完整转移语义时不创建FID=0账号证明。这里的限制进入缺口文档，不能宣称完整实现FNAME转移历史。

旧proof时间或同proof时间但更旧的源状态不能覆盖更晚投影。没有出现在这一轮source里的其他目标名字不会被全表删除。

## 实施步骤

> [!WARNING]
> **已被 shuttle 内联投影取代（见 [100](100_shuttle-inline-projection.md)）**
> 本节描述 Go 一次性 snapshot/init（restart/resume/CLI）的做法，仅作存档；增量现在由 shuttle 在同一事务里内联投影。业务规则以本文其他章节为准。

1. 通过Go命令与独立测试库，先验证结构复制和一个结构化FNAME证明；再加入type=12的ENS/Basename解码和主网校验。
2. 复用internal/initrun快照生命周期、独立锁和进度。将已有主网protobuf校验提取为共享能力，保持blocks原行为。
3. 按FID读取完整快照后，以(type,username)归并，批量读取目标与保存的证明依据，流水线写入。
4. 保存逐证明的字段缺口；必填字段未知或有效性冲突的记录pending，nullable owner未知的记录partial，两者可区分查询。
5. 提供schema usernames、init usernames的预览、restart与resume，保留旧数据和幂等行为。
6. 覆盖同FID多个ENS名、同名不同类型、消息UTF-8解码、时间单位、缺失owner、失效状态、冲突、旧快照保护、失败回滚与离线恢复。
7. 检查真实source覆盖；真实数据不足时明确报告0或实际数量，不复制reference用户名充数。完成审查和适当验证后提交。

## 验证和依赖

无需等待target.fids或profiles齐备。沿用既定CLI+隔离数据库测试边界，真实source/reference只读。逐表计划、实际覆盖和无法实现的字段写入实施进度与缺口清单。

依据：Shuttle的004_usernames迁移、UsernameProofReconciliation、utils中的USERNAME_PROOF转换，以及core/validations.ts的validateUsernameProofBody。

## 当前实施结果

> [!WARNING]
> **已被 shuttle 内联投影取代（见 [100](100_shuttle-inline-projection.md)）**
> 本节描述 Go 一次性 snapshot/init（restart/resume/CLI）的做法，仅作存档；增量现在由 shuttle 在同一事务里内联投影。业务规则以本文其他章节为准。

已实现证明表与消息两个来源的转换、字段缺口/冲突记录、持久快照和restart/resume。有效标记决定当前证明，保留全历史最高proof水位防止旧快照回滚。共享主网检查已提取为messagewire，原blocks行为通过回归。

隔离用例覆盖UTF-8、Unix时间、同FID多个ENS名、nullable owner缺口、跨来源owner冲突、FNAME多归属、软删除恢复、失败原子回滚和离线resume、非法owner与主网隔离。完整Go race suite及vet已通过。

真实完整预览：结构化证明0条，type=12消息0条。target已按reference创建9字段、3索引的空表；初始化将记录empty_source=true。真实抽查待证明采集数据齐备，不复制reference记录、不以人工数据充当真实验证。代码审查已完成，选中证明独立版本判断已修复并通过复审。

审查修正：整组最高proof水位仅用于判断历史是否变旧；实际选中证明的Time/Updated从持久basis单独比较，失效历史的较新更新时间不能掩盖有效证明回退。新增回归先失败后通过。

真实空快照任务：d4e08e70-6d52-4e39-a6b2-9fc5899c04b4，0条输入、0条目标数据，empty_source=true。真实完成任务resume曾因reference连接超时未执行完成，需恢复连接后补核；隔离环境中的完成/失败resume已通过。

## 2026-11-16：source复查与Neynar可行性

2026-11-16，只读核查。没有写source、reference或target，也没有实现/执行补齐程序。

## source现在有什么

23:06 UTC全量查询：source.usernames为0行，messages.type=12的用户名证明也为0条，target.usernames仍为0行。现有初始化任务 d4e08e70-6d52-4e39-a6b2-9fc5899c04b4 是空快照，不能靠resume读取后来出现的数据。

23:11 UTC再次统计消息范围：source.messages涉及76,459个FID；其中用户名资料消息（messages.type=11、body.type=6）共69,318条，69,312个FID有有效且非空的用户名资料。数量会随采集继续增长。

用户名字符串已经有了，但这类资料消息没有用户名证明的owner和证明时间。资料更新时间不能填成proof_timestamp；当前账号托管地址也不能冒充证明当时的owner。因此现有转换器保持空结果是符合规则的。

## Neynar实际能取到吗

接口为 https://snapchain-api.neynar.com/v1/userNameProofsByFid?fid=... ，用现有本地密钥查询，固定官方HTTPS主机、禁重定向、并发最多2。已完整核对target.fids的247个FID，结果为：

| 结果 | 数量 |
| --- | ---: |
| 成功查询FID | 247 |
| 有用户名证明的FID | 186 |
| 返回空证明列表的FID | 61 |
| 查询失败 | 0 |
| 原始返回证明 | 370 |
| 去重后的证明身份 | 186 |
| 完全相同的重复项 | 184 |

这186条全为FNAME，名称、FID、owner、Unix证明时间和签名字节均满足必要格式检查。没有发现同一身份内容冲突或同名FNAME跨FID冲突；抽取10个名称调用userNameProofByName反查，FID、name、type、owner、timestamp均一致。这里没有声称完成签名密码学验证。

示例：

| FID | 用户名 | 类型 |
| --- | --- | --- |
| 897299 | inabahoor | FNAME |
| 1069937 | xafmd | FNAME |
| 3346614 | assay-payers | FNAME |
| 3351620 | assay | FNAME |
| 3358486 | ledlight | FNAME |
| 3358728 | pixelbark | FNAME |

61个返回空列表的FID，在source中也未找到有效的非空首选用户名资料。空列表不等于账号一定缺记录，也不能证明账号从未持有用户名，先记录“未返回证明”。

额外对照查询了FID 3、347和9152，它们不计入上面的247范围。FID 3去重后有dwr、dwr.eth、danromero.eth三条证明，FID 347有greg与gregskril.eth，说明必须保留一人多个用户名，ENS也能查到。接口返回的重复证明需要去重。

## 哪些字段能补，哪些仍不能

| target字段 | 依据或限制 |
| --- | --- |
| fid | proof.fid，检查目标integer范围 |
| username | proof.name，保留原始UTF-8名称；Neynar接口的name是明文，不套用Shuttle的hex解码 |
| type | FNAME=1、ENS_L1=2；Basename=3沿用既有规则，但本次没有获得Basename样本 |
| custody_address | proof.owner的20字节地址，不能改用账号当前custody地址 |
| proof_timestamp | proof.timestamp是Unix秒，转换UTC，不再加Farcaster epoch |
| id、created_at、updated_at | 复用现有稳定ID和导入时间规则，不冒充证明时间 |
| deleted_at | 这两个证明接口不提供完整删除/转移时间，不能据此恢复所有旧用户名历史 |

结论：Neynar已证明可以提供可用于当前证明补齐的数据；仅靠这两个接口不能保证历史删除/转移记录与reference完全一致。没有返回某个名字时，不能推测它的删除时间。

## 正式实施应怎样定范围

本次247个FID只是接口可行性核查范围。source消息已有76,459个FID，usernames实现也不依赖target.fids齐备，不能只给这247个FID补就宣布完成全量。

后续应从source消息所涉及的FID发现候选，合并已知账号范围，冻结任务清单后按FID分页，支持断点继续；再拉取每个FID的全部用户名证明。source的证明表为空，因此不能只从usernames_issues出发，那样会一直没有待处理记录。

按(fid,type,username)识别业务行。完全相同的重复证明跳过；冲突、无证明和请求失败分别记录，不猜字段、不伪造软删除。业务行、水位和Neynar来源审计同事务，只写target；按用户既定要求增加neynar_backfilled来源标记。保留同FID的多个ENS名。

完整历史恢复另行确定来源与转换规则；当前证明补齐与历史完整性分开验收。

## 证据位置

- reports/usernames-source-check.json：来源与目标结构、证明数量、原初始化任务。
- reports/usernames-source-userdata-check.json：资料消息分类与样例。
- reports/usernames-candidate-scope.json：source实际FID范围与用户名资料计数。
- reports/usernames-neynar-discovery.json：12个target样本及3个对照FID原始证明。
- reports/usernames-neynar-all-target-fids.json：247个FID的全部响应。
- reports/usernames-neynar-validation.json：必要字段、重复冲突及10个名称反查结果。
- reports/usernames-neynar-empty-source-names.json：61个空结果FID的source用户名资料复查，当前为空。

接口文档：[按FID读取证明](https://docs.neynar.com/reference/fetch-username-proofs-by-fid)、[按名称读取证明](https://docs.neynar.com/reference/fetch-username-proof-by-name)。

## 2026-11-16：用户名也有专门的节点事件

数据库的onchain_events和节点订阅事件是两回事。

只读检查source.onchain_events，目前有公钥事件885条、账号事件308条、存储租赁事件474条；没有用户名证明事件或对应字段。source中也没有保存完整HubEvent的业务事件表。检查证据：reports/usernames-event-check.json。

但Snapchain的节点事件流有 `HUB_EVENT_TYPE_MERGE_USERNAME_PROOF=6`，其内容可带新用户名证明、被替换的旧证明，以及相应证明消息。它属于HubEvent，不能在OnChainEvent类型里找。节点事件文档说明事件仅保留3天，不能假定订阅事件能恢复所有更久历史。

核对用户提供的DimensionDev/hub-monorepo main：

- hubSubscriber.ts的默认订阅列表包含MERGE_USERNAME_PROOF。
- 示例App.onHubEvent只处理MERGE_ON_CHAIN_EVENT并写onchain_events，没有用户名证明落库分支。
- 通用HubEventProcessor在调用App回调后只处理消息merge/revoke/prune。
- 示例App.reconcileFids只使用MessageReconciliation；UsernameProofReconciliation虽然已实现，但没有接入该App的backfill路径。

因此，如果线上使用的是这个示例App，用户名表为空有明确的代码原因；不能只等现有消息backfill跑完。无Neynar方案是先在Shuttle接上用户名证明事件落库，并单独接入节点getUserNameProofsByFid的证明核对/补采。是否与线上部署完全相同，仍需核对实际部署版本；本次未修改Shuttle或source。

依据：[Snapchain事件类型](https://snapchain.farcaster.xyz/reference/datatypes/events)、[订阅类型](https://github.com/DimensionDev/hub-monorepo/blob/main/packages/shuttle/src/shuttle/hubSubscriber.ts#L46-L52)、[示例App事件处理](https://github.com/DimensionDev/hub-monorepo/blob/main/packages/shuttle/src/example-app/app.ts#L114-L179)、[示例App补采](https://github.com/DimensionDev/hub-monorepo/blob/main/packages/shuttle/src/example-app/app.ts#L240-L266)、[证明核对工具](https://github.com/DimensionDev/hub-monorepo/blob/main/packages/shuttle/src/shuttle/usernameProofReconciliation.ts)。

## 用户确认：Shuttle保存原始事件，转换器读取事件

用户希望Shuttle只保存原始用户名证明事件，由本项目从source.onchain_events读取并生成target.usernames，无需先生成source.usernames业务表。

已只读核实当前onchain_events：chain_id、block_timestamp、block_number、log_index、type、tx_hash、fid和body均为NOT NULL。用户名证明属于HubEvent，没有链上交易哈希、链ID和链上日志序号，因此当前结构不能直接保存，不能编造链ID、交易哈希或日志序号来满足约束。

若保留用户指定的onchain_events路径，须先扩展表结构，使其明确区分链上事件与用户名证明事件、保留原始HubEvent身份/类型及完整内容，并允许不适用的链上字段为空，保持已有链上记录的约束语义及转换行为。HubEvent类型编号和OnChainEvent类型编号不得混为一个命名空间。实时写入和历史证明补采均需处理，不能只接新事件便认为历史已齐。

本次只核实结构并记录方向，未修改Shuttle或source数据库。结构证据：reports/usernames-onchain-storage-check.json。


## 2026-11-16 实施更新

> [!WARNING]
> **已被 shuttle 内联投影取代（见 [100](100_shuttle-inline-projection.md)）**
> 本节描述 Go 一次性 snapshot/init（restart/resume/CLI）的做法，仅作存档；增量现在由 shuttle 在同一事务里内联投影。业务规则以本文其他章节为准。

校验流程以《000017_usernames转换与reference校验》为准：先转换到target，再读取target和reference比较。真实source的两类用户名证明仍均为0，已运行一轮空初始化，真实一致性验证待证明数据齐备。reference结构复查为9列、3个索引。
