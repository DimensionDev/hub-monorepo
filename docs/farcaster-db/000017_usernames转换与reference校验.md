# usernames 怎么转换、怎么和 reference 对比

> [!NOTE]
> **部分取代**
> 业务规则章节仍有效。snapshot/init/restart/resume/CLI 相关章节已被 shuttle 内联投影取代，见 [100_shuttle-inline-projection](100_shuttle-inline-projection.md)。

Status: implemented-real-validation-pending

这轮按两个步骤执行：先把 source 的用户名证明转换到 target，再拿 target 中实际保存的记录和 reference 比。reference 只提供对照，不能用它的数据生成 target。

## 转换需要什么

现在的转换器读两种原料：

| 原料 | 能提供什么 |
|---|---|
| source.usernames | 已经整理好的用户名证明 |
| source.messages 中 type=12 的消息 | ENS、Basename 的用户名证明 |

这两个来源有一个有数据，就可能生成结果。资料里展示的用户名字符串不够用，因为它不包含完整的所有权证明。普通账号或公钥链上事件也不能替代用户名证明。

转换器按 `(fid, type, username)` 识别一条记录。同一个账号可以有多个名字，不能只保留一个“当前用户名”。同一个名字的重复证明要归并，不会每次运行都新增一行。

## 每个字段怎么来

reference.usernames 实际有 **9 列、3 个索引**，之前文档写成10列，这里已纠正。

| 字段 | 转换规则 | 是否与 reference 的历史值逐字比较 |
|---|---|---|
| id | 按业务身份生成稳定 UUID；已有记录保留原 ID | 否，两边各自生成 |
| created_at | 首次生成目标记录的入库时间；重跑保留 | 否 |
| updated_at | 目标业务字段变化时更新；内容不变则保留 | 否 |
| username | 证明中的名字；消息中的十六进制字节按 UTF-8 解码 | 是，作为匹配条件 |
| fid | 证明属于哪个账号 | 是，作为匹配条件 |
| type | 1 为 FNAME，2 为 ENS，3 为 Basename | 是，作为匹配条件 |
| custody_address | 用户名证明的 owner；不是账号当前的 custody 地址 | 是 |
| proof_timestamp | 证明自身的时间，统一 UTC；不拿入库时间代替 | 是；不同时间另列为证明版本不同 |
| deleted_at | 来源明确给出的失效时间；消息的删除、剪枝、撤销时间取最早者 | 是，包括是否为 NULL |

缺少必填字段或归属冲突时，暂缓生成这条记录并记录原因。owner 缺失时允许写 NULL，但仍保留字段缺口。来源没有给出失效依据时，不编造取消或转移时间。

已有的 restart/resume 不变：restart 读取一轮新的 source 快照；resume 继续上次已经保存的快照。想处理后来采集到的新证明，需要 restart。旧证明不能覆盖已经保存的较新结果。

## 怎么运行

> [!WARNING]
> **已被 shuttle 内联投影取代（见 [100](100_shuttle-inline-projection.md)）**
> 本节描述 Go 一次性 snapshot/init（restart/resume/CLI）的做法，仅作存档；增量现在由 shuttle 在同一事务里内联投影。业务规则以本文其他章节为准。

先构建：

```sh
go build -o bin/farcaster-init ./cmd/farcaster-init
```

再转换：

```sh
bin/farcaster-init init usernames --env-file .env.readonly --apply --restart
```

命令会返回 run_id。拿这个任务作为校验范围：

```sh
bin/farcaster-init verify usernames --env-file .env.readonly \
  --run-id RUN_ID \
  --report reports/usernames-verification.json
```

校验先检查 target 的字段、约束和索引是否与 reference 一致，再检查业务记录。它只读 target 和 reference，不连接 source，也不修改任何业务行。

指定 run_id 时，用这次任务捕获到的 FID 确定范围，包括处理过但没生成业务行的 FID。任务必须已完成。报告也会保留该任务的待处理、字段缺失数量。**它读取的是当前 target，不是把 target 还原到任务完成时的历史状态。**

也可以单独查几个账号：

```sh
bin/farcaster-init verify usernames --env-file .env.readonly \
  --fids 3,347,9152,897299 \
  --report reports/usernames-selected-fids.json
```

`--run-id` 和 `--fids` 二选一。两者都不指定时，只检查 target.usernames 已有记录涉及的 FID，不代表检查过整个 reference。每次取一批 FID，默认500个；`--batch-size` 可以调整。

## 报告怎么看

| 结果 | 含义 |
|---|---|
| matched | 本次范围中有可比结果，业务字段一致，也没有阻碍判断的缺口 |
| different | 存在缺行、多行、地址或删除状态等差异；看详情中的两边原值 |
| pending | 空结果、没有可比记录、证明版本不同、记录归属冲突或初始化字段缺口，尚不能判定通过 |

退出码分别是0、1、2；执行错误也返回1，但不会输出完成的校验报告。

`target_missing` 表示 reference 有这条记录、target 没有；`reference_missing` 表示 target 有、reference 没有。匹配的名字、FID、类型相同时，会列出不同的字段及两边的值。相同业务身份对应多个互相矛盾的记录时，报告歧义，不随意选择一条。

证明时间不同，报告 `proof_version_difference`。这说明两边用的证明不是同一版本，不能仅凭这个结果认定转换算法出错。两边分别用只读快照，记录 UTC 查询时间；两个数据库无法保证同一瞬间的全局快照，活动状态变化也需要结合采集时间确认。

报告默认最多展示1000条差异详情，计数不受这个限制；`--detail-limit` 可以调整。没有列入详情的数量在 details_omitted 中。reference 中业务字段完全相同的重复记录按一条证明比较，并单独统计重复数量；target 出现重复业务行会判为差异。

**0行不算通过。** 即使差异计数为0，没有可比记录也只能标记待验证。

## 本次真实结果

2026-11-17 02:39—02:42 UTC（本地11月16日）：

| 检查项 | 实际结果 |
|---|---:|
| source.usernames 的证明 | 0 |
| source.messages.type=12 的证明 | 0 |
| 本次捕获的证明 | 0 |
| 本次 target 新增/修改的用户名 | 0 / 0 |
| target/reference 字段、约束、索引 | 一致，9列、3索引 |
| 另查4个FID：reference 记录 | 7 |
| 另查4个FID：target 记录 | 0 |
| 真实转换一致性验证 | 待完成，缺少原始证明 |

初始化任务：`ce22a0a9-0178-4be3-82c3-eb0c95353bff`，已完成，empty_source=true。空任务校验没有把 reference 全库算作0行；它的任务范围本来就是空。四个FID的单独对照显示了真正存在的7条 reference 记录：

| FID | reference 的用户名 | target |
|---|---|---|
| 3 | dwr、danromero.eth、dwr.eth | 无 |
| 347 | greg、gregskril.eth | 无 |
| 9152 | warpcast | 无 |
| 897299 | inabahoor | 无 |

当前代码可以完成转换和对照，但本次真实数据不能证明转换结果已与 reference 一致。待 source 有证明后重新 restart，再用新 run_id 校验。

检查依据保存在本地 reports 目录（不提交数据库报告）：usernames-before-conversion.json、usernames-conversion-run.json、usernames-target-reference-run.json、usernames-target-reference-fids.json。

## 代码检查边界

> [!WARNING]
> **已被 shuttle 内联投影取代（见 [100](100_shuttle-inline-projection.md)）**
> 本节描述 Go 一次性 snapshot/init（restart/resume/CLI）的做法，仅作存档；增量现在由 shuttle 在同一事务里内联投影。业务规则以本文其他章节为准。

复用现有 Go 命令入口，在独立测试数据库里从原始证明执行初始化，再读取它实际生成的 target 记录与独立 reference 对照。检查空结果、同账号多个名字、UTF-8、NULL地址、软删除、字段差异、不同证明版本、reference冲突、任务范围、分页和只读行为。没有增加固定JSON验收样例，也没有用真实reference记录充当转换输入。

这些回归检查验证命令行为，不能替代上面的真实数据验收。

本轮本地检查：`TEST_DATABASE_URL=... go test -race ./... -count=1` 和 `go vet ./...` 已通过。使用独立测试库，真实source/reference始终只读。真实结果文件位于项目的reports目录：usernames-before-conversion.json、usernames-conversion-run.json、usernames-target-reference-run.json、usernames-target-reference-fids.json。

两路审查已完成：规范审查没有发现问题；需求审查发现“冲突掩盖缺行”的问题，已修复并补充命令回归、通过复审。项目内审查记录位于.scratch/init-usernames/review.md。
