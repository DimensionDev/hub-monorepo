# 从 Neynar 补齐 FID

2026-11-16

原来的四个账号在 source 中缺少注册事件。现在可以从 Neynar 查询账号事件，把账号补到 target.fids。source 保持只读；旧初始化任务保留当时的缺口记录。

| FID | Neynar 返回的账号事件 | target 状态 |
| --- | --- | --- |
| 897299 | 注册、修改恢复地址 | 已补齐 |
| 1069937 | 注册、修改恢复地址 | 已补齐 |
| 3346614 | 注册、转移 | 已补齐 |
| 3351620 | 注册、转移 | 已补齐 |

target.fids 从243条变为247条。四条新账号的托管地址、恢复地址、注册时间和业务时间已逐字段核对；其他243条账号和原有索引不变。再次运行新增0条，四条结果均为 unchanged。

## 来源标记

target.fids 增加 neynar_backfilled 字段，类型 boolean，默认 false。通过本功能新增的账号设为 true，以后普通初始化保留这个标记，表示该账号曾经通过 Neynar 补齐。

这是用户要求的 target 扩展字段，reference 不增加该字段。结构检查明确允许该扩展，其余字段、约束和索引仍按 reference 检查。

完整账号事件和补齐时间保存在 target 的 conversion_state.fids_neynar_repairs，账号水位沿用 conversion_state.fids。旧 fids_events 和 fids_applied 不改写，所以旧任务仍会记着当时的四条缺口，网页另用“target 状态”显示当前是否已补齐。

## 网页操作

1. 打开 http://127.0.0.1:8765/?table=fids 。需要时先搜索 FID 或筛选缺失字段。
2. 点击“从 Neynar 补齐”。“预览本页”只查询当前页，展示账号字段和事件。
3. 批量补齐针对当前筛选的全部缺口。选择每批数量后开始，每个账号独立提交。已有 target 账号直接跳过，不请求 Neynar。
4. 页面显示已处理/总数、新增、跳过、失败和最近FID。单个失败保留原因，其余继续；完整失败记录可以导出。
5. 可以暂停、继续。关闭窗口或刷新页面不取消后台任务。网页服务重启后显示“已暂停”，点击继续即可。

任务只使用已经完成的初始化任务作为范围，每批读取1至100个FID，避免一次把全部缺口放进内存。每条结束后原子保存进度，网页只显示最近20条错误，完整错误保存在 JSONL 文件。

Neynar 限流或密钥失效时暂停，检查后继续。“继续”从保存位置往后处理；已经记为失败的账号需要在同一筛选下新建任务来重试，已成功的账号会跳过。

进度文件是项目目录下的 reports/neynar-fids-job.json。完整错误日志文件名包含任务ID。不要在任务执行期间删除这些文件。

## 命令行

在项目目录运行：

```sh
# 默认预览原来的四个 FID
sh scripts/backfill_fids_neynar.sh

# 执行补齐并保存结果
sh scripts/backfill_fids_neynar.sh --apply --report reports/neynar-fids-repair.json

# 指定其他 FID，每次最多100个
sh scripts/backfill_fids_neynar.sh --fids 123,456
```

脚本和网页共用 Go 实现。数据库配置用 .env.readonly，密钥用 .env.neynar 中的 <redacted>。密钥保留在服务端，不放进网页和命令参数。

## 验证

隔离数据库验证了事务失败整体回滚、重复执行不改ID和时间、来源标记、普通初始化兼容，以及23条记录跨3批处理、已有账号跳过、单个失败继续、暂停和重启恢复。真实四条账号的验证报告在项目 reports/neynar-fids-verification.json。


## 补齐后复核与页面口径

重新从 Neynar 获取事件后，四条账号的托管地址、恢复地址、注册时间、业务时间、来源标记、事件依据和账号水位全部通过；没有重复账号，其他243条及索引保持原样。完整复核报告在项目 reports/neynar-fids-recheck.json 和同名CSV。

FID页已将“source复查”改为“target补齐情况”，当前为仍缺账号0、Neynar已补齐4。按钮和列表按当前target核对；原初始化缺口只保留历史记录，不再让source缺REGISTER影响target补齐结果的显示。
