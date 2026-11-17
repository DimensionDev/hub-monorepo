# Shuttle

服务器 `/opt/shuttle` 运行两个 shard 的增量同步。本地保留源码、构建和开发工具。

## 数据库

当前启动迁移创建：

- `messages`
- `onchain_events`
- `blocks`（屏蔽关系）
- `shuttle_message_state`（被投影消息的生命周期版本，迁移 006 由 `shuttle_block_message_state` 改名而来）
- `shuttle_projection_issues`（投影遇到的数据缺口，例如非 mainnet 消息；重放不重复记录）
- `kysely_migration`、`kysely_migration_lock`（迁移元数据）

`pgcrypto` 扩展和 `generate_ulid()` 函数用于生成主键。`casts`、`usernames` 的示例迁移不在启动迁移列表内，消息处理也不写入 casts 投影。可选用户名重建器的测试会在独立测试数据库中创建自己的测试表。

### 业务投影框架

设计文档见 [docs/farcaster-db](docs/farcaster-db/README.md)，新架构见 `100_shuttle-inline-projection.md`。Shuttle 消费现有 Redis Stream（`hub:<HUB_HOST>:evt:msg:<shard>`），在写入原始消息的同一事务里直接更新业务表，不经过 Kafka。

- `packages/shuttle/src/example-app/projections/`：`ProjectionDispatcher` 负责按 FID 加锁（所有表共用一个锁命名空间）、生命周期守卫、原始落库，然后调用已注册的投影；目前只注册了 `blocks`。没有投影关心的事件仍走原 `HubEventProcessor`。
- 生命周期守卫：hub 事件按 `event_id` 防重放；backfill（reconcile 补进来的消息）只允许插入，永远不会复活已删除、prune 或 revoke 的消息。
- 投影从 `messages` 重算，active 关系与到达顺序无关。墓碑行与顺序有关：REMOVE 先于 ADD 到达时不补删除行，读取方以 `deleted_at IS NULL` 为准。
- 可预期的数据问题写入 `shuttle_projection_issues` 并跳过该消息，不阻塞事件流。
- `onchain_events` 以 `(chain_id, block_number, log_index)` 唯一，重放不会产生重复行。迁移 006 删除内容完全相同的历史重复；如果存在内容不同的重复会停止，需要先人工处理：

```sql
SELECT chain_id, block_number, log_index, count(*), count(DISTINCT (type, fid, tx_hash, body::text))
FROM onchain_events GROUP BY 1, 2, 3 HAVING count(*) > 1;
```

- Redis Stream：同一事件投递超过 `STREAM_MAX_DELIVERIES` 次（默认 5）后移入 `<streamKey>:dlq` 并 ack；定期裁剪不会删除未处理或未投递的事件。

### blocks 增量转换

Shuttle 直接在 `POSTGRES_URL` 指定的数据库、`POSTGRES_SCHEMA` 指定的 schema 内生成屏蔽关系。仅处理 mainnet 的 block Link 添加、取消、compact、prune 和 revoke。原始消息、生命周期版本和业务行在同一事务中提交；失败保留 Redis 待处理事件供重试。

普通事件只更新其影响的用户对，可读取该用户已有 block 消息判断冲突。compact 表达整个屏蔽集合，按节点的 `deletedMessages` 标记旧消息失效，再归并集合。首选协议时间、同时间 remove 优先，再按 hash 排序；`displayTimestamp` 只用于业务展示时间。无时区业务列统一按 UTC 读写。

取消使用软删除；不存在的关系不补造删除行；再次屏蔽复用原 ID。新 ID 与 Go 初始化器使用相同的 UUID v5 规则。没有实际变化的业务行保留原更新时间。

迁移保持 reference 的七列和四个索引，不增加业务键唯一约束。已存在的兼容表保留数据并补建缺失索引，结构或已有索引不兼容时停止。处理事件时按 schema/FID 加事务锁，遇到既有重复用户对报错。同一 hash 的生命周期事件版本防止旧事件重试恢复新状态；同一来源的 shard 布局变更会显式报错，需要单独迁移版本依据。

本轮不执行全库历史重建，也不接入其他业务表。节点没有提供或旧采集阶段已经遗漏的历史无法自动补全，业务表行数不代表全网完整关系数。不要让旧 Go 初始化器并发写入这里的 `blocks`，它没有遵守增量处理器的锁和版本规则。

## 服务端

Dockerfile 在本仓库内完整构建（core → hub-nodejs → shuttle），库代码和 example-app 的修改都会进入镜像。Compose 默认只运行 `shuttle-1`、`shuttle-2`。

```bash
cd /opt/shuttle
sudo docker compose build shuttle-1
sudo docker compose up -d --no-deps --force-recreate shuttle-1 shuttle-2
sudo docker compose logs --tail=50 shuttle-1 shuttle-2
```

仅在首次启动空 Redis、且只需要从当前时刻同步时，运行一次 `sudo ./scripts/seed-last-event.sh`。日常重启不要再次运行，以免跳过停机期间的事件。

`shuttle-watchdog.timer` 保持停用。worker 平时不启动，只在按下文步骤执行 backfill 时临时运行。升级后由两个现有 shard 进程执行启动迁移和 blocks 转换；保留原 Redis 订阅位置，不重新 seed。当前代码修改不会自动部署到服务器。

### 部署新版本并用 backfill 补 blocks

1. 部署前检查 `onchain_events` 有没有内容不同的重复行（见上文 SQL）。第二列都等于 1 才能继续。
2. 更新 `/opt/shuttle` 到新提交后重建镜像，再重启两个 shard 进程。迁移 006 由 shard 进程在启动时执行，`backfill` 命令不跑迁移，所以必须先完成这一步：

```bash
cd /opt/shuttle
sudo docker compose build shuttle-1
sudo docker compose up -d --no-deps --force-recreate shuttle-1 shuttle-2
sudo docker compose logs --tail=50 shuttle-1 shuttle-2 | grep -i migration
```

3. 先用少量 fid 试跑只补投影的 backfill。`BACKFILL_PROJECTIONS_ONLY=true` 表示只向节点拉取投影需要的消息族（blocks 对应 link 消息和 compact），没有投影关心的消息（例如 follow）不写库，每个 fid 在一个事务里只重算一次：

```bash
sudo docker compose run --rm -e BACKFILL_PROJECTIONS_ONLY=true -e FIDS=3,2,1 shuttle-worker yarn start backfill
```

日志出现 `Reconciliation started at ... complete at ...` 后按 Ctrl-C 停止（worker 不会自己退出）。有 block 的 fid 会打出 `Backfilled projected messages`。然后查结果：

```sql
SELECT count(*) AS total, count(*) FILTER (WHERE deleted_at IS NULL) AS active FROM blocks;
SELECT projection, reason, count(*) FROM shuttle_projection_issues GROUP BY 1, 2;
```

4. 确认无误后去掉 `FIDS` 跑全量，会排队到节点当前注册数为止，每批 10 个 fid，`CONCURRENCY` 控制并发。节点仍然要为每个 fid 返回全部 link 消息，所以会跑较久，但只写 block 相关的行。中断后可以重跑：已存在的消息会被跳过，结果不变。

backfill 只能补回节点现在还保留的消息，早年已被 prune 的 block 无法恢复。

## 本地开发

使用 Node 24。依赖和 core、hub-nodejs、shuttle 构建产物已准备好。

```bash
# 独立终端中保持 SSH 通道运行
python3 scripts/dev-tunnel.py

# 验证数据库、Redis、Snapchain 连接，只读操作
node --env-file=.env.development scripts/dev-check.mjs

# 重新安装依赖
npm exec --yes --package=yarn@1.22.19 -- yarn install --frozen-lockfile

# 按依赖顺序构建
npm exec --yes --package=yarn@1.22.19 --call 'yarn --cwd packages/core build && yarn --cwd packages/hub-nodejs build && yarn --cwd packages/shuttle build'
```

`.env` 保留服务端连接配置，`.env.development` 使用本机 SSH 转发端口，两者均不提交 Git。默认端口：PostgreSQL `15432`、Redis `26379`、Snapchain gRPC `23383`、HTTP `23381`。可通过 `SHUTTLE_SSH_HOST`、`SHUTTLE_SSH_IDENTITY` 覆盖 SSH 目标和私钥路径。

本地不启动第二组 shard 订阅进程；业务处理代码修改并验证后，再构建和更新服务器镜像。

## 测试

Shuttle 集成测试会重建 `shuttle_test` 数据库并清空测试 Redis。只使用隔离的本地测试实例，禁止使用服务器 `.env` 或 `.env.development` 运行测试。

blocks 和投影框架集成测试（`src/blocks.integration.test.ts`、`src/projections.integration.test.ts`）只使用显式的 `TEST_DATABASE_URL`，为每次测试创建并删除随机命名的独立数据库，不读取应用连接配置；未指定时跳过。测试账号需要 CREATEDB 权限。

```bash
cd packages/shuttle
TEST_DATABASE_URL='postgres://shuttle@127.0.0.1:55441/postgres' \
  NODE_ENV=test NODE_OPTIONS=--experimental-vm-modules \
  ../../node_modules/.bin/jest --runTestsByPath src/blocks.integration.test.ts src/projections.integration.test.ts --runInBand
```
