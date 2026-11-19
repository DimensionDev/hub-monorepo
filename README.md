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

`pgcrypto` 扩展和 `generate_ulid()` 函数用于生成主键。业务表（含 `usernames`、`casts` 等）由迁移 008 按 reference 结构建好；旧的 `casts` 示例迁移和 username proof 原始表（`username_proofs`，迁移 004）不在启动迁移列表内，消息处理也不写入 casts 投影。可选用户名重建器的测试会在独立测试数据库中创建自己的测试表。

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

> **警告：原始数据先入库、投影后上线时，这些数据不会再被分发给新投影。** reconcile（backfill）只处理库里缺失的消息，库里已有的消息不会交给 dispatcher。实时流从部署那一刻起就在写 `messages`、`onchain_events`，所以新投影上线前已经入库的数据，只能用下面的 `rebuild` 命令补投影。
>
> **不要跑默认的全量 backfill**（不设 `BACKFILL_PROJECTIONS_ONLY=true`）。它会把全网所有消息写进 `messages`，但不经过尚未上线的投影，等于把上面的缺口扩大到全部历史。只补投影的 backfill 不受影响：它跳过的消息不入库，以后上线新投影时仍会被当作缺失消息拉回来。

#### 新增一张业务表的上线顺序

1. 注册投影（实现 `Projection` 的 `apply`、`rebuildFids`、`rebuild`），部署。从部署时刻起，实时事件开始投影。
2. 用库里已有的原始数据重建这个投影：

```bash
sudo docker compose run --rm -e REBUILD_PROJECTIONS=<name> shuttle-worker pnpm start rebuild
```

   - `REBUILD_PROJECTIONS` 是逗号分隔的投影名，不设时重建全部已注册的投影；`FIDS` 可以限定范围，用于试跑。
   - 每个 fid 一个事务，持有与实时处理相同的 fid 锁，可以在 shard 运行时执行。只读原始表、只写业务表，不修改 `messages` 和生命周期状态，可以重复执行，结果不变。
   - 以 `Rebuild finished` 日志为结束标志，命令会自己退出。某个 fid 的数据无效时（例如历史上存过的非 mainnet 消息），该 fid 回滚、记日志并跳过，其余继续，最后退出码为 1。
   - 命令不执行迁移；有未执行的迁移时直接拒绝运行，需要先部署 shard。
3. 跑只补投影的 backfill（见下文），补齐节点上有、库里没有的部分。

### blocks 增量转换

Shuttle 直接在 `POSTGRES_URL` 指定的数据库、`POSTGRES_SCHEMA` 指定的 schema 内生成屏蔽关系。仅处理 mainnet 的 block Link 添加、取消、compact、prune 和 revoke。原始消息、生命周期版本和业务行在同一事务中提交；失败保留 Redis 待处理事件供重试。

普通事件只更新其影响的用户对，可读取该用户已有 block 消息判断冲突。compact 表达整个屏蔽集合，按节点的 `deletedMessages` 标记旧消息失效，再归并集合。首选协议时间、同时间 remove 优先，再按 hash 排序；`displayTimestamp` 只用于业务展示时间。无时区业务列统一按 UTC 读写。

取消使用软删除；不存在的关系不补造删除行；再次屏蔽复用原 ID。新 ID 与 Go 初始化器使用相同的 UUID v5 规则。没有实际变化的业务行保留原更新时间。

迁移保持 reference 的七列和四个索引，不增加业务键唯一约束。已存在的兼容表保留数据并补建缺失索引，结构或已有索引不兼容时停止。处理事件时按 schema/FID 加事务锁，遇到既有重复用户对报错。同一 hash 的生命周期事件版本防止旧事件重试恢复新状态；同一来源的 shard 布局变更会显式报错，需要单独迁移版本依据。

本轮不执行全库历史重建，也不接入其他业务表。节点没有提供或旧采集阶段已经遗漏的历史无法自动补全，业务表行数不代表全网完整关系数。不要让旧 Go 初始化器并发写入这里的 `blocks`，它没有遵守增量处理器的锁和版本规则。

### 投影状态页（本地）

只在本机运行的网页，查看各投影的处理情况：业务表行数、数据缺口（`shuttle_projection_issues`，按原因分组、可按 FID 筛选、导出 CSV、查看 payload）、Redis Stream 积压与 DLQ、backfill worker 和队列，以及「从数据库重建」/ backfill 的运行记录和进度。也可以在页面上发起重建和只补投影的 backfill。

```bash
cd packages/shuttle
cp .env.dashboard.example .env.dashboard   # 填连接；不会提交（.gitignore）
ssh -N -L 55432:127.0.0.1:5432 -L 56379:127.0.0.1:6379 <server>   # 按服务器实际端口改
pnpm dashboard                              # http://127.0.0.1:8790
pnpm dashboard --env-file <file> --addr 127.0.0.1:8791
```

- 只允许监听本机地址，非 `127.0.0.1` / `::1` / `localhost` 直接拒绝；请求的 Host 也必须是本机名。没有鉴权。
- env 文件里放 `POSTGRES_URL`、`POSTGRES_SCHEMA`、`REDIS_URL`；进程环境变量优先。不配 `REDIS_URL` 时只显示数据库部分。某一块读取失败时单独显示错误，不会显示成 0；刷新失败时保留上一次的数据。
- 运行记录表 `shuttle_projection_runs` 由迁移 007 创建（shard 启动时执行），所以要先部署一次。之前跑过的重建 / backfill 只在日志里。命令行的 `rebuild`、`backfill` 和 worker 也会写这张表，页面能看到它们的进度。backfill 以每批（10 个 FID）完成数计进度，全部批次完成或重试耗尽后才标记结束，不再依赖 `completionMarker`。超过 5 分钟没有心跳的 `running` 记录显示为“可能已中断”。
- 页面上的「从数据库重建」在本机进程里执行、经隧道写服务器库（与命令行 `rebuild` 相同：每个 FID 一个事务、同一把锁、可重复）。全量重建较慢，建议仍在服务器上跑命令行；关掉页面服务会中断它，记录标为失败。同一时间只允许一个重建。
- 页面上的 backfill 只排队“只补投影”的任务，由服务器上的 worker 执行；worker 不在线时拒绝。不提供全量原始 backfill。不指定 FID 时需要填 FID 上限，页面用 `onchain_events` 里最大的 fid 作参考值。

## 服务端

没有 CI 和镜像仓库。本地 `scripts/deploy.sh` 把指定提交的 `git archive` 上传到服务器 `/opt/shuttle/releases/<sha>`，在服务器上原生构建 `shuttle:<sha>`，再切换版本。`nice` 只降低 docker 命令行客户端的优先级，真正执行构建的 dockerd 不受影响；2 核机器上首次构建会和 shard 争用 CPU，最好在低峰期部署。Dockerfile 先根据各 workspace 的 `package.json`、`pnpm-lock.yaml` 和 `pnpm-workspace.yaml` 安装依赖，再复制源码构建，所以只改代码时会复用依赖层。新增 workspace 时要同步更新 Dockerfile 里的 manifest 列表。

```bash
scripts/deploy.sh deploy            # 部署 HEAD，工作区必须没有未提交的修改
scripts/deploy.sh deploy <ref>      # 部署指定提交
scripts/deploy.sh status            # 当前 IMAGE_TAG、容器、保留的镜像和 release
scripts/deploy.sh rollback <sha>    # 切回服务器上仍保留的旧镜像，不重新构建
```

切换流程：

1. 迁移守卫：目标镜像注册的迁移比当前镜像少时拒绝切换。迁移只能前进，跨迁移回滚需要 `--force` 并自行确认 schema 兼容。
2. 从镜像内复制 `docker-compose.yml` 和 `scripts/*.sh` 到 `/opt/shuttle`，旧文件和 `.env` 备份到 `/opt/shuttle/backups/<时间>`。
3. 把 `.env` 里的 `IMAGE_TAG` 改成新 sha，Compose 用 `shuttle:${IMAGE_TAG}`。
4. 先重建 `shuttle-1`，等日志出现 `Starting stream consumer`（启动迁移已完成）再重建 `shuttle-2`。失败时停止，不动另一个 shard，也不自动回滚。
5. 保留最近 `KEEP_RELEASES`（默认 3）个 release 和镜像，外加当前和上一个版本。

每个 shard 只能有一个消费进程，部署时先停后启，不做蓝绿部署。脚本不修改 Redis 订阅位置。SSH 目标沿用 `SHUTTLE_SSH_HOST`、`SHUTTLE_SSH_IDENTITY`。

首次使用脚本时，服务器 `.env` 还没有 `IMAGE_TAG`，旧版是在 `/opt/shuttle` 源码目录里构建的。脚本会用新的 compose 文件覆盖旧文件并重建两个 shard，旧源码文件可以确认无误后手动清理（保留 `.env`、`docker-compose.yml`、`scripts/`、`releases/`、`backups/`）。

仅在首次启动空 Redis、且只需要从当前时刻同步时，运行一次 `sudo ./scripts/seed-last-event.sh`。日常重启不要再次运行，以免跳过停机期间的事件。

`shuttle-watchdog.timer` 保持停用。worker 平时不启动，只在按下文步骤执行 backfill 时临时运行。升级后由两个现有 shard 进程执行启动迁移和 blocks 转换；保留原 Redis 订阅位置，不重新 seed。代码修改要提交后运行 `scripts/deploy.sh` 才会部署。

### 部署新版本并用 backfill 补 blocks

1. 部署前检查 `onchain_events` 有没有内容不同的重复行（见上文 SQL）。第二列都等于 1 才能继续。
2. 在本地运行 `scripts/deploy.sh deploy`。迁移 006 由 shard 进程在启动时执行，`backfill` 命令不跑迁移，所以必须先完成这一步。脚本会打印执行过的迁移。之后在服务器 `/opt/shuttle` 下执行以下步骤：

3. 先用少量 fid 试跑只补投影的 backfill。`BACKFILL_PROJECTIONS_ONLY=true` 表示只向节点拉取投影需要的消息族（blocks 对应 link 消息和 compact），没有投影关心的消息（例如 follow）不写库，每个 fid 在一个事务里只重算一次：

```bash
sudo docker compose run --rm -e BACKFILL_PROJECTIONS_ONLY=true -e FIDS=3,2,1 shuttle-worker pnpm start backfill
```

日志出现覆盖最后一个 fid 的 `Reconciled N upto <fid>` 后按 Ctrl-C 停止（worker 不会自己退出）。不要以 `Reconciliation started at ... complete at ...` 为准：worker 并发执行任务，这条完成标记可能在 reconcile 任务真正跑完之前就打印。有 block 的 fid 会打出 `Backfilled projected messages`。然后查结果：

```sql
SELECT count(*) AS total, count(*) FILTER (WHERE deleted_at IS NULL) AS active FROM blocks;
SELECT projection, reason, count(*) FROM shuttle_projection_issues GROUP BY 1, 2;
```

4. 确认无误后去掉 `FIDS` 跑全量（务必保留 `BACKFILL_PROJECTIONS_ONLY=true`），会排队到节点当前注册数为止，每批 10 个 fid，`CONCURRENCY` 控制并发。节点仍然要为每个 fid 返回全部 link 消息，所以会跑较久，但只写 block 相关的行。中断后可以重跑：已存在的消息会被跳过，结果不变。

backfill 只能补回节点现在还保留的消息，早年已被 prune 的 block 无法恢复。

## 本地开发

使用 Node 24。依赖和 core、hub-nodejs、shuttle 构建产物已准备好。

```bash
# 独立终端中保持 SSH 通道运行
python3 scripts/dev-tunnel.py

# 验证数据库、Redis、Snapchain 连接，只读操作
node --env-file=.env.development scripts/dev-check.mjs

# 重新安装依赖
pnpm install --frozen-lockfile

# 按依赖顺序构建
pnpm --dir packages/core build && pnpm --dir packages/hub-nodejs build && pnpm --dir packages/shuttle build
```

`.env` 保留服务端连接配置，`.env.development` 使用本机 SSH 转发端口，两者均不提交 Git。默认端口：PostgreSQL `15432`、Redis `26379`、Snapchain gRPC `23383`、HTTP `23381`。可通过 `SHUTTLE_SSH_HOST`、`SHUTTLE_SSH_IDENTITY` 覆盖 SSH 目标和私钥路径。

本地不启动第二组 shard 订阅进程；业务处理代码修改并验证后，再构建和更新服务器镜像。

## 测试

Shuttle 集成测试会重建 `shuttle_test` 数据库并清空测试 Redis。只使用隔离的本地测试实例，禁止使用服务器 `.env` 或 `.env.development` 运行测试。

blocks、fids、storage_rentals 和投影框架集成测试（`src/blocks.integration.test.ts`、`src/fids.integration.test.ts`、`src/storageRentals.integration.test.ts`、`src/projections.integration.test.ts`）只使用显式的 `TEST_DATABASE_URL`，为每次测试创建并删除随机命名的独立数据库，不读取应用连接配置；未指定时跳过。测试账号需要 CREATEDB 权限。

```bash
cd packages/shuttle
TEST_DATABASE_URL='postgres://shuttle@127.0.0.1:55441/postgres' \
  NODE_ENV=test NODE_OPTIONS=--experimental-vm-modules \
  ../../node_modules/.bin/jest --runTestsByPath src/blocks.integration.test.ts src/fids.integration.test.ts src/storageRentals.integration.test.ts src/projections.integration.test.ts --runInBand
```
