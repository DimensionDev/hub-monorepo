# Shuttle

服务器 `/opt/shuttle` 运行两个 shard 的增量同步。本地保留源码、构建和开发工具。

## 数据库

当前迁移只创建：

- `messages`
- `onchain_events`
- `kysely_migration`、`kysely_migration_lock`（迁移元数据）

`pgcrypto` 扩展和 `generate_ulid()` 函数用于生成主键。`casts`、`usernames` 的示例迁移不在启动迁移列表内，消息处理也不写入 casts 投影。可选用户名重建器的测试会在独立测试数据库中创建自己的测试表。

## 服务端

Dockerfile 使用固定的 DimensionDev 镜像作为依赖基础，并复制当前 `packages/shuttle/src/example-app` 源码。Compose 默认只运行 `shuttle-1`、`shuttle-2`。

```bash
cd /opt/shuttle
sudo docker compose build shuttle-1
sudo systemctl enable --now shuttle.service
sudo docker compose logs --tail=50 shuttle-1 shuttle-2
```

仅在首次启动空 Redis、且只需要从当前时刻同步时，运行一次 `sudo ./scripts/seed-last-event.sh`。日常重启不要再次运行，以免跳过停机期间的事件。

`shuttle-watchdog.timer` 保持停用。Backfill 和 worker 暂不启动，等待业务表和处理逻辑完成。

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
