---
tags:
  - 类型/操作指南
  - 主题/数据同步
  - 技术/CDC
sbc_capture: true
sbc_title: CKafka 连接器（DataHub）
sbc_kind: 有用
sbc_reason: 打通腾讯云PG与Kafka的连接, 将表的变化转换成Kafka的消息.
sbc_source: Gemini
sbc_created: 2026-11-12T07:54:36.486Z
sbc_id: 1
---

> [!WARNING]
> **已废弃，仅存档：增量改为 shuttle 内联投影 + 现有 Redis Stream**
> 新设计见 [100_shuttle-inline-projection](100_shuttle-inline-projection.md)。

使用 **CKafka 连接器（原 DataHub 模块）** 将 PostgreSQL 数据导入 Kafka，需要在 [腾讯云消息队列 CKafka](https://cloud.tencent.com/document/product/597/83542) 控制台内创建一个**全托管的“数据流入”任务**。底层由腾讯云托管的 CDC 引擎自动读取 PostgreSQL（下文简称 PG）的逻辑复制流（WAL），并将数据实时投递到指定的 CKafka Topic，无需购买或维护 DTS 实例，也无需自建 Debezium 集群。 [[1](https://cloud.tencent.com/document/product/597/83542)]

配置流程分为**源端准备**、**创建连接**和**创建流入任务**三步。

第一步：PostgreSQL 源端准备

配置连接器前，需先完成源端准备，并创建目标 Topic。源端支持腾讯云数据库 PG、TDSQL-C PostgreSQL 版或自建 PG：

- **开启逻辑复制**。检查参数 `wal_level` 是否已设置为 `logical`（腾讯云托管版 PG 默认支持或可在控制台参数设置中修改，修改后需重启实例生效）。同时，确保 `max_wal_senders` 和 `max_replication_slots` 均大于当前已有的复制任务数。
- **创建专用同步账号**。连接器使用的数据库账号需具备登录、复制及目标库表的查询权限。可执行以下 SQL 授权：
    
```sql
CREATE USER ckafka_sync WITH REPLICATION LOGIN PASSWORD '<redacted>';
GRANT SELECT ON ALL TABLES IN SCHEMA public TO ckafka_sync;
```
    
- **==🟡检查表主键与复制标识==**。建议为需要同步的表设置主键。如果表没有主键，但需要捕获 `UPDATE` 和 `DELETE` 的完整变更记录，需在 PG 中执行 `ALTER TABLE 表名 REPLICA IDENTITY FULL;`。
- **==🟡准备目标 Topic==**。在 [CKafka 控制台](https://cloud.tencent.com/document/product/597/83283) 中提前创建用于接收数据的 Topic（支持普通实例内 Topic 或弹性 Topic）。

---

第二步：在 CKafka 中创建 PostgreSQL 连接

创建连接时，需保存源数据库的访问凭证和网络路由信息：

- **进入连接管理**。登录腾讯云 CKafka 控制台，在左侧导航栏选择 **连接器 > 连接列表**，在顶部切换至与 PG 实例相同的地域，单击 **新建连接**。 [[1](https://www.shangyun51.com/documentdetail?id=15547)]
- **选择连接类型**。在连接类型列表中选择 **PostgreSQL**（云原生版选择对应的 TDSQL-C PostgreSQL），单击 **下一步**。 [[1](https://www.shangyun51.com/documentdetail?id=15547)]
- **填写实例与认证信息**。
    
    - **数据库来源**：选择“云数据库”（可直接从下拉列表选取同账号下的实例）或“自建数据库”（需手动填写内网 IP/域名和端口，默认 `5432`）。
    - **网络配置**：选择与 PostgreSQL 互通的私有网络（VPC）和子网，系统会自动打通内网链路。
    - **账号密码**：填入第一步创建的 `ckafka_sync` 账号及密码，单击 **测试连接**，验证通过后保存。
    

---


第三步：创建“数据流入”同步任务

连接创建完成后，即可配置数据抓取与投递规则：

- **新建流入任务**。在左侧导航栏进入 **连接器 > 任务列表**，单击 **新建任务**。自定义任务名称，任务类型选择 **数据流入**，数据源类型选择 **PostgreSQL**。
- **配置数据源与同步对象**。
    
    - **关联连接**：选择第二步创建的 PostgreSQL 连接。
    - **同步方式**：可选择 **全量 + 增量**（先扫描全表历史数据，再无缝衔接 WAL 增量）或 **==🟡仅增量==%%callout-link:cr-e2e67b3a-edfc-4821-800d-b657ad4dce12%%**。
    - **选择库表**：勾选需要同步的 Database、Schema（如 `public`）及具体表名（支持单表、多表或按正则匹配）。
    - **解码插件与槽位**：解码插件通常使用默认的 `pgoutput`（PG 10+ 原生支持，无需额外安装插件），复制槽名称可由系统自动生成或手动指定。

    
- **配置数据清洗（可选 ETL）**。CKafka 连接器内置轻量级数据处理能力。如果不需要整行原始数据，可在此配置 **字段过滤、字段映射、JSON 解析或简单正则替换**，清洗后再写入 Kafka。
- **配置数据目标（CKafka Topic）**。
    
    - **目标类型**：选择 **CKafka**，并指定目标实例及提前创建的 **Topic**。
    - **==🟡投递与分区策略==**：可根据下游对消费顺序的要求设置分区键（例如，默认按“表名 + 主键”进行哈希，将数据投递到固定分区，确保单行数据的变更严格有序）。
    
- **启动与监控**。提交任务后，状态变为“运行中”即表示开始同步。可在任务详情页查看 **同步延迟、每秒写入条数（TPS）** 及异常日志。

---

下游消费的数据格式示例

CKafka 连接器默认将 PG 的变更记录封装为标准的 **JSON 格式**，包含操作类型 `op`、变更前数据 `before`、变更后数据 `after` 及库表元数据：

```json
{
  "before": null,
  "after": {
    "id": 1001,
    "username": "zhangsan",
    "updated_at": "2026-11-11T09:10:00Z"
  },
  "source": {
    "db": "order_db",
    "schema": "public",
    "table": "users",
    "ts_ms": 1794388200000
  },
  "op": "c" 
}
```

请谨慎使用示例代码。

_注：`op` 字段中，`c` 代表新增（INSERT），`u` 代表更新（UPDATE），`d` 代表删除（DELETE），`r` 代表全量初始化读取（READ）。_

使用限制与避坑提示

- **版本兼容性**。CKafka 连接器通常要求源端 PostgreSQL 版本为 **10 及以上**，以使用原生支持的 `pgoutput` 逻辑解码插件。
- **复制槽积压风险**。逻辑复制依赖 PG 的复制槽（Replication Slot）。如果==🟡连接器任务长时间暂停==，或==🟡任务删除后未清理复制槽==，PG 源库的 WAL 日志将无法回收，进而占满磁盘。==🟡停止不再使用的任务时，务必确认复制槽已释放==。

> [!TIP] 仅增量
> 先用代码针对messages表跑一遍数据, 然后开启仅增量. 再跑一遍全量到指定位置, 随后开始跑增量的处理服务.
> ^cr-e2e67b3a-edfc-4821-800d-b657ad4dce12

> [!QUESTION] PostgreSQL Schema 是什么?
> 在 PostgreSQL（简称 PG）中，**Schema（模式）** 是数据库内部的一个**逻辑命名空间（Namespace）**，用于组织和隔离数据库对象（如表、视图、函数、索引、数据类型等）。
 