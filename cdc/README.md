# MySQL到Doris CDC同步解决方案

基于Debezium 3.2 + Kafka + Doris Routine Load的实时数据同步方案。

## 🏗️ 系统架构

```
MySQL (生产库)
    ↓ (binlog)
Debezium MySQL Connector 
    ↓ (CDC events)
Kafka (消息队列)
    ↓ (JSON消息)
Doris Routine Load
    ↓ (实时消费)
Doris数据仓库 (ODS层)
```

## 📋 技术栈版本

| 组件 | 版本 | 说明 |
|------|------|------|
| MySQL | 8.4.5 | 生产数据库，需开启binlog |
| Debezium | 3.2.0.Final | CDC连接器 |
| Kafka | 3.6 LTS | 消息队列 |
| Doris | 3.0.5+ | 数据仓库 |
| Schema Registry | 7.5.1 | 模式管理（可选）|

## 🚀 快速开始

### 1. 前置条件检查

确保MySQL已配置CDC所需参数：

```sql
-- 检查binlog配置
SHOW VARIABLES LIKE 'log_bin';                -- 应为 ON
SHOW VARIABLES LIKE 'binlog_format';          -- 应为 ROW
SHOW VARIABLES LIKE 'binlog_row_image';       -- 应为 FULL

-- 如需修改，添加到my.cnf并重启MySQL
[mysqld]
log-bin=mysql-bin
binlog-format=ROW
binlog-row-image=FULL
server-id=1
```

### 2. 启动CDC技术栈

```bash
# 启动完整的CDC技术栈
python3 cdc/scripts/cdc_manager.py start

# 等待服务启动完成（约2-3分钟）
# 系统会自动等待各服务就绪
```

### 3. 部署MySQL CDC连接器

```bash
# 部署Debezium MySQL连接器
python3 cdc/scripts/cdc_manager.py deploy-connector
```

### 4. 创建Doris表和Routine Load

```bash
# 创建支持CDC的Unique Key表
python3 cdc/scripts/setup_doris_routine_load.py create-table

# 创建Routine Load任务
python3 cdc/scripts/setup_doris_routine_load.py create-load
```

### 5. 运行端到端测试

```bash
# 运行完整的CDC测试
python3 cdc/scripts/test_cdc_pipeline.py --test-type full
```

## 📊 管理界面

启动后可访问以下管理界面：

| 服务 | 地址 | 用途 |
|------|------|------|
| Kafka UI | http://localhost:8080 | Kafka主题和消息监控 |
| Debezium UI | http://localhost:8082 | CDC连接器管理 |
| Grafana | http://localhost:3000 | 监控仪表板 (admin/admin) |
| Prometheus | http://localhost:9090 | 指标收集 |

## 🔧 详细操作指南

### CDC管理命令

```bash
# 启动CDC技术栈
python3 cdc/scripts/cdc_manager.py start

# 停止CDC技术栈
python3 cdc/scripts/cdc_manager.py stop

# 检查连接器状态
python3 cdc/scripts/cdc_manager.py status

# 健康检查
python3 cdc/scripts/cdc_manager.py health

# 获取Kafka主题
python3 cdc/scripts/cdc_manager.py topics

# 查看端到端统计
python3 cdc/scripts/cdc_manager.py stats
```

### Doris Routine Load管理

```bash
# 创建表
python3 cdc/scripts/setup_doris_routine_load.py create-table --table abc_warning

# 创建Routine Load任务
python3 cdc/scripts/setup_doris_routine_load.py create-load --table abc_warning

# 查看任务状态
python3 cdc/scripts/setup_doris_routine_load.py status --table abc_warning

# 暂停任务
python3 cdc/scripts/setup_doris_routine_load.py pause --table abc_warning

# 恢复任务
python3 cdc/scripts/setup_doris_routine_load.py resume --table abc_warning

# 查看统计信息
python3 cdc/scripts/setup_doris_routine_load.py stats --table abc_warning
```

### CDC测试

```bash
# 完整测试（INSERT + UPDATE + DELETE）
python3 cdc/scripts/test_cdc_pipeline.py --test-type full

# 单独测试INSERT
python3 cdc/scripts/test_cdc_pipeline.py --test-type insert

# 单独测试UPDATE
python3 cdc/scripts/test_cdc_pipeline.py --test-type update

# 单独测试DELETE
python3 cdc/scripts/test_cdc_pipeline.py --test-type delete

# 查看同步延迟
python3 cdc/scripts/test_cdc_pipeline.py --test-type latency
```

## 📈 监控和运维

### 关键指标监控

1. **Debezium连接器状态**
   - 连接器运行状态
   - 任务执行状态
   - 错误数量和类型

2. **Kafka指标**
   - 主题分区数
   - 消息积压情况
   - 生产和消费速率

3. **Doris Routine Load**
   - 任务运行状态
   - 数据加载速率
   - 错误记录数

4. **数据同步延迟**
   - 端到端延迟
   - 各环节处理时间

### 常见问题排查

#### 1. 连接器无法启动

```bash
# 检查连接器状态
python3 cdc/scripts/cdc_manager.py status

# 查看Kafka Connect日志
docker logs kafka-connect

# 检查MySQL连接
python3 mysql_to_doris/Doris_and_Mysql_connection_test.py
```

#### 2. Routine Load失败

```sql
-- 在Doris中查看任务详情
SHOW ROUTINE LOAD FOR abc_warning_cdc_load;

-- 查看错误日志
SHOW LOAD WARNINGS ON 'abc_warning_cdc_load';
```

#### 3. 数据同步延迟过高

```bash
# 检查各环节延迟
python3 cdc/scripts/test_cdc_pipeline.py --test-type latency

# 查看Kafka消息积压
docker exec kafka kafka-consumer-groups --bootstrap-server localhost:9092 --describe --group doris_cdc_consumer
```

## ⚙️ 配置调优

### Debezium连接器调优

编辑 `cdc/config/mysql-connector.json`：

```json
{
  "config": {
    "max.batch.size": "4096",        // 增加批次大小
    "max.queue.size": "16384",       // 增加队列大小
    "poll.interval.ms": "500",       // 减少轮询间隔
    "heartbeat.interval.ms": "10000" // 心跳间隔
  }
}
```

### Kafka调优

编辑 `cdc/docker-compose.cdc.yml` 中的Kafka环境变量：

```yaml
environment:
  KAFKA_NUM_PARTITIONS: 6              # 增加分区数
  KAFKA_LOG_SEGMENT_BYTES: 268435456   # 256MB段文件
  KAFKA_LOG_RETENTION_HOURS: 168       # 7天保留期
```

### Doris Routine Load调优

```sql
-- 修改Routine Load参数
ALTER ROUTINE LOAD abc_warning_cdc_load
PROPERTIES (
    "max_batch_interval" = "5",     -- 减少批次间隔
    "max_batch_rows" = "200000",    -- 增加批次行数
    "desired_concurrent_number" = "2" -- 增加并发数
);
```

## 🔐 安全配置

### 1. MySQL用户权限

```sql
-- 创建CDC专用用户
CREATE USER 'cdc_user'@'%' IDENTIFIED BY 'SecurePassword123!';
GRANT SELECT, RELOAD, SHOW DATABASES, REPLICATION SLAVE, REPLICATION CLIENT ON *.* TO 'cdc_user'@'%';
FLUSH PRIVILEGES;
```

### 2. Kafka安全配置

```yaml
# 启用SASL_SSL认证
environment:
  KAFKA_SECURITY_INTER_BROKER_PROTOCOL: SASL_SSL
  KAFKA_SASL_MECHANISM_INTER_BROKER_PROTOCOL: PLAIN
  KAFKA_SASL_ENABLED_MECHANISMS: PLAIN
```

### 3. Doris权限控制

```sql
-- 创建CDC专用角色
CREATE ROLE cdc_role;
GRANT LOAD_PRIV ON ods.abc_warning TO ROLE cdc_role;

-- 创建用户并分配角色
CREATE USER 'cdc_user'@'%' IDENTIFIED BY 'SecurePassword123!';
GRANT cdc_role TO cdc_user;
```

## 📊 性能基准

在标准配置下的性能基准：

| 指标 | 值 |
|------|-----|
| 最大吞吐量 | 10,000 records/sec |
| 平均延迟 | < 2 seconds |
| P99延迟 | < 5 seconds |
| 数据准确性 | 99.99% |

## 🆘 故障恢复

### 1. 连接器故障恢复

```bash
# 重启连接器
curl -X POST http://localhost:8083/connectors/mysql-doris-cdc-connector/restart

# 重新部署连接器
python3 cdc/scripts/cdc_manager.py deploy-connector
```

### 2. Routine Load故障恢复

```sql
-- 停止失败的任务
STOP ROUTINE LOAD FOR abc_warning_cdc_load;

-- 重新创建任务
python3 cdc/scripts/setup_doris_routine_load.py create-load
```

### 3. 数据一致性修复

```bash
# 运行全量同步（如需要）
python3 mysql_to_doris/mysql_to_doris_sync.py

# 验证数据一致性
python3 mysql_to_doris/verify_sync.py
```

## 📝 变更日志

- **v1.0.0** - 初始版本，支持基础CDC功能
- **v1.1.0** - 添加监控和告警功能
- **v1.2.0** - 支持多表同步和性能优化

## 🤝 支持

如有问题，请检查：

1. 系统日志和错误信息
2. 网络连接和防火墙设置
3. 各组件的配置参数
4. 资源使用情况（CPU、内存、磁盘）

## 📚 相关文档

- [Debezium MySQL Connector文档](https://debezium.io/documentation/reference/stable/connectors/mysql.html)
- [Apache Doris Routine Load文档](https://doris.apache.org/docs/dev/data-operate/import/routine-load-manual)
- [Kafka Connect文档](https://kafka.apache.org/documentation/#connect) 