# MySQL到Doris数据同步部署指南

## 📋 项目概述

这是一个完整的MySQL到Doris数据同步解决方案，包含：
- **全量数据导入**：一次性将MySQL表数据导入Doris
- **CDC增量同步**：使用Flink CDC实现实时数据同步
- **数据验证**：确保数据同步的准确性

## 🔧 环境配置

### 服务器信息
- **MySQL服务器**: 10.10.0.117:6033
- **Doris服务器**: 192.168.99.6
  - FE WebUI: http://192.168.99.6:18030
  - MySQL协议: 192.168.99.6:19030

### 依赖包安装
```bash
pip install pymysql pandas requests
```

## 🚀 快速开始

### 1. 连接测试
```bash
python3 connection_test.py
```

**预期输出**：
```
✅ MySQL连接成功!
✅ abc_warning表存在
✅ Doris连接成功!
🎉 所有连接测试通过，可以开始数据同步!
```

### 2. 全量数据同步
```bash
python3 mysql_to_doris_sync.py
```

**功能**：
- 自动分析MySQL表结构
- 在Doris中创建对应表（ods.abc_warning）
- 批量导入全量数据
- 生成CDC配置文件

### 3. 数据验证
```bash
python3 verify_sync.py
```

**验证内容**：
- 记录数对比
- 样本数据对比
- Doris查询功能测试

## 📊 同步结果

### 表结构映射
| MySQL字段 | MySQL类型 | Doris字段 | Doris类型 |
|-----------|-----------|-----------|-----------|
| snapshot_date | date | snapshot_date | DATE |
| student_id | bigint | student_id | BIGINT |
| attendance_rate | decimal(5,2) | attendance_rate | DECIMAL |
| submit_rate | decimal(5,2) | submit_rate | DECIMAL |
| violation_cnt | int | violation_cnt | INT |
| core_fail_cnt | int | core_fail_cnt | INT |
| risk_level | varchar(20) | risk_level | VARCHAR(20) |

### 数据统计
- **总记录数**: 100条
- **同步状态**: ✅ 完全一致
- **按风险等级分布**:
  - High: 20条 (89.84%出勤率, 85.56%提交率)
  - Med: 30条 (91.79%出勤率, 85.77%提交率)
  - Low: 50条 (89.52%出勤率, 85.50%提交率)

## 🔄 CDC增量同步配置

### Flink环境要求
1. **Flink版本**: 1.20.1
2. **必需依赖**:
   - flink-connector-mysql-cdc
   - flink-connector-doris

### CDC配置步骤

#### 1. 启动Flink
在Doris服务器上启动Flink：
```bash
# 连接到服务器
ssh root@192.168.99.6

# 进入Flink目录并启动
cd /path/to/flink
./bin/start-cluster.sh

# 访问Flink WebUI
# http://192.168.99.6:8081
```

#### 2. 执行CDC配置
在Flink SQL Client中执行以下SQL：
```sql
-- 使用生成的 flink_cdc_config.sql 配置文件
```

#### 3. 启动CDC任务
```sql
-- 任务将自动监听MySQL的abc_warning表变更
-- 实时同步到Doris的ods.abc_warning表
```

## 🔍 验证增量同步

### 1. 在MySQL中插入测试数据
```sql
INSERT INTO abc_warning VALUES 
('2025-07-21', 9999, 95.50, 88.30, 1, 0, 'Low');
```

### 2. 检查Doris中是否同步
```sql
-- 连接Doris
mysql -h 192.168.99.6 -P 19030 -u root

-- 查询新增数据
USE ods;
SELECT * FROM abc_warning WHERE student_id = 9999;
```

### 3. 测试更新和删除
```sql
-- MySQL中更新
UPDATE abc_warning SET risk_level = 'High' WHERE student_id = 9999;

-- MySQL中删除
DELETE FROM abc_warning WHERE student_id = 9999;
```

## 📈 数据仓库分层建议

### ODS层 (已完成)
- **表名**: ods.abc_warning
- **用途**: 原始数据存储，保持与源系统一致

### DWD层 (明细数据层)
```sql
-- 建议创建清洗后的明细表
CREATE TABLE dwd.student_behavior_detail AS
SELECT 
    snapshot_date,
    student_id,
    CASE 
        WHEN attendance_rate >= 95 THEN '优秀'
        WHEN attendance_rate >= 85 THEN '良好'
        WHEN attendance_rate >= 75 THEN '一般'
        ELSE '较差'
    END as attendance_level,
    attendance_rate,
    submit_rate,
    violation_cnt,
    core_fail_cnt,
    risk_level
FROM ods.abc_warning;
```

### DWS层 (汇总数据层)
```sql
-- 按学生汇总的行为指标
CREATE TABLE dws.student_behavior_summary AS
SELECT 
    student_id,
    AVG(attendance_rate) as avg_attendance_rate,
    AVG(submit_rate) as avg_submit_rate,
    SUM(violation_cnt) as total_violations,
    SUM(core_fail_cnt) as total_core_fails,
    COUNT(*) as record_count
FROM ods.abc_warning
GROUP BY student_id;
```

### ADS层 (应用数据层)
```sql
-- 风险学生识别
CREATE TABLE ads.risk_students AS
SELECT 
    student_id,
    risk_level,
    avg_attendance_rate,
    avg_submit_rate,
    total_violations,
    total_core_fails,
    CASE 
        WHEN avg_attendance_rate < 80 OR total_core_fails > 3 THEN '高风险'
        WHEN avg_attendance_rate < 90 OR total_violations > 5 THEN '中风险'
        ELSE '低风险'
    END as final_risk_assessment
FROM dws.student_behavior_summary;
```

## 🛠 故障排除

### 常见问题

#### 1. 连接失败
- 检查防火墙设置
- 验证网络连通性
- 确认服务端口开放

#### 2. CDC同步延迟
- 检查Flink任务状态
- 查看MySQL binlog配置
- 监控网络带宽

#### 3. 数据类型不匹配
- 查看类型映射配置
- 调整Doris表结构
- 重新执行同步脚本

### 监控建议

#### 1. 数据一致性监控
```python
# 定期运行验证脚本
python3 verify_sync.py
```

#### 2. 同步延迟监控
```sql
-- 检查最新同步时间
SELECT MAX(snapshot_date) as latest_sync FROM ods.abc_warning;
```

#### 3. 错误日志监控
- Flink任务日志
- Doris FE/BE日志
- MySQL错误日志

## 📞 技术支持

### 配置文件
- `connection_test.py`: 连接测试
- `mysql_to_doris_sync.py`: 全量同步
- `verify_sync.py`: 数据验证
- `flink_cdc_config.sql`: CDC配置

### 联系方式
如有问题，请检查：
1. 服务器状态
2. 网络连接
3. 配置参数
4. 日志文件

---

## 🎯 下一步计划

1. **扩展到更多表**: 同步其他业务表
2. **性能优化**: 调整批量大小和并行度
3. **监控告警**: 建立完善的监控体系
4. **数据质量**: 实现数据质量检查
5. **自动化**: 构建自动化部署流程

---

*更新时间: 2025-07-21*
*版本: v1.0* 