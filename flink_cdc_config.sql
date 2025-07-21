-- =========================================
-- Flink CDC 配置 - MySQL到Doris实时同步
-- =========================================

-- 1. 创建MySQL CDC源表
CREATE TABLE mysql_abc_warning_source (
    snapshot_date DATE,
    student_id BIGINT,
    attendance_rate DECIMAL(5,2),
    submit_rate DECIMAL(5,2),
    violation_cnt INT,
    core_fail_cnt INT,
    risk_level STRING,
    PRIMARY KEY (snapshot_date, student_id) NOT ENFORCED
) WITH (
    'connector' = 'mysql-cdc',
    'hostname' = '10.10.0.117',
    'port' = '6033',
    'username' = 'root',
    'password' = 'Xml123&45!',
    'database-name' = 'data_ware_test',
    'table-name' = 'abc_warning',
    'server-time-zone' = 'Asia/Shanghai'
);

-- 2. 创建Doris sink表
CREATE TABLE doris_abc_warning_sink (
    snapshot_date DATE,
    student_id BIGINT,
    attendance_rate DECIMAL(5,2),
    submit_rate DECIMAL(5,2),
    violation_cnt INT,
    core_fail_cnt INT,
    risk_level STRING
) WITH (
    'connector' = 'doris',
    'fenodes' = '192.168.99.6:18030',
    'table.identifier' = 'ods.abc_warning',
    'username' = 'root',
    'password' = '',
    'sink.label-prefix' = 'abc_warning_cdc',
    'sink.properties.format' = 'json',
    'sink.properties.read_json_by_line' = 'true',
    'sink.enable-delete' = 'true'
);

-- 3. 启动实时同步任务
INSERT INTO doris_abc_warning_sink 
SELECT 
    snapshot_date,
    student_id,
    attendance_rate,
    submit_rate,
    violation_cnt,
    core_fail_cnt,
    risk_level
FROM mysql_abc_warning_source; 