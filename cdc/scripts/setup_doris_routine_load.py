#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Doris Routine Load 配置脚本
用于设置从Kafka消费CDC数据的Routine Load任务
"""

import pymysql
import json
import time
import logging

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

class DorisRoutineLoadManager:
    def __init__(self):
        self.doris_config = {
            'host': '192.168.99.6',
            'port': 19030,  # MySQL协议端口
            'user': 'root',
            'password': '',
            'database': 'ods',
            'charset': 'utf8mb4'
        }
        
        self.kafka_config = {
            'brokers': 'localhost:9092',
            'topic': 'abc_warning',  # 转换后的topic名称
            'group': 'doris_cdc_consumer'
        }
    
    def connect_doris(self):
        """连接Doris数据库"""
        try:
            conn = pymysql.connect(**self.doris_config)
            logger.info("✅ Doris连接成功")
            return conn
        except Exception as e:
            logger.error(f"❌ Doris连接失败: {e}")
            return None
    
    def create_routine_load_job(self, table_name='abc_warning'):
        """创建Routine Load任务"""
        conn = self.connect_doris()
        if not conn:
            return False
        
        try:
            cursor = conn.cursor()
            cursor.execute(f"USE {self.doris_config['database']}")
            
            # 停止已存在的同名任务
            try:
                stop_sql = f"STOP ROUTINE LOAD FOR {table_name}_cdc_load"
                cursor.execute(stop_sql)
                logger.info(f"🛑 已停止存在的Routine Load任务: {table_name}_cdc_load")
                time.sleep(5)  # 等待任务停止
            except Exception as e:
                logger.info(f"ℹ️  没有需要停止的任务: {e}")
            
            # 创建新的Routine Load任务
            routine_load_sql = f"""
CREATE ROUTINE LOAD {table_name}_cdc_load ON {table_name}
COLUMNS(
    snapshot_date,
    student_id,
    attendance_rate,
    submit_rate,
    violation_cnt,
    core_fail_cnt,
    risk_level,
    __op = jsonb_extract_string(message, '$.op'),
    __ts_ms = jsonb_extract_string(message, '$.ts_ms'),
    __source_db = jsonb_extract_string(message, '$.source_db'),
    __source_table = jsonb_extract_string(message, '$.source_table')
)
PROPERTIES (
    "format" = "json",
    "jsonpaths" = "[
        \\"$.snapshot_date\\",
        \\"$.student_id\\",
        \\"$.attendance_rate\\",
        \\"$.submit_rate\\",
        \\"$.violation_cnt\\",
        \\"$.core_fail_cnt\\",
        \\"$.risk_level\\",
        \\"$\\"
    ]",
    "strip_outer_array" = "false",
    "fuzzy_parse" = "true",
    "max_batch_interval" = "10",
    "max_batch_rows" = "100000",
    "max_batch_size" = "104857600",
    "desired_concurrent_number" = "1",
    "max_error_number" = "100"
)
FROM KAFKA (
    "kafka_broker_list" = "{self.kafka_config['brokers']}",
    "kafka_topic" = "{self.kafka_config['topic']}",
    "property.group.id" = "{self.kafka_config['group']}",
    "property.client.id" = "doris_cdc_client",
    "property.kafka_default_offsets" = "OFFSET_BEGINNING",
    "property.enable.auto.commit" = "true",
    "property.auto.commit.interval.ms" = "5000",
    "property.session.timeout.ms" = "30000",
    "property.request.timeout.ms" = "60000",
    "property.max.poll.records" = "1000"
)
"""
            
            logger.info(f"🚀 正在创建Routine Load任务: {table_name}_cdc_load")
            logger.info(f"SQL: {routine_load_sql}")
            
            cursor.execute(routine_load_sql)
            logger.info(f"✅ Routine Load任务创建成功: {table_name}_cdc_load")
            
            return True
            
        except Exception as e:
            logger.error(f"❌ 创建Routine Load任务失败: {e}")
            return False
        finally:
            conn.close()
    
    def create_unique_key_table(self, table_name='abc_warning'):
        """创建支持CDC的Unique Key表"""
        conn = self.connect_doris()
        if not conn:
            return False
        
        try:
            cursor = conn.cursor()
            cursor.execute(f"USE {self.doris_config['database']}")
            
            # 删除已存在的表（如果存在）
            try:
                cursor.execute(f"DROP TABLE IF EXISTS {table_name}")
                logger.info(f"🗑️  已删除存在的表: {table_name}")
            except Exception as e:
                logger.info(f"ℹ️  表不存在或删除失败: {e}")
            
            # 创建支持CDC的Unique Key表
            create_table_sql = f"""
CREATE TABLE IF NOT EXISTS {table_name} (
    `snapshot_date` DATE NOT NULL COMMENT "快照日期",
    `student_id` VARCHAR(50) NOT NULL COMMENT "学生ID",
    `attendance_rate` DECIMAL(5,4) COMMENT "出勤率",
    `submit_rate` DECIMAL(5,4) COMMENT "提交率",
    `violation_cnt` INT COMMENT "违规次数",
    `core_fail_cnt` INT COMMENT "核心课程挂科次数",
    `risk_level` VARCHAR(20) COMMENT "风险等级",
    `__op` VARCHAR(10) COMMENT "CDC操作类型",
    `__ts_ms` BIGINT COMMENT "CDC时间戳",
    `__source_db` VARCHAR(100) COMMENT "源数据库",
    `__source_table` VARCHAR(100) COMMENT "源表名",
    `__doris_version_count` BIGINT DEFAULT 1 COMMENT "Doris版本计数器"
) UNIQUE KEY(`snapshot_date`, `student_id`)
DISTRIBUTED BY HASH(`student_id`) BUCKETS 10
PROPERTIES (
    "replication_allocation" = "tag.location.default: 1",
    "enable_unique_key_merge_on_write" = "true",
    "function_column.sequence_col" = "__doris_version_count",
    "light_schema_change" = "true",
    "store_row_column" = "true"
)
"""
            
            logger.info(f"🏗️  正在创建Unique Key表: {table_name}")
            cursor.execute(create_table_sql)
            logger.info(f"✅ Unique Key表创建成功: {table_name}")
            
            return True
            
        except Exception as e:
            logger.error(f"❌ 创建表失败: {e}")
            return False
        finally:
            conn.close()
    
    def check_routine_load_status(self, table_name='abc_warning'):
        """检查Routine Load任务状态"""
        conn = self.connect_doris()
        if not conn:
            return None
        
        try:
            cursor = conn.cursor(pymysql.cursors.DictCursor)
            cursor.execute(f"USE {self.doris_config['database']}")
            
            sql = f"SHOW ROUTINE LOAD FOR {table_name}_cdc_load"
            cursor.execute(sql)
            result = cursor.fetchone()
            
            if result:
                logger.info(f"📊 Routine Load状态:")
                logger.info(f"   任务名: {result.get('Name', 'N/A')}")
                logger.info(f"   状态: {result.get('State', 'N/A')}")
                logger.info(f"   数据源: {result.get('DataSourceType', 'N/A')}")
                logger.info(f"   进度: {result.get('Progress', 'N/A')}")
                logger.info(f"   创建时间: {result.get('CreateTime', 'N/A')}")
                logger.info(f"   错误信息: {result.get('ReasonOfStateChanged', 'N/A')}")
            
            return result
            
        except Exception as e:
            logger.error(f"❌ 查询Routine Load状态失败: {e}")
            return None
        finally:
            conn.close()
    
    def pause_routine_load(self, table_name='abc_warning'):
        """暂停Routine Load任务"""
        conn = self.connect_doris()
        if not conn:
            return False
        
        try:
            cursor = conn.cursor()
            cursor.execute(f"USE {self.doris_config['database']}")
            
            sql = f"PAUSE ROUTINE LOAD FOR {table_name}_cdc_load"
            cursor.execute(sql)
            logger.info(f"⏸️  Routine Load任务已暂停: {table_name}_cdc_load")
            return True
            
        except Exception as e:
            logger.error(f"❌ 暂停Routine Load任务失败: {e}")
            return False
        finally:
            conn.close()
    
    def resume_routine_load(self, table_name='abc_warning'):
        """恢复Routine Load任务"""
        conn = self.connect_doris()
        if not conn:
            return False
        
        try:
            cursor = conn.cursor()
            cursor.execute(f"USE {self.doris_config['database']}")
            
            sql = f"RESUME ROUTINE LOAD FOR {table_name}_cdc_load"
            cursor.execute(sql)
            logger.info(f"▶️  Routine Load任务已恢复: {table_name}_cdc_load")
            return True
            
        except Exception as e:
            logger.error(f"❌ 恢复Routine Load任务失败: {e}")
            return False
        finally:
            conn.close()
    
    def get_load_statistics(self, table_name='abc_warning'):
        """获取加载统计信息"""
        conn = self.connect_doris()
        if not conn:
            return None
        
        try:
            cursor = conn.cursor(pymysql.cursors.DictCursor)
            cursor.execute(f"USE {self.doris_config['database']}")
            
            # 获取表行数
            cursor.execute(f"SELECT COUNT(*) as total_rows FROM {table_name}")
            table_stats = cursor.fetchone()
            
            # 获取最新CDC操作统计
            cursor.execute(f"""
                SELECT 
                    __op, 
                    COUNT(*) as count,
                    MAX(__ts_ms) as latest_ts
                FROM {table_name} 
                WHERE __op IS NOT NULL 
                GROUP BY __op
            """)
            op_stats = cursor.fetchall()
            
            return {
                'total_rows': table_stats['total_rows'],
                'operations': op_stats
            }
            
        except Exception as e:
            logger.error(f"❌ 获取统计信息失败: {e}")
            return None
        finally:
            conn.close()

def main():
    """主函数"""
    import argparse
    
    parser = argparse.ArgumentParser(description='Doris Routine Load管理工具')
    parser.add_argument('action', choices=['create-table', 'create-load', 'status', 'pause', 'resume', 'stats'],
                        help='执行的操作')
    parser.add_argument('--table', default='abc_warning', help='表名')
    
    args = parser.parse_args()
    
    manager = DorisRoutineLoadManager()
    
    if args.action == 'create-table':
        print("🏗️  创建Unique Key表...")
        if manager.create_unique_key_table(args.table):
            print("✅ 表创建成功!")
        else:
            print("❌ 表创建失败!")
    
    elif args.action == 'create-load':
        print("🚀 创建Routine Load任务...")
        if manager.create_routine_load_job(args.table):
            print("✅ Routine Load任务创建成功!")
        else:
            print("❌ Routine Load任务创建失败!")
    
    elif args.action == 'status':
        print("📊 查询Routine Load状态...")
        manager.check_routine_load_status(args.table)
    
    elif args.action == 'pause':
        print("⏸️  暂停Routine Load任务...")
        if manager.pause_routine_load(args.table):
            print("✅ 任务已暂停!")
        else:
            print("❌ 暂停失败!")
    
    elif args.action == 'resume':
        print("▶️  恢复Routine Load任务...")
        if manager.resume_routine_load(args.table):
            print("✅ 任务已恢复!")
        else:
            print("❌ 恢复失败!")
    
    elif args.action == 'stats':
        print("📈 获取加载统计...")
        stats = manager.get_load_statistics(args.table)
        if stats:
            print(f"总行数: {stats['total_rows']}")
            print("操作统计:")
            for op in stats['operations']:
                print(f"  {op['__op']}: {op['count']} 次")
        else:
            print("❌ 获取统计失败!")

if __name__ == '__main__':
    main() 