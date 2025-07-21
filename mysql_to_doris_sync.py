#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
MySQL到Doris数据同步工具
功能：
1. 全量导入MySQL表到Doris
2. 设置CDC增量同步
"""

import pymysql
import pandas as pd
import requests
import json
import time
from datetime import datetime
import logging

# 配置日志
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

class MySQLToDorisSync:
    def __init__(self):
        # MySQL配置
        self.mysql_config = {
            'host': '10.10.0.117',
            'port': 6033,
            'user': 'root',
            'password': 'Xml123&45!',
            'database': 'data_ware_test',
            'charset': 'utf8mb4'
        }
        
        # Doris配置（远程服务器）
        self.doris_config = {
            'fe_host': '192.168.99.6',
            'fe_http_port': 18030,  # 对应docker.yml中的端口映射
            'fe_query_port': 19030,  # MySQL协议端口
            'username': 'root',
            'password': '',  # Doris默认root用户无密码
            'database': 'ods'  # 数据仓库ODS层
        }
        
    def connect_mysql(self):
        """连接MySQL数据库"""
        try:
            conn = pymysql.connect(**self.mysql_config)
            logger.info("MySQL连接成功")
            return conn
        except Exception as e:
            logger.error(f"MySQL连接失败: {e}")
            return None
    
    def connect_doris(self):
        """连接Doris数据库"""
        try:
            import pymysql
            conn = pymysql.connect(
                host=self.doris_config['fe_host'],
                port=self.doris_config['fe_query_port'],
                user=self.doris_config['username'],
                password=self.doris_config['password'],
                charset='utf8mb4'
            )
            logger.info("Doris连接成功")
            return conn
        except Exception as e:
            logger.error(f"Doris连接失败: {e}")
            return None
    
    def get_table_schema(self, table_name):
        """获取MySQL表结构"""
        mysql_conn = self.connect_mysql()
        if not mysql_conn:
            return None
            
        try:
            cursor = mysql_conn.cursor(pymysql.cursors.DictCursor)
            cursor.execute(f"DESCRIBE {table_name}")
            schema = cursor.fetchall()
            
            cursor.execute(f"SELECT COUNT(*) as count FROM {table_name}")
            count = cursor.fetchone()['count']
            
            logger.info(f"表 {table_name} 结构获取成功，共 {count} 条记录")
            return schema, count
        except Exception as e:
            logger.error(f"获取表结构失败: {e}")
            return None, 0
        finally:
            mysql_conn.close()
    
    def mysql_to_doris_type(self, mysql_type):
        """MySQL数据类型转换为Doris数据类型"""
        type_mapping = {
            'int': 'INT',
            'bigint': 'BIGINT',
            'varchar': 'VARCHAR',
            'text': 'TEXT',
            'datetime': 'DATETIME',
            'timestamp': 'DATETIME',
            'date': 'DATE',
            'decimal': 'DECIMAL',
            'float': 'FLOAT',
            'double': 'DOUBLE',
            'tinyint': 'TINYINT',
            'smallint': 'SMALLINT',
            'mediumint': 'INT',
            'longtext': 'TEXT',
            'json': 'JSON'
        }
        
        # 提取基础类型名
        base_type = mysql_type.split('(')[0].lower()
        return type_mapping.get(base_type, 'VARCHAR(500)')
    
    def create_doris_table(self, table_name, mysql_schema):
        """在Doris中创建对应的表"""
        doris_conn = self.connect_doris()
        if not doris_conn:
            return False
            
        try:
            cursor = doris_conn.cursor()
            
            # 创建数据库（如果不存在）
            cursor.execute(f"CREATE DATABASE IF NOT EXISTS {self.doris_config['database']}")
            cursor.execute(f"USE {self.doris_config['database']}")
            
            # 构建CREATE TABLE语句
            columns = []
            key_columns = []
            
            for field in mysql_schema:
                field_name = field['Field']
                mysql_type = field['Type']
                is_null = field['Null'] == 'YES'
                is_key = field['Key'] in ('PRI', 'UNI')
                
                doris_type = self.mysql_to_doris_type(mysql_type)
                
                # 处理VARCHAR长度
                if 'varchar' in mysql_type.lower():
                    doris_type = mysql_type.upper().replace('VARCHAR', 'VARCHAR')
                
                null_str = '' if is_null else ' NOT NULL'
                columns.append(f"`{field_name}` {doris_type}{null_str}")
                
                if is_key:
                    key_columns.append(f"`{field_name}`")
            
            # 如果没有主键，使用第一个字段作为key
            if not key_columns and columns:
                first_field = mysql_schema[0]['Field']
                key_columns.append(f"`{first_field}`")
            
            create_sql = f"""
CREATE TABLE IF NOT EXISTS `{table_name}` (
{', '.join(columns)}
) DUPLICATE KEY({', '.join(key_columns)})
DISTRIBUTED BY HASH({', '.join(key_columns)}) BUCKETS 10
PROPERTIES (
    "replication_allocation" = "tag.location.default: 1"
)
"""
            
            logger.info(f"创建Doris表SQL: {create_sql}")
            cursor.execute(create_sql)
            logger.info(f"Doris表 {table_name} 创建成功")
            return True
            
        except Exception as e:
            logger.error(f"创建Doris表失败: {e}")
            return False
        finally:
            doris_conn.close()
    
    def full_import_data(self, table_name, batch_size=1000):
        """全量导入数据"""
        mysql_conn = self.connect_mysql()
        doris_conn = self.connect_doris()
        
        if not mysql_conn or not doris_conn:
            return False
            
        try:
            # 使用pandas分批读取MySQL数据
            mysql_cursor = mysql_conn.cursor(pymysql.cursors.DictCursor)
            mysql_cursor.execute(f"SELECT COUNT(*) as total FROM {table_name}")
            total_rows = mysql_cursor.fetchone()['total']
            
            logger.info(f"开始全量导入 {table_name}，总计 {total_rows} 条记录")
            
            doris_cursor = doris_conn.cursor()
            doris_cursor.execute(f"USE {self.doris_config['database']}")
            
            # 分批处理
            offset = 0
            imported_rows = 0
            
            while offset < total_rows:
                # 读取一批数据
                mysql_cursor.execute(f"""
                    SELECT * FROM {table_name} 
                    LIMIT {batch_size} OFFSET {offset}
                """)
                batch_data = mysql_cursor.fetchall()
                
                if not batch_data:
                    break
                
                # 构建INSERT语句
                if batch_data:
                    columns = list(batch_data[0].keys())
                    placeholders = ', '.join(['%s'] * len(columns))
                    insert_sql = f"""
                        INSERT INTO {table_name} ({', '.join(f'`{col}`' for col in columns)}) 
                        VALUES ({placeholders})
                    """
                    
                    # 准备数据
                    values = []
                    for row in batch_data:
                        row_values = []
                        for col in columns:
                            value = row[col]
                            if isinstance(value, datetime):
                                value = value.strftime('%Y-%m-%d %H:%M:%S')
                            row_values.append(value)
                        values.append(tuple(row_values))
                    
                    # 批量插入
                    doris_cursor.executemany(insert_sql, values)
                    imported_rows += len(batch_data)
                    
                    logger.info(f"已导入 {imported_rows}/{total_rows} 条记录")
                
                offset += batch_size
                
            logger.info(f"全量导入完成，共导入 {imported_rows} 条记录")
            return True
            
        except Exception as e:
            logger.error(f"全量导入失败: {e}")
            return False
        finally:
            if mysql_conn:
                mysql_conn.close()
            if doris_conn:
                doris_conn.close()
    
    def setup_cdc_sync(self, table_name):
        """设置CDC增量同步（使用Flink CDC）"""
        # 这里提供CDC设置的配置建议
        cdc_config = {
            "flink_sql": f"""
            -- 创建MySQL CDC源表
            CREATE TABLE mysql_{table_name}_source (
                -- 这里需要根据实际表结构填写字段
            ) WITH (
                'connector' = 'mysql-cdc',
                'hostname' = '{self.mysql_config['host']}',
                'port' = '{self.mysql_config['port']}',
                'username' = '{self.mysql_config['user']}',
                'password' = '{self.mysql_config['password']}',
                'database-name' = '{self.mysql_config['database']}',
                'table-name' = '{table_name}'
            );
            
            -- 创建Doris sink表
            CREATE TABLE doris_{table_name}_sink (
                -- 这里需要根据实际表结构填写字段
            ) WITH (
                'connector' = 'doris',
                'fenodes' = '{self.doris_config['fe_host']}:{self.doris_config['fe_http_port']}',
                'table.identifier' = '{self.doris_config['database']}.{table_name}',
                'username' = '{self.doris_config['username']}',
                'password' = '{self.doris_config['password']}'
            );
            
            -- 启动CDC同步
            INSERT INTO doris_{table_name}_sink SELECT * FROM mysql_{table_name}_source;
            """
        }
        
        logger.info("CDC配置生成完成，需要在Flink中执行以下SQL:")
        logger.info(cdc_config["flink_sql"])
        
        return cdc_config
    
    def sync_table(self, table_name):
        """同步指定表的完整流程"""
        logger.info(f"开始同步表: {table_name}")
        
        # 1. 获取表结构
        schema, row_count = self.get_table_schema(table_name)
        if not schema:
            logger.error("获取表结构失败")
            return False
        
        # 2. 在Doris中创建表
        if not self.create_doris_table(table_name, schema):
            logger.error("创建Doris表失败")
            return False
        
        # 3. 全量导入数据
        if not self.full_import_data(table_name):
            logger.error("全量导入失败")
            return False
        
        # 4. 设置CDC配置
        cdc_config = self.setup_cdc_sync(table_name)
        
        logger.info(f"表 {table_name} 同步设置完成")
        return True

def main():
    """主函数"""
    sync_tool = MySQLToDorisSync()
    table_name = "abc_warning"
    
    # 执行同步
    success = sync_tool.sync_table(table_name)
    
    if success:
        print(f"\n✅ 表 {table_name} 同步设置成功!")
        print("\n📋 后续步骤:")
        print("1. 全量数据已导入Doris")
        print("2. 请在Flink中执行生成的CDC SQL配置")
        print("3. 验证增量同步是否正常工作")
    else:
        print(f"\n❌ 表 {table_name} 同步设置失败，请检查日志")

if __name__ == "__main__":
    main() 