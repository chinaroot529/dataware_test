#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
云端CDC系统部署脚本
适配现有Kafka环境的CDC流水线部署
"""

import requests
import json
import time
import subprocess
import logging
import pymysql
from datetime import datetime, timedelta
from typing import Dict, List, Any

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

class CloudCDCManager:
    def __init__(self):
        self.kafka_connect_url = "http://localhost:8083"
        self.kafka_ui_url = "http://localhost:8080"
        self.debezium_ui_url = "http://localhost:8082"
        
        # 云端Kafka配置
        self.kafka_broker = "192.168.99.6:9092"
        
        self.doris_config = {
            'host': '192.168.99.6',
            'port': 19030,
            'user': 'root',
            'password': '',
            'database': 'ods',
            'charset': 'utf8mb4'
        }
        
        self.mysql_config = {
            'host': '10.10.0.117',
            'port': 6033,
            'user': 'root',
            'password': 'Xml123&45!',
            'database': 'data_ware_test',
            'charset': 'utf8mb4'
        }
    
    def start_cdc_services(self):
        """启动CDC服务（使用云端配置）"""
        try:
            logger.info("🚀 启动CDC服务...")
            cmd = ["docker-compose", "-f", "docker-compose.cloud.yml", "up", "-d"]
            result = subprocess.run(cmd, capture_output=True, text=True, cwd=".")
            
            if result.returncode == 0:
                logger.info("✅ CDC服务启动成功!")
                logger.info("📋 服务地址:")
                logger.info("   Kafka UI: http://192.168.99.6:8080")
                logger.info("   Debezium UI: http://192.168.99.6:8082")
                logger.info("   Kafka Connect API: http://192.168.99.6:8083")
                logger.info("   Prometheus: http://192.168.99.6:9090")
                logger.info("   Grafana: http://192.168.99.6:3000 (admin/admin)")
                
                # 等待服务启动
                self.wait_for_services()
                return True
            else:
                logger.error(f"❌ CDC服务启动失败: {result.stderr}")
                return False
                
        except Exception as e:
            logger.error(f"❌ 启动CDC服务时出错: {e}")
            return False
    
    def stop_cdc_services(self):
        """停止CDC服务"""
        try:
            logger.info("🛑 停止CDC服务...")
            cmd = ["docker-compose", "-f", "docker-compose.cloud.yml", "down"]
            result = subprocess.run(cmd, capture_output=True, text=True, cwd=".")
            
            if result.returncode == 0:
                logger.info("✅ CDC服务已停止!")
                return True
            else:
                logger.error(f"❌ 停止CDC服务失败: {result.stderr}")
                return False
                
        except Exception as e:
            logger.error(f"❌ 停止CDC服务时出错: {e}")
            return False
    
    def wait_for_services(self, timeout=300):
        """等待服务启动完成"""
        logger.info("⏳ 等待服务启动完成...")
        
        services = {
            "Kafka Connect": "http://localhost:8083",
            "Kafka UI": "http://localhost:8080",
            "Debezium UI": "http://localhost:8082"
        }
        
        start_time = time.time()
        for service_name, url in services.items():
            logger.info(f"🔍 等待 {service_name} 启动...")
            while time.time() - start_time < timeout:
                try:
                    response = requests.get(url, timeout=5)
                    if response.status_code == 200:
                        logger.info(f"✅ {service_name} 已启动")
                        break
                except requests.exceptions.RequestException:
                    pass
                time.sleep(5)
            else:
                logger.warning(f"⚠️  {service_name} 启动超时")
        
        logger.info("🎉 服务启动完成!")
    
    def deploy_mysql_connector(self):
        """部署MySQL CDC连接器（云端版本）"""
        try:
            # 读取云端连接器配置
            with open('config/mysql-connector-cloud.json', 'r') as f:
                connector_config = json.load(f)
            
            # 发送到Kafka Connect
            url = f"{self.kafka_connect_url}/connectors"
            headers = {"Content-Type": "application/json"}
            
            logger.info("🔗 部署MySQL CDC连接器...")
            response = requests.post(url, headers=headers, json=connector_config)
            
            if response.status_code in [200, 201]:
                logger.info("✅ MySQL CDC连接器部署成功!")
                return True
            else:
                logger.error(f"❌ MySQL CDC连接器部署失败: {response.status_code} - {response.text}")
                return False
                
        except Exception as e:
            logger.error(f"❌ 部署MySQL CDC连接器时出错: {e}")
            return False
    
    def check_kafka_connection(self):
        """检查Kafka连接"""
        try:
            # 使用docker exec检查Kafka主题
            cmd = [
                "docker", "exec", "-i", "kafka-connect",
                "kafka-topics", "--bootstrap-server", self.kafka_broker, "--list"
            ]
            result = subprocess.run(cmd, capture_output=True, text=True)
            
            if result.returncode == 0:
                topics = result.stdout.strip().split('\n')
                logger.info(f"📋 Kafka主题 ({self.kafka_broker}): {topics}")
                return True
            else:
                logger.error(f"❌ 连接Kafka失败: {result.stderr}")
                return False
                
        except Exception as e:
            logger.error(f"❌ 检查Kafka连接时出错: {e}")
            return False
    
    def create_doris_routine_load(self):
        """创建Doris Routine Load（使用云端Kafka）"""
        try:
            conn = pymysql.connect(**self.doris_config)
            cursor = conn.cursor()
            cursor.execute(f"USE {self.doris_config['database']}")
            
            # 停止已存在的同名任务
            try:
                stop_sql = "STOP ROUTINE LOAD FOR abc_warning_cdc_load"
                cursor.execute(stop_sql)
                logger.info("🛑 已停止存在的Routine Load任务")
                time.sleep(5)
            except Exception as e:
                logger.info(f"ℹ️  没有需要停止的任务: {e}")
            
            # 创建新的Routine Load任务（使用云端Kafka地址）
            routine_load_sql = f"""
CREATE ROUTINE LOAD abc_warning_cdc_load ON abc_warning
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
    "kafka_broker_list" = "{self.kafka_broker}",
    "kafka_topic" = "abc_warning",
    "property.group.id" = "doris_cdc_consumer",
    "property.client.id" = "doris_cdc_client",
    "property.kafka_default_offsets" = "OFFSET_BEGINNING",
    "property.enable.auto.commit" = "true",
    "property.auto.commit.interval.ms" = "5000",
    "property.session.timeout.ms" = "30000",
    "property.request.timeout.ms" = "60000",
    "property.max.poll.records" = "1000"
)
"""
            
            logger.info("🚀 正在创建Routine Load任务...")
            cursor.execute(routine_load_sql)
            logger.info("✅ Routine Load任务创建成功!")
            
            cursor.close()
            conn.close()
            return True
            
        except Exception as e:
            logger.error(f"❌ 创建Routine Load任务失败: {e}")
            return False
    
    def health_check(self):
        """健康检查"""
        logger.info("🏥 CDC流水线健康检查")
        
        health_status = {
            'kafka_connection': False,
            'kafka_connect': False,
            'connector': False,
            'doris_connection': False
        }
        
        # 检查Kafka连接
        health_status['kafka_connection'] = self.check_kafka_connection()
        
        # 检查Kafka Connect
        try:
            response = requests.get(f"{self.kafka_connect_url}/connectors")
            health_status['kafka_connect'] = response.status_code == 200
        except:
            pass
        
        # 检查连接器状态
        try:
            response = requests.get(f"{self.kafka_connect_url}/connectors")
            if response.status_code == 200:
                connectors = response.json()
                health_status['connector'] = len(connectors) > 0
        except:
            pass
        
        # 检查Doris连接
        try:
            conn = pymysql.connect(**self.doris_config)
            conn.close()
            health_status['doris_connection'] = True
        except:
            pass
        
        # 打印健康检查结果
        logger.info("🎯 健康检查结果:")
        for component, status in health_status.items():
            status_icon = "✅" if status else "❌"
            logger.info(f"   {component}: {status_icon}")
        
        return health_status

def main():
    """主函数"""
    import argparse
    
    parser = argparse.ArgumentParser(description='云端CDC管理工具')
    parser.add_argument('action', choices=[
        'start', 'stop', 'deploy-connector', 'create-routine-load', 'health', 'check-kafka'
    ], help='执行的操作')
    
    args = parser.parse_args()
    manager = CloudCDCManager()
    
    if args.action == 'start':
        print("🚀 启动CDC服务...")
        if manager.start_cdc_services():
            print("✅ CDC服务启动成功!")
        else:
            print("❌ CDC服务启动失败!")
    
    elif args.action == 'stop':
        print("🛑 停止CDC服务...")
        if manager.stop_cdc_services():
            print("✅ CDC服务已停止!")
        else:
            print("❌ CDC服务停止失败!")
    
    elif args.action == 'deploy-connector':
        print("🔗 部署MySQL CDC连接器...")
        if manager.deploy_mysql_connector():
            print("✅ MySQL CDC连接器部署成功!")
        else:
            print("❌ MySQL CDC连接器部署失败!")
    
    elif args.action == 'create-routine-load':
        print("🚀 创建Doris Routine Load...")
        if manager.create_doris_routine_load():
            print("✅ Routine Load创建成功!")
        else:
            print("❌ Routine Load创建失败!")
    
    elif args.action == 'health':
        print("🏥 CDC流水线健康检查...")
        manager.health_check()
    
    elif args.action == 'check-kafka':
        print("📋 检查Kafka连接...")
        manager.check_kafka_connection()

if __name__ == '__main__':
    main() 