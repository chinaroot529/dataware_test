#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
CDC管理和监控工具
统一管理Debezium + Kafka + Doris的CDC流水线
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

class CDCManager:
    def __init__(self):
        self.kafka_connect_url = "http://localhost:8083"
        self.kafka_ui_url = "http://localhost:8080"
        self.debezium_ui_url = "http://localhost:8082"
        
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
    
    def start_cdc_stack(self):
        """启动CDC技术栈"""
        try:
            logger.info("🚀 启动CDC技术栈...")
            cmd = ["docker-compose", "-f", "cdc/docker-compose.cdc.yml", "up", "-d"]
            result = subprocess.run(cmd, capture_output=True, text=True, cwd=".")
            
            if result.returncode == 0:
                logger.info("✅ CDC技术栈启动成功!")
                logger.info("📋 服务地址:")
                logger.info("   Kafka UI: http://localhost:8080")
                logger.info("   Debezium UI: http://localhost:8082")
                logger.info("   Kafka Connect API: http://localhost:8083")
                logger.info("   Prometheus: http://localhost:9090")
                logger.info("   Grafana: http://localhost:3000 (admin/admin)")
                
                # 等待服务启动
                self.wait_for_services()
                return True
            else:
                logger.error(f"❌ CDC技术栈启动失败: {result.stderr}")
                return False
                
        except Exception as e:
            logger.error(f"❌ 启动CDC技术栈时出错: {e}")
            return False
    
    def stop_cdc_stack(self):
        """停止CDC技术栈"""
        try:
            logger.info("🛑 停止CDC技术栈...")
            cmd = ["docker-compose", "-f", "cdc/docker-compose.cdc.yml", "down"]
            result = subprocess.run(cmd, capture_output=True, text=True, cwd=".")
            
            if result.returncode == 0:
                logger.info("✅ CDC技术栈已停止!")
                return True
            else:
                logger.error(f"❌ 停止CDC技术栈失败: {result.stderr}")
                return False
                
        except Exception as e:
            logger.error(f"❌ 停止CDC技术栈时出错: {e}")
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
        """部署MySQL CDC连接器"""
        try:
            # 读取连接器配置
            with open('cdc/config/mysql-connector.json', 'r') as f:
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
    
    def check_connector_status(self):
        """检查连接器状态"""
        try:
            url = f"{self.kafka_connect_url}/connectors"
            response = requests.get(url)
            
            if response.status_code == 200:
                connectors = response.json()
                logger.info(f"📊 当前连接器: {connectors}")
                
                for connector in connectors:
                    status_url = f"{self.kafka_connect_url}/connectors/{connector}/status"
                    status_response = requests.get(status_url)
                    
                    if status_response.status_code == 200:
                        status = status_response.json()
                        logger.info(f"🔍 连接器 {connector} 状态:")
                        logger.info(f"   状态: {status['connector']['state']}")
                        logger.info(f"   任务数: {len(status['tasks'])}")
                        
                        for i, task in enumerate(status['tasks']):
                            logger.info(f"   任务{i}: {task['state']}")
                            if task['state'] == 'FAILED':
                                logger.error(f"     错误: {task.get('trace', 'N/A')}")
                
                return True
            else:
                logger.error(f"❌ 获取连接器状态失败: {response.status_code}")
                return False
                
        except Exception as e:
            logger.error(f"❌ 检查连接器状态时出错: {e}")
            return False
    
    def get_kafka_topics(self):
        """获取Kafka主题列表"""
        try:
            # 使用docker exec调用kafka命令
            cmd = [
                "docker", "exec", "kafka",
                "kafka-topics", "--bootstrap-server", "localhost:9092", "--list"
            ]
            result = subprocess.run(cmd, capture_output=True, text=True)
            
            if result.returncode == 0:
                topics = result.stdout.strip().split('\n')
                logger.info(f"📋 Kafka主题: {topics}")
                return topics
            else:
                logger.error(f"❌ 获取Kafka主题失败: {result.stderr}")
                return []
                
        except Exception as e:
            logger.error(f"❌ 获取Kafka主题时出错: {e}")
            return []
    
    def check_mysql_binlog_status(self):
        """检查MySQL binlog状态"""
        try:
            conn = pymysql.connect(**self.mysql_config)
            cursor = conn.cursor(pymysql.cursors.DictCursor)
            
            # 检查binlog是否启用
            cursor.execute("SHOW VARIABLES LIKE 'log_bin'")
            log_bin = cursor.fetchone()
            
            cursor.execute("SHOW VARIABLES LIKE 'binlog_format'")
            binlog_format = cursor.fetchone()
            
            cursor.execute("SHOW VARIABLES LIKE 'binlog_row_image'")
            binlog_row_image = cursor.fetchone()
            
            cursor.execute("SHOW MASTER STATUS")
            master_status = cursor.fetchone()
            
            logger.info("📊 MySQL Binlog状态:")
            logger.info(f"   Log_bin: {log_bin['Value'] if log_bin else 'N/A'}")
            logger.info(f"   Binlog_format: {binlog_format['Value'] if binlog_format else 'N/A'}")
            logger.info(f"   Binlog_row_image: {binlog_row_image['Value'] if binlog_row_image else 'N/A'}")
            logger.info(f"   当前binlog文件: {master_status['File'] if master_status else 'N/A'}")
            logger.info(f"   当前位置: {master_status['Position'] if master_status else 'N/A'}")
            
            cursor.close()
            conn.close()
            return True
            
        except Exception as e:
            logger.error(f"❌ 检查MySQL binlog状态时出错: {e}")
            return False
    
    def get_doris_routine_load_status(self):
        """获取Doris Routine Load状态"""
        try:
            conn = pymysql.connect(**self.doris_config)
            cursor = conn.cursor(pymysql.cursors.DictCursor)
            cursor.execute(f"USE {self.doris_config['database']}")
            
            cursor.execute("SHOW ROUTINE LOAD")
            loads = cursor.fetchall()
            
            logger.info("📊 Doris Routine Load状态:")
            for load in loads:
                logger.info(f"   任务: {load['Name']}")
                logger.info(f"   状态: {load['State']}")
                logger.info(f"   进度: {load.get('Progress', 'N/A')}")
                logger.info(f"   数据源: {load.get('DataSourceType', 'N/A')}")
            
            cursor.close()
            conn.close()
            return loads
            
        except Exception as e:
            logger.error(f"❌ 获取Doris Routine Load状态时出错: {e}")
            return []
    
    def get_end_to_end_stats(self):
        """获取端到端统计信息"""
        try:
            # MySQL统计
            mysql_conn = pymysql.connect(**self.mysql_config)
            mysql_cursor = mysql_conn.cursor(pymysql.cursors.DictCursor)
            mysql_cursor.execute("SELECT COUNT(*) as count FROM abc_warning")
            mysql_count = mysql_cursor.fetchone()['count']
            mysql_cursor.close()
            mysql_conn.close()
            
            # Doris统计
            doris_conn = pymysql.connect(**self.doris_config)
            doris_cursor = doris_conn.cursor(pymysql.cursors.DictCursor)
            doris_cursor.execute(f"USE {self.doris_config['database']}")
            doris_cursor.execute("SELECT COUNT(*) as count FROM abc_warning")
            doris_count = doris_cursor.fetchone()['count']
            doris_cursor.close()
            doris_conn.close()
            
            logger.info("📈 端到端统计:")
            logger.info(f"   MySQL记录数: {mysql_count}")
            logger.info(f"   Doris记录数: {doris_count}")
            logger.info(f"   同步差异: {mysql_count - doris_count}")
            
            return {
                'mysql_count': mysql_count,
                'doris_count': doris_count,
                'sync_diff': mysql_count - doris_count
            }
            
        except Exception as e:
            logger.error(f"❌ 获取端到端统计时出错: {e}")
            return None
    
    def health_check(self):
        """CDC流水线健康检查"""
        logger.info("🏥 CDC流水线健康检查")
        
        health_status = {
            'mysql_binlog': False,
            'kafka_connect': False,
            'connector': False,
            'kafka_topics': False,
            'doris_routine_load': False,
            'end_to_end_sync': False
        }
        
        # 检查MySQL binlog
        health_status['mysql_binlog'] = self.check_mysql_binlog_status()
        
        # 检查Kafka Connect
        try:
            response = requests.get(f"{self.kafka_connect_url}/connectors")
            health_status['kafka_connect'] = response.status_code == 200
        except:
            pass
        
        # 检查连接器状态
        health_status['connector'] = self.check_connector_status()
        
        # 检查Kafka主题
        topics = self.get_kafka_topics()
        health_status['kafka_topics'] = len(topics) > 0
        
        # 检查Doris Routine Load
        loads = self.get_doris_routine_load_status()
        health_status['doris_routine_load'] = len(loads) > 0
        
        # 检查端到端同步
        stats = self.get_end_to_end_stats()
        health_status['end_to_end_sync'] = stats and stats['sync_diff'] < 1000
        
        # 打印健康检查结果
        logger.info("🎯 健康检查结果:")
        for component, status in health_status.items():
            status_icon = "✅" if status else "❌"
            logger.info(f"   {component}: {status_icon}")
        
        return health_status

def main():
    """主函数"""
    import argparse
    
    parser = argparse.ArgumentParser(description='CDC管理工具')
    parser.add_argument('action', choices=[
        'start', 'stop', 'deploy-connector', 'status', 'health', 'topics', 'stats'
    ], help='执行的操作')
    
    args = parser.parse_args()
    manager = CDCManager()
    
    if args.action == 'start':
        print("🚀 启动CDC技术栈...")
        if manager.start_cdc_stack():
            print("✅ CDC技术栈启动成功!")
        else:
            print("❌ CDC技术栈启动失败!")
    
    elif args.action == 'stop':
        print("🛑 停止CDC技术栈...")
        if manager.stop_cdc_stack():
            print("✅ CDC技术栈已停止!")
        else:
            print("❌ CDC技术栈停止失败!")
    
    elif args.action == 'deploy-connector':
        print("🔗 部署MySQL CDC连接器...")
        if manager.deploy_mysql_connector():
            print("✅ MySQL CDC连接器部署成功!")
        else:
            print("❌ MySQL CDC连接器部署失败!")
    
    elif args.action == 'status':
        print("📊 检查连接器状态...")
        manager.check_connector_status()
    
    elif args.action == 'health':
        print("🏥 CDC流水线健康检查...")
        manager.health_check()
    
    elif args.action == 'topics':
        print("📋 获取Kafka主题...")
        manager.get_kafka_topics()
    
    elif args.action == 'stats':
        print("📈 获取端到端统计...")
        manager.get_end_to_end_stats()

if __name__ == '__main__':
    main() 