#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
CDC流水线端到端测试脚本
验证MySQL -> Debezium -> Kafka -> Doris的完整数据流
"""

import time
import random
import pymysql
import json
import logging
from datetime import datetime, date
from decimal import Decimal

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

class CDCPipelineTest:
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
        
        # Doris配置
        self.doris_config = {
            'host': '192.168.99.6',
            'port': 19030,
            'user': 'root',
            'password': '',
            'database': 'ods',
            'charset': 'utf8mb4'
        }
        
        self.test_student_id = f"TEST_{int(time.time())}"
        
    def connect_mysql(self):
        """连接MySQL"""
        return pymysql.connect(**self.mysql_config)
    
    def connect_doris(self):
        """连接Doris"""
        return pymysql.connect(**self.doris_config)
    
    def insert_test_record(self):
        """向MySQL插入测试记录"""
        try:
            conn = self.connect_mysql()
            cursor = conn.cursor()
            
            test_record = {
                'snapshot_date': date.today(),
                'student_id': self.test_student_id,
                'attendance_rate': round(random.uniform(0.7, 1.0), 4),
                'submit_rate': round(random.uniform(0.6, 1.0), 4),
                'violation_cnt': random.randint(0, 5),
                'core_fail_cnt': random.randint(0, 3),
                'risk_level': random.choice(['低', '中', '高'])
            }
            
            insert_sql = """
                INSERT INTO abc_warning 
                (snapshot_date, student_id, attendance_rate, submit_rate, 
                 violation_cnt, core_fail_cnt, risk_level)
                VALUES (%s, %s, %s, %s, %s, %s, %s)
            """
            
            cursor.execute(insert_sql, (
                test_record['snapshot_date'],
                test_record['student_id'],
                test_record['attendance_rate'],
                test_record['submit_rate'],
                test_record['violation_cnt'],
                test_record['core_fail_cnt'],
                test_record['risk_level']
            ))
            
            conn.commit()
            cursor.close()
            conn.close()
            
            logger.info(f"✅ 插入测试记录到MySQL: {test_record}")
            return test_record
            
        except Exception as e:
            logger.error(f"❌ 插入测试记录失败: {e}")
            return None
    
    def update_test_record(self):
        """更新MySQL中的测试记录"""
        try:
            conn = self.connect_mysql()
            cursor = conn.cursor()
            
            new_risk_level = random.choice(['低', '中', '高'])
            new_violation_cnt = random.randint(0, 10)
            
            update_sql = """
                UPDATE abc_warning 
                SET risk_level = %s, violation_cnt = %s
                WHERE student_id = %s AND snapshot_date = %s
            """
            
            cursor.execute(update_sql, (
                new_risk_level,
                new_violation_cnt,
                self.test_student_id,
                date.today()
            ))
            
            conn.commit()
            rows_affected = cursor.rowcount
            cursor.close()
            conn.close()
            
            if rows_affected > 0:
                logger.info(f"✅ 更新测试记录: risk_level={new_risk_level}, violation_cnt={new_violation_cnt}")
                return True
            else:
                logger.warning("⚠️  没有找到要更新的记录")
                return False
                
        except Exception as e:
            logger.error(f"❌ 更新测试记录失败: {e}")
            return False
    
    def delete_test_record(self):
        """删除MySQL中的测试记录"""
        try:
            conn = self.connect_mysql()
            cursor = conn.cursor()
            
            delete_sql = """
                DELETE FROM abc_warning 
                WHERE student_id = %s AND snapshot_date = %s
            """
            
            cursor.execute(delete_sql, (self.test_student_id, date.today()))
            conn.commit()
            rows_affected = cursor.rowcount
            cursor.close()
            conn.close()
            
            if rows_affected > 0:
                logger.info(f"✅ 删除测试记录: student_id={self.test_student_id}")
                return True
            else:
                logger.warning("⚠️  没有找到要删除的记录")
                return False
                
        except Exception as e:
            logger.error(f"❌ 删除测试记录失败: {e}")
            return False
    
    def check_doris_record(self, operation_type="INSERT", max_wait=60):
        """检查Doris中是否有对应的记录"""
        start_time = time.time()
        
        while time.time() - start_time < max_wait:
            try:
                conn = self.connect_doris()
                cursor = conn.cursor(pymysql.cursors.DictCursor)
                cursor.execute(f"USE {self.doris_config['database']}")
                
                # 查询测试记录
                select_sql = """
                    SELECT *, __op, __ts_ms 
                    FROM abc_warning 
                    WHERE student_id = %s AND snapshot_date = %s
                    ORDER BY __ts_ms DESC
                    LIMIT 1
                """
                
                cursor.execute(select_sql, (self.test_student_id, date.today()))
                result = cursor.fetchone()
                cursor.close()
                conn.close()
                
                if result:
                    logger.info(f"📊 在Doris中找到记录: {result}")
                    
                    if operation_type == "INSERT" and result.get('__op') in ['c', 'r']:  # create or read
                        logger.info(f"✅ {operation_type} 操作已同步到Doris")
                        return True
                    elif operation_type == "UPDATE" and result.get('__op') == 'u':  # update
                        logger.info(f"✅ {operation_type} 操作已同步到Doris")
                        return True
                    elif operation_type == "DELETE" and result.get('__op') == 'd':  # delete
                        logger.info(f"✅ {operation_type} 操作已同步到Doris")
                        return True
                else:
                    if operation_type == "DELETE":
                        logger.info("✅ DELETE 操作已同步到Doris (记录已被删除)")
                        return True
                
                logger.info(f"⏳ 等待 {operation_type} 操作同步到Doris... ({int(time.time() - start_time)}s)")
                time.sleep(3)
                
            except Exception as e:
                logger.error(f"❌ 检查Doris记录时出错: {e}")
                time.sleep(3)
        
        logger.error(f"❌ {operation_type} 操作在 {max_wait}s 内未同步到Doris")
        return False
    
    def test_insert_operation(self):
        """测试INSERT操作的CDC同步"""
        logger.info("🧪 测试INSERT操作...")
        
        # 1. 插入记录到MySQL
        record = self.insert_test_record()
        if not record:
            return False
        
        # 2. 等待同步到Doris
        return self.check_doris_record("INSERT", max_wait=30)
    
    def test_update_operation(self):
        """测试UPDATE操作的CDC同步"""
        logger.info("🧪 测试UPDATE操作...")
        
        # 1. 更新MySQL记录
        if not self.update_test_record():
            return False
        
        # 2. 等待同步到Doris
        return self.check_doris_record("UPDATE", max_wait=30)
    
    def test_delete_operation(self):
        """测试DELETE操作的CDC同步"""
        logger.info("🧪 测试DELETE操作...")
        
        # 1. 删除MySQL记录
        if not self.delete_test_record():
            return False
        
        # 2. 等待同步到Doris
        return self.check_doris_record("DELETE", max_wait=30)
    
    def cleanup_test_data(self):
        """清理测试数据"""
        logger.info("🧹 清理测试数据...")
        
        # 清理MySQL
        try:
            conn = self.connect_mysql()
            cursor = conn.cursor()
            cursor.execute("DELETE FROM abc_warning WHERE student_id = %s", (self.test_student_id,))
            conn.commit()
            cursor.close()
            conn.close()
            logger.info("✅ MySQL测试数据已清理")
        except Exception as e:
            logger.error(f"❌ 清理MySQL测试数据失败: {e}")
        
        # 清理Doris
        try:
            conn = self.connect_doris()
            cursor = conn.cursor()
            cursor.execute(f"USE {self.doris_config['database']}")
            cursor.execute("DELETE FROM abc_warning WHERE student_id = %s", (self.test_student_id,))
            cursor.close()
            conn.close()
            logger.info("✅ Doris测试数据已清理")
        except Exception as e:
            logger.error(f"❌ 清理Doris测试数据失败: {e}")
    
    def get_sync_latency_stats(self):
        """获取同步延迟统计"""
        try:
            conn = self.connect_doris()
            cursor = conn.cursor(pymysql.cursors.DictCursor)
            cursor.execute(f"USE {self.doris_config['database']}")
            
            # 获取最近的CDC操作
            cursor.execute("""
                SELECT 
                    __op,
                    __ts_ms,
                    NOW() as current_time,
                    (UNIX_TIMESTAMP(NOW()) * 1000 - __ts_ms) as latency_ms
                FROM abc_warning 
                WHERE __ts_ms IS NOT NULL 
                ORDER BY __ts_ms DESC 
                LIMIT 10
            """)
            
            results = cursor.fetchall()
            cursor.close()
            conn.close()
            
            if results:
                logger.info("📈 同步延迟统计 (最近10条记录):")
                for result in results:
                    latency_ms = result.get('latency_ms', 0)
                    latency_seconds = latency_ms / 1000 if latency_ms else 0
                    logger.info(f"   操作: {result['__op']}, 延迟: {latency_seconds:.2f}s")
                
                avg_latency = sum(r.get('latency_ms', 0) for r in results) / len(results) / 1000
                logger.info(f"📊 平均同步延迟: {avg_latency:.2f}s")
                return avg_latency
            else:
                logger.info("📊 没有找到CDC记录")
                return None
                
        except Exception as e:
            logger.error(f"❌ 获取同步延迟统计失败: {e}")
            return None
    
    def run_full_test(self):
        """运行完整的CDC测试"""
        logger.info("🚀 开始CDC流水线端到端测试")
        logger.info(f"🆔 测试ID: {self.test_student_id}")
        
        test_results = {
            'insert': False,
            'update': False,
            'delete': False,
            'latency': None
        }
        
        try:
            # 测试INSERT
            test_results['insert'] = self.test_insert_operation()
            time.sleep(2)
            
            # 测试UPDATE
            if test_results['insert']:
                test_results['update'] = self.test_update_operation()
                time.sleep(2)
            
            # 获取延迟统计
            test_results['latency'] = self.get_sync_latency_stats()
            
            # 测试DELETE
            if test_results['update']:
                test_results['delete'] = self.test_delete_operation()
            
        except Exception as e:
            logger.error(f"❌ 测试过程中出错: {e}")
        
        finally:
            # 清理测试数据
            self.cleanup_test_data()
        
        # 打印测试结果
        logger.info("📋 CDC测试结果:")
        logger.info(f"   INSERT同步: {'✅' if test_results['insert'] else '❌'}")
        logger.info(f"   UPDATE同步: {'✅' if test_results['update'] else '❌'}")
        logger.info(f"   DELETE同步: {'✅' if test_results['delete'] else '❌'}")
        
        if test_results['latency']:
            logger.info(f"   平均延迟: {test_results['latency']:.2f}s")
        
        all_passed = all([test_results['insert'], test_results['update'], test_results['delete']])
        
        if all_passed:
            logger.info("🎉 所有CDC测试通过！")
        else:
            logger.error("💥 部分CDC测试失败！")
        
        return test_results

def main():
    """主函数"""
    import argparse
    
    parser = argparse.ArgumentParser(description='CDC流水线测试工具')
    parser.add_argument('--test-type', choices=['full', 'insert', 'update', 'delete', 'latency'],
                        default='full', help='测试类型')
    parser.add_argument('--wait-time', type=int, default=30, help='等待同步的最大时间(秒)')
    
    args = parser.parse_args()
    
    tester = CDCPipelineTest()
    
    if args.test_type == 'full':
        results = tester.run_full_test()
        exit(0 if all(results[k] for k in ['insert', 'update', 'delete']) else 1)
    elif args.test_type == 'insert':
        success = tester.test_insert_operation()
        tester.cleanup_test_data()
        exit(0 if success else 1)
    elif args.test_type == 'update':
        tester.insert_test_record()
        success = tester.test_update_operation()
        tester.cleanup_test_data()
        exit(0 if success else 1)
    elif args.test_type == 'delete':
        tester.insert_test_record()
        success = tester.test_delete_operation()
        tester.cleanup_test_data()
        exit(0 if success else 1)
    elif args.test_type == 'latency':
        tester.get_sync_latency_stats()

if __name__ == '__main__':
    main() 