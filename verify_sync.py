#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
数据同步验证脚本
验证MySQL数据是否正确同步到Doris
"""

import pymysql
import pandas as pd
from datetime import datetime

class SyncVerifier:
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
    
    def get_mysql_data(self, table_name):
        """获取MySQL中的数据"""
        try:
            conn = pymysql.connect(**self.mysql_config)
            cursor = conn.cursor(pymysql.cursors.DictCursor)
            
            cursor.execute(f"SELECT COUNT(*) as count FROM {table_name}")
            count = cursor.fetchone()['count']
            
            cursor.execute(f"SELECT * FROM {table_name} ORDER BY snapshot_date, student_id LIMIT 5")
            sample_data = cursor.fetchall()
            
            conn.close()
            return count, sample_data
        except Exception as e:
            print(f"获取MySQL数据失败: {e}")
            return 0, []
    
    def get_doris_data(self, table_name):
        """获取Doris中的数据"""
        try:
            conn = pymysql.connect(**self.doris_config)
            cursor = conn.cursor(pymysql.cursors.DictCursor)
            
            cursor.execute(f"USE {self.doris_config['database']}")
            cursor.execute(f"SELECT COUNT(*) as count FROM {table_name}")
            count = cursor.fetchone()['count']
            
            cursor.execute(f"SELECT * FROM {table_name} ORDER BY snapshot_date, student_id LIMIT 5")
            sample_data = cursor.fetchall()
            
            conn.close()
            return count, sample_data
        except Exception as e:
            print(f"获取Doris数据失败: {e}")
            return 0, []
    
    def verify_table(self, table_name):
        """验证表数据同步"""
        print(f"🔍 验证表 {table_name} 的数据同步...")
        
        # 获取MySQL数据
        mysql_count, mysql_sample = self.get_mysql_data(table_name)
        print(f"📊 MySQL记录数: {mysql_count}")
        
        # 获取Doris数据
        doris_count, doris_sample = self.get_doris_data(table_name)
        print(f"📊 Doris记录数: {doris_count}")
        
        # 对比记录数
        if mysql_count == doris_count:
            print(f"✅ 记录数一致: {mysql_count}")
        else:
            print(f"❌ 记录数不一致! MySQL: {mysql_count}, Doris: {doris_count}")
        
        # 对比样本数据
        print(f"\n📋 MySQL样本数据 (前5条):")
        for i, row in enumerate(mysql_sample, 1):
            print(f"  {i}. {dict(row)}")
        
        print(f"\n📋 Doris样本数据 (前5条):")
        for i, row in enumerate(doris_sample, 1):
            print(f"  {i}. {dict(row)}")
        
        # 简单对比第一条记录
        if mysql_sample and doris_sample:
            mysql_first = mysql_sample[0]
            doris_first = doris_sample[0]
            
            matches = 0
            total_fields = len(mysql_first)
            
            for key in mysql_first:
                if str(mysql_first[key]) == str(doris_first.get(key, '')):
                    matches += 1
                else:
                    print(f"⚠️  字段 {key} 不匹配: MySQL={mysql_first[key]}, Doris={doris_first.get(key, 'N/A')}")
            
            if matches == total_fields:
                print(f"✅ 样本数据一致")
            else:
                print(f"⚠️  样本数据部分匹配: {matches}/{total_fields}")
        
        return mysql_count == doris_count
    
    def test_doris_query(self):
        """测试Doris查询功能"""
        print(f"\n🔍 测试Doris查询功能...")
        try:
            conn = pymysql.connect(**self.doris_config)
            cursor = conn.cursor(pymysql.cursors.DictCursor)
            
            cursor.execute(f"USE {self.doris_config['database']}")
            
            # 统计查询
            cursor.execute("""
                SELECT 
                    risk_level,
                    COUNT(*) as count,
                    AVG(attendance_rate) as avg_attendance,
                    AVG(submit_rate) as avg_submit
                FROM abc_warning 
                GROUP BY risk_level
            """)
            stats = cursor.fetchall()
            
            print(f"📊 按风险等级统计:")
            for stat in stats:
                print(f"  {stat['risk_level']}: {stat['count']}条, 出勤率: {stat['avg_attendance']:.2f}%, 提交率: {stat['avg_submit']:.2f}%")
            
            conn.close()
            return True
        except Exception as e:
            print(f"❌ Doris查询失败: {e}")
            return False

def main():
    """主函数"""
    print("=" * 60)
    print("    MySQL to Doris 数据同步验证")
    print("=" * 60)
    
    verifier = SyncVerifier()
    
    # 验证数据同步
    sync_ok = verifier.verify_table('abc_warning')
    
    # 测试查询功能
    query_ok = verifier.test_doris_query()
    
    print("\n" + "=" * 60)
    print("验证结果:")
    print(f"数据同步: {'✅ 成功' if sync_ok else '❌ 失败'}")
    print(f"查询功能: {'✅ 正常' if query_ok else '❌ 异常'}")
    
    if sync_ok and query_ok:
        print("\n🎉 全量数据同步验证通过!")
        print("\n📋 接下来可以:")
        print("1. 在Flink中配置CDC实时同步")
        print("2. 测试增量数据变更")
        print("3. 构建数仓分层模型")
    else:
        print("\n⚠️  请检查并解决验证中发现的问题")

if __name__ == "__main__":
    main() 