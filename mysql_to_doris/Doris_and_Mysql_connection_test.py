#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
连接测试脚本 - 验证MySQL和Doris连通性
"""

import pymysql
import sys

def test_mysql_connection():
    """测试MySQL连接"""
    mysql_config = {
        'host': '10.10.0.117',
        'port': 6033,
        'user': 'root',
        'password': 'Xml123&45!',
        'database': 'data_ware_test',
        'charset': 'utf8mb4'
    }
    
    print("🔍 测试MySQL连接...")
    try:
        conn = pymysql.connect(**mysql_config)
        cursor = conn.cursor(pymysql.cursors.DictCursor)
        
        # 测试基本查询
        cursor.execute("SELECT VERSION() as version")
        result = cursor.fetchone()
        print(f"✅ MySQL连接成功!")
        print(f"   版本: {result['version']}")
        
        # 检查abc_warning表是否存在
        cursor.execute("SHOW TABLES LIKE 'abc_warning'")
        table_exists = cursor.fetchone()
        
        if table_exists:
            print(f"✅ abc_warning表存在")
            
            # 获取表结构
            cursor.execute("DESCRIBE abc_warning")
            schema = cursor.fetchall()
            print(f"   表结构:")
            for field in schema:
                print(f"     {field['Field']}: {field['Type']} ({field['Null']}, {field['Key']})")
            
            # 获取记录数
            cursor.execute("SELECT COUNT(*) as count FROM abc_warning")
            count = cursor.fetchone()['count']
            print(f"   记录数: {count}")
            
        else:
            print(f"❌ abc_warning表不存在")
            print("   可用的表:")
            cursor.execute("SHOW TABLES")
            tables = cursor.fetchall()
            for table in tables[:10]:  # 只显示前10个表
                print(f"     {list(table.values())[0]}")
        
        conn.close()
        return True
        
    except Exception as e:
        print(f"❌ MySQL连接失败: {e}")
        return False

def test_doris_connection():
    """测试Doris连接"""
    doris_config = {
        'host': '192.168.99.6',
        'port': 19030,  # MySQL协议端口
        'user': 'root',
        'password': '',
        'charset': 'utf8mb4'
    }
    
    print("\n🔍 测试Doris连接...")
    try:
        conn = pymysql.connect(**doris_config)
        cursor = conn.cursor(pymysql.cursors.DictCursor)
        
        # 测试基本查询
        cursor.execute("SELECT @@version_comment as version")
        result = cursor.fetchone()
        print(f"✅ Doris连接成功!")
        print(f"   版本: {result['version']}")
        
        # 检查数据库
        cursor.execute("SHOW DATABASES")
        databases = cursor.fetchall()
        print(f"   可用数据库:")
        for db in databases:
            print(f"     {list(db.values())[0]}")
        
        # 尝试创建测试数据库
        cursor.execute("CREATE DATABASE IF NOT EXISTS ods")
        cursor.execute("USE ods")
        print(f"✅ 测试数据库 'ods' 创建/使用成功")
        
        conn.close()
        return True
        
    except Exception as e:
        print(f"❌ Doris连接失败: {e}")
        print(f"   请检查:")
        print(f"   1. Doris服务是否在192.168.99.6:19030上运行")
        print(f"   2. 防火墙是否开放19030端口")
        print(f"   3. Docker容器是否正常启动")
        return False

def main():
    """主函数"""
    print("=" * 50)
    print("    MySQL to Doris 连接测试")
    print("=" * 50)
    
    mysql_ok = test_mysql_connection()
    doris_ok = test_doris_connection()
    
    print("\n" + "=" * 50)
    print("测试结果汇总:")
    print(f"MySQL: {'✅ 成功' if mysql_ok else '❌ 失败'}")
    print(f"Doris: {'✅ 成功' if doris_ok else '❌ 失败'}")
    
    if mysql_ok and doris_ok:
        print("\n🎉 所有连接测试通过，可以开始数据同步!")
        return True
    else:
        print("\n⚠️  请先解决连接问题再进行数据同步")
        return False

if __name__ == "__main__":
    success = main()
    sys.exit(0 if success else 1) 