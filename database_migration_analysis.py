#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
数据库结构对比和迁移分析脚本
"""

import pymysql
import json
from collections import defaultdict
from datetime import datetime

# 旧数据库配置（测试环境）
OLD_DB_CONFIG = {
    'host': '10.10.0.117',
    'port': 6033,
    'user': 'root',
    'password': 'Xml123&45!',
    'database': 'data_ware_test',
    'charset': 'utf8mb4'
}

# 新数据库配置（正式环境）
NEW_DB_CONFIG = {
    'host': '192.168.99.6',
    'port': 13306,
    'user': 'root',
    'password': 'root',
    'database': 'dataware',  # 需要确认
    'charset': 'utf8mb4'
}

def get_connection(config):
    """获取数据库连接"""
    return pymysql.connect(**config)

def get_table_structure(config):
    """获取数据库表结构"""
    conn = get_connection(config)
    cursor = conn.cursor()
    
    # 获取所有表名
    cursor.execute("SHOW TABLES")
    tables = [row[0] for row in cursor.fetchall()]
    
    table_structures = {}
    
    for table in tables:
        # 获取表结构
        cursor.execute(f"DESCRIBE {table}")
        columns = cursor.fetchall()
        
        # 获取索引信息
        cursor.execute(f"SHOW INDEX FROM {table}")
        indexes = cursor.fetchall()
        
        table_structures[table] = {
            'columns': columns,
            'indexes': indexes
        }
    
    cursor.close()
    conn.close()
    return table_structures

def compare_databases():
    """对比两个数据库结构"""
    print("正在分析数据库结构差异...")
    
    try:
        old_structure = get_table_structure(OLD_DB_CONFIG)
        print(f"旧数据库表数量: {len(old_structure)}")
        
        new_structure = get_table_structure(NEW_DB_CONFIG)
        print(f"新数据库表数量: {len(new_structure)}")
        
        # 分析差异
        old_tables = set(old_structure.keys())
        new_tables = set(new_structure.keys())
        
        print("\n=== 表名对比 ===")
        print(f"旧数据库独有表: {old_tables - new_tables}")
        print(f"新数据库独有表: {new_tables - old_tables}")
        print(f"共同表: {old_tables & new_tables}")
        
        # 详细对比共同表的结构
        print("\n=== 表结构对比 ===")
        for table in old_tables & new_tables:
            old_cols = {col[0]: col for col in old_structure[table]['columns']}
            new_cols = {col[0]: col for col in new_structure[table]['columns']}
            
            if old_cols != new_cols:
                print(f"\n表 {table} 结构有差异:")
                print(f"  旧表字段: {list(old_cols.keys())}")
                print(f"  新表字段: {list(new_cols.keys())}")
        
        return old_structure, new_structure
        
    except Exception as e:
        print(f"数据库连接或分析失败: {e}")
        return None, None

def generate_mapping_config(old_structure, new_structure):
    """生成字段映射配置"""
    mapping = {}
    
    # 基于现有接口分析需要的表映射
    api_table_mapping = {
        # 用户相关
        '个人维度': 'xbsq_dwd_user_personal_dim',
        '用户组织关系表': 'xbsq_dwd_user_class_member_detail',
        '组织树维度': 'xbsq_dwd_user_org_tree_dim',
        
        # 权限相关
        '角色定义表': 'xbsq_dwd_user_role_def_dim',
        '权限定义表': 'xbsq_dwd_user_permission_def_dim',
        '角色权限桥接表': 'xbsq_dwd_user_role_permission_bridge',
        
        # 题目相关
        '题目库': 'xbsq_dwd_question_bank',
        '题目操作ACL表': 'xbsq_dwd_question_acl',
        
        # 审计相关
        '操作审计表': 'xbsq_dwd_audit_log',
        '权限申请表': 'xbsq_dwd_permission_request'
    }
    
    return api_table_mapping

def analyze_field_mapping(old_structure, new_structure):
    """详细分析字段映射关系"""
    print("\n=== 详细字段映射分析 ===")
    
    # 现有接口使用的表和字段
    current_api_usage = {
        '个人维度': ['用户ID', '用户名', '真实姓名', '用户类型', '账户状态'],
        '用户组织关系表': ['用户ID', '组织ID', '角色ID', '关系类型', '生效时间', '失效时间'],
        '组织树维度': ['组织ID', '组织名称', '组织类型', '父组织ID', '组织路径'],
        '角色定义表': ['角色ID', '角色名称', '角色描述'],
        '权限定义表': ['权限ID', '权限名称', '权限描述'],
        '角色权限桥接表': ['角色ID', '权限ID'],
        '题目库': ['题目ID', '题目标题', '题目内容', '创建者ID', '学段路径', '创建时间'],
        '题目操作ACL表': ['题目ID', '用户ID', '权限级别', '授权时间'],
        '操作审计表': ['操作ID', '用户ID', '操作类型', '操作时间', '操作详情'],
        '权限申请表': ['申请ID', '申请人ID', '申请权限', '申请状态', '申请时间']
    }
    
    field_mapping = {}
    
    for old_table, fields in current_api_usage.items():
        print(f"\n分析表: {old_table}")
        
        # 查找新数据库中对应的表
        possible_new_tables = []
        for new_table in new_structure.keys():
            if any(keyword in new_table.lower() for keyword in ['user', 'org', 'role', 'permission', 'question', 'acl', 'audit']):
                possible_new_tables.append(new_table)
        
        print(f"  可能对应的新表: {possible_new_tables}")
        
        # 分析字段映射
        for field in fields:
            print(f"    需要映射字段: {field}")
    
    return field_mapping

def generate_sql_migration():
    """生成SQL迁移语句"""
    print("\n=== 生成接口迁移建议 ===")
    
    # 基于app.py中的SQL语句生成迁移建议
    migration_sqls = {
        'login': """
        -- 原SQL: SELECT 用户ID, 用户名, 真实姓名, 用户类型 FROM 个人维度 WHERE 用户名=%s AND 账户状态='正常'
        -- 新SQL需要根据新表结构调整
        """,
        'user_permissions': """
        -- 原SQL涉及多表JOIN，需要根据新表结构重写
        """,
        'questions_list': """
        -- 原SQL: SELECT * FROM 题目库 WHERE 条件
        -- 新SQL需要根据新表结构调整
        """
    }
    
    for api, sql in migration_sqls.items():
        print(f"\n{api}接口:")
        print(sql)

if __name__ == "__main__":
    print("开始数据库结构对比分析...")
    old_struct, new_struct = compare_databases()
    
    if old_struct and new_struct:
        # 生成表映射
        mapping = generate_mapping_config(old_struct, new_struct)
        print(f"\n=== 建议的表映射 ===")
        for old_table, new_table in mapping.items():
            print(f"{old_table} -> {new_table}")
        
        # 详细字段分析
        analyze_field_mapping(old_struct, new_struct)
        
        # 生成迁移建议
        generate_sql_migration()
        
        # 保存分析结果
        with open('database_migration_report.json', 'w', encoding='utf-8') as f:
            json.dump({
                'old_tables': list(old_struct.keys()),
                'new_tables': list(new_struct.keys()),
                'table_mapping': mapping,
                'timestamp': str(datetime.now())
            }, f, ensure_ascii=False, indent=2)
        
        print(f"\n分析报告已保存到: database_migration_report.json")
    else:
        print("数据库连接失败，请检查配置")
