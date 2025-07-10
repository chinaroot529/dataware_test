#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
学霸神器数仓 - 权限系统建表脚本
功能：创建完整的权限系统数据模型
"""

import pymysql
import sys
import traceback
from datetime import datetime

# 数据库连接配置
DB_CONFIG = {
    'host': '10.10.0.117',
    'port': 6033,
    'user': 'root',
    'password': 'Xml123&45!',
    'charset': 'utf8mb4',
    'autocommit': True
}

DATABASE_NAME = 'data_ware_test'

def get_connection(use_database=False):
    """获取数据库连接"""
    config = DB_CONFIG.copy()
    if use_database:
        config['database'] = DATABASE_NAME
    return pymysql.connect(**config)

def execute_sql(sql, use_database=False, ignore_errors=False):
    """执行SQL语句"""
    try:
        conn = get_connection(use_database)
        cursor = conn.cursor()
        cursor.execute(sql)
        result = cursor.fetchall()
        cursor.close()
        conn.close()
        return result
    except Exception as e:
        if not ignore_errors:
            print(f"SQL执行错误: {str(e)}")
            print(f"SQL语句: {sql}")
            raise
        return None

def create_database():
    """创建数据库"""
    print("正在创建数据库...")
    sql = f"CREATE DATABASE IF NOT EXISTS {DATABASE_NAME} CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci"
    execute_sql(sql)
    print(f"数据库 {DATABASE_NAME} 创建成功")

def create_tables():
    """创建所有表"""
    print("正在创建数据表...")
    
    # 建表SQL列表
    table_sqls = [
        # 1. 组织树维度表
        """
        CREATE TABLE IF NOT EXISTS 组织树维度 (
            组织ID VARCHAR(50) PRIMARY KEY COMMENT '组织唯一标识，如1000',
            组织名称 VARCHAR(100) NOT NULL COMMENT '组织名称，如博雅中学',
            组织类型 ENUM('学校', '学部', '年级', '班级', '教研组', '其他') NOT NULL COMMENT '组织类型',
            父节点ID VARCHAR(50) COMMENT '父组织ID',
            组织路径 VARCHAR(500) NOT NULL COMMENT '完整路径，如1000/1100/1110/1112',
            组织层级 INT NOT NULL COMMENT '组织层级，1-学校 2-学部 3-年级 4-班级',
            学段 ENUM('小学', '初中', '高中', '其他') COMMENT '所属学段',
            年级 VARCHAR(20) COMMENT '年级信息，如高一、高二',
            是否启用 BOOLEAN DEFAULT TRUE COMMENT '是否启用',
            排序号 INT DEFAULT 0 COMMENT '同级排序',
            创建时间 TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            更新时间 TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
            INDEX idx_parent_id (父节点ID),
            INDEX idx_org_path (组织路径),
            INDEX idx_org_level (组织层级),
            INDEX idx_segment (学段)
        ) COMMENT='组织树维度表，支持学校-学部-年级-班级的层级结构'
        """,
        
        # 2. 角色定义表
        """
        CREATE TABLE IF NOT EXISTS 角色定义表 (
            角色ID VARCHAR(50) PRIMARY KEY COMMENT '角色唯一标识',
            角色名称 VARCHAR(100) NOT NULL COMMENT '角色名称',
            角色类型 ENUM('系统角色', '业务角色', '功能角色') NOT NULL COMMENT '角色类型',
            角色描述 TEXT COMMENT '角色描述',
            是否系统内置 BOOLEAN DEFAULT FALSE COMMENT '是否系统内置角色',
            权限级别 INT DEFAULT 1 COMMENT '权限级别，数字越大权限越高',
            是否启用 BOOLEAN DEFAULT TRUE COMMENT '是否启用',
            创建时间 TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            更新时间 TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
            INDEX idx_role_type (角色类型),
            INDEX idx_permission_level (权限级别)
        ) COMMENT='角色定义表，定义系统中的所有角色类型'
        """,
        
        # 3. 个人维度表
        """
        CREATE TABLE IF NOT EXISTS 个人维度 (
            用户ID VARCHAR(50) PRIMARY KEY COMMENT '用户唯一标识',
            用户类型 ENUM('教师', '学生', '管理员', '家长', '其他') NOT NULL COMMENT '用户类型',
            用户名 VARCHAR(100) NOT NULL COMMENT '用户名',
            真实姓名 VARCHAR(50) NOT NULL COMMENT '真实姓名',
            性别 ENUM('男', '女', '未知') DEFAULT '未知' COMMENT '性别',
            出生日期 DATE COMMENT '出生日期',
            身份证号 VARCHAR(18) COMMENT '身份证号',
            手机号 VARCHAR(20) COMMENT '手机号',
            邮箱 VARCHAR(100) COMMENT '邮箱',
            主要学科 VARCHAR(50) COMMENT '主要任教学科（教师）',
            职称 VARCHAR(50) COMMENT '职称（教师）',
            工号 VARCHAR(50) COMMENT '工号',
            学号 VARCHAR(50) COMMENT '学号（学生）',
            注册渠道 ENUM('学霸神器', '课中系统', '管理员创建', '其他') DEFAULT '其他' COMMENT '注册渠道',
            账户状态 ENUM('正常', '冻结', '注销', '待激活') DEFAULT '待激活' COMMENT '账户状态',
            最后登录时间 TIMESTAMP COMMENT '最后登录时间',
            创建时间 TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            更新时间 TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
            INDEX idx_user_type (用户类型),
            INDEX idx_real_name (真实姓名),
            INDEX idx_mobile (手机号),
            INDEX idx_account_status (账户状态),
            UNIQUE KEY uk_username (用户名),
            UNIQUE KEY uk_mobile (手机号),
            UNIQUE KEY uk_email (邮箱)
        ) COMMENT='个人维度表，存储所有用户的基本信息'
        """,
        
        # 4. 权限定义表
        """
        CREATE TABLE IF NOT EXISTS 权限定义表 (
            权限ID VARCHAR(50) PRIMARY KEY COMMENT '权限唯一标识',
            权限名称 VARCHAR(100) NOT NULL COMMENT '权限名称',
            权限类型 ENUM('数据权限', '功能权限', '操作权限') NOT NULL COMMENT '权限类型',
            资源类型 ENUM('题目', '试卷', '成绩', '学生', '班级', '系统功能') NOT NULL COMMENT '资源类型',
            操作类型 ENUM('查看', '编辑', '删除', '分享', '审批', '管理') NOT NULL COMMENT '操作类型',
            权限代码 VARCHAR(100) NOT NULL COMMENT '权限代码，如question:view',
            权限描述 TEXT COMMENT '权限描述',
            是否系统内置 BOOLEAN DEFAULT FALSE COMMENT '是否系统内置权限',
            创建时间 TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            更新时间 TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
            INDEX idx_permission_type (权限类型),
            INDEX idx_resource_type (资源类型),
            INDEX idx_operation_type (操作类型),
            UNIQUE KEY uk_permission_code (权限代码)
        ) COMMENT='权限定义表，定义系统中的所有权限类型'
        """,
        
        # 5. 用户组织关系表
        """
        CREATE TABLE IF NOT EXISTS 用户组织关系表 (
            关系ID VARCHAR(50) PRIMARY KEY COMMENT '关系唯一标识',
            用户ID VARCHAR(50) NOT NULL COMMENT '用户ID',
            组织ID VARCHAR(50) NOT NULL COMMENT '组织ID',
            组织路径 VARCHAR(500) NOT NULL COMMENT '完整组织路径',
            关系类型 ENUM('主要归属', '任课关系', '临时权限', '兼职关系') NOT NULL COMMENT '关系类型',
            角色ID VARCHAR(50) NOT NULL COMMENT '在该组织中的角色',
            是否主要角色 BOOLEAN DEFAULT FALSE COMMENT '是否为用户的主要角色',
            生效时间 TIMESTAMP DEFAULT CURRENT_TIMESTAMP COMMENT '关系生效时间',
            失效时间 TIMESTAMP NULL COMMENT '关系失效时间，NULL表示长期有效',
            创建者ID VARCHAR(50) COMMENT '创建者ID',
            创建时间 TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            更新时间 TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
            INDEX idx_user_id (用户ID),
            INDEX idx_org_id (组织ID),
            INDEX idx_org_path (组织路径),
            INDEX idx_relation_type (关系类型),
            INDEX idx_role_id (角色ID),
            INDEX idx_effective_time (生效时间, 失效时间)
        ) COMMENT='用户组织关系表，支持用户在多个组织中担任不同角色'
        """,
        
        # 6. 角色权限桥接表
        """
        CREATE TABLE IF NOT EXISTS 角色权限桥接表 (
            桥接ID VARCHAR(50) PRIMARY KEY COMMENT '桥接关系唯一标识',
            角色ID VARCHAR(50) NOT NULL COMMENT '角色ID',
            权限ID VARCHAR(50) NOT NULL COMMENT '权限ID',
            授权范围 JSON COMMENT '权限授权范围限制',
            是否继承 BOOLEAN DEFAULT TRUE COMMENT '是否可被下级继承',
            生效时间 TIMESTAMP DEFAULT CURRENT_TIMESTAMP COMMENT '权限生效时间',
            失效时间 TIMESTAMP NULL COMMENT '权限失效时间，NULL表示长期有效',
            创建者ID VARCHAR(50) COMMENT '创建者ID',
            创建时间 TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            更新时间 TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
            INDEX idx_role_id (角色ID),
            INDEX idx_permission_id (权限ID),
            INDEX idx_effective_time (生效时间, 失效时间),
            UNIQUE KEY uk_role_permission (角色ID, 权限ID)
        ) COMMENT='角色权限桥接表，定义角色拥有的权限'
        """,
        
        # 7. 题目操作ACL表
        """
        CREATE TABLE IF NOT EXISTS 题目操作ACL表 (
            ACL_ID VARCHAR(50) PRIMARY KEY COMMENT 'ACL记录唯一标识',
            资源ID VARCHAR(50) NOT NULL COMMENT '资源ID（题目ID、试卷ID等）',
            资源类型 ENUM('题目', '试卷', '成绩', '其他') NOT NULL COMMENT '资源类型',
            授权对象类型 ENUM('user', 'org', 'role') NOT NULL COMMENT '授权对象类型',
            授权对象ID VARCHAR(50) NOT NULL COMMENT '授权对象ID',
            权限类型 ENUM('查看', '编辑', '删除', '分享', '所有权') NOT NULL COMMENT '权限类型',
            权限范围 VARCHAR(500) COMMENT '权限适用的组织路径范围',
            是否可编辑原资源 BOOLEAN DEFAULT FALSE COMMENT '是否可以编辑原资源（否则创建副本）',
            权限来源 ENUM('默认继承', '路径权限', '申请获得', '手动授权') NOT NULL COMMENT '权限来源',
            申请状态 ENUM('无需申请', '待审批', '已通过', '已拒绝', '已撤回') DEFAULT '无需申请' COMMENT '申请状态',
            申请者ID VARCHAR(50) COMMENT '申请者ID',
            审批者ID VARCHAR(50) COMMENT '审批者ID',
            申请时间 TIMESTAMP NULL COMMENT '申请时间',
            审批时间 TIMESTAMP NULL COMMENT '审批时间',
            审批意见 TEXT COMMENT '审批意见',
            生效时间 TIMESTAMP DEFAULT CURRENT_TIMESTAMP COMMENT '权限生效时间',
            失效时间 TIMESTAMP NULL COMMENT '权限失效时间',
            创建者ID VARCHAR(50) NOT NULL COMMENT '创建者ID',
            创建时间 TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            更新时间 TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
            INDEX idx_resource (资源ID, 资源类型),
            INDEX idx_grantee (授权对象类型, 授权对象ID),
            INDEX idx_permission_type (权限类型),
            INDEX idx_permission_source (权限来源),
            INDEX idx_apply_status (申请状态),
            INDEX idx_effective_time (生效时间, 失效时间),
            INDEX idx_creator (创建者ID)
        ) COMMENT='题目操作ACL表，控制对题目等资源的访问权限'
        """,
        
        # 8. 权限申请表
        """
        CREATE TABLE IF NOT EXISTS 权限申请表 (
            申请ID VARCHAR(50) PRIMARY KEY COMMENT '申请唯一标识',
            申请类型 ENUM('资源权限', '角色权限', '组织权限') NOT NULL COMMENT '申请类型',
            申请者ID VARCHAR(50) NOT NULL COMMENT '申请者ID',
            目标资源ID VARCHAR(50) COMMENT '目标资源ID',
            目标资源类型 ENUM('题目', '试卷', '成绩', '角色', '组织') COMMENT '目标资源类型',
            申请权限类型 ENUM('查看', '编辑', '删除', '分享', '管理') NOT NULL COMMENT '申请的权限类型',
            申请原因 TEXT COMMENT '申请原因',
            申请状态 ENUM('待审批', '已通过', '已拒绝', '已撤回', '已过期') DEFAULT '待审批' COMMENT '申请状态',
            审批者ID VARCHAR(50) COMMENT '审批者ID',
            审批时间 TIMESTAMP NULL COMMENT '审批时间',
            审批意见 TEXT COMMENT '审批意见',
            申请时间 TIMESTAMP DEFAULT CURRENT_TIMESTAMP COMMENT '申请时间',
            过期时间 TIMESTAMP NULL COMMENT '申请过期时间',
            处理时间 TIMESTAMP NULL COMMENT '处理完成时间',
            创建时间 TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            更新时间 TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
            INDEX idx_applicant (申请者ID),
            INDEX idx_approver (审批者ID),
            INDEX idx_target_resource (目标资源ID, 目标资源类型),
            INDEX idx_apply_status (申请状态),
            INDEX idx_apply_time (申请时间)
        ) COMMENT='权限申请表，记录所有权限申请和审批流程'
        """,
        
        # 9. 操作审计表
        """
        CREATE TABLE IF NOT EXISTS 操作审计表 (
            审计ID VARCHAR(50) PRIMARY KEY COMMENT '审计记录唯一标识',
            操作用户ID VARCHAR(50) NOT NULL COMMENT '操作用户ID',
            操作类型 ENUM('登录', '登出', '创建', '编辑', '删除', '查看', '分享', '权限变更') NOT NULL COMMENT '操作类型',
            操作对象类型 ENUM('题目', '试卷', '成绩', '用户', '角色', '组织', '权限') NOT NULL COMMENT '操作对象类型',
            操作对象ID VARCHAR(50) COMMENT '操作对象ID',
            操作描述 TEXT COMMENT '操作描述',
            操作前数据 JSON COMMENT '操作前的数据状态',
            操作后数据 JSON COMMENT '操作后的数据状态',
            操作结果 ENUM('成功', '失败', '部分成功') DEFAULT '成功' COMMENT '操作结果',
            失败原因 TEXT COMMENT '操作失败原因',
            操作IP VARCHAR(50) COMMENT '操作IP地址',
            用户代理 TEXT COMMENT '用户代理信息',
            会话ID VARCHAR(100) COMMENT '会话ID',
            操作时间 TIMESTAMP DEFAULT CURRENT_TIMESTAMP COMMENT '操作时间',
            INDEX idx_operator (操作用户ID),
            INDEX idx_operation_type (操作类型),
            INDEX idx_target_object (操作对象类型, 操作对象ID),
            INDEX idx_operation_time (操作时间),
            INDEX idx_operation_result (操作结果)
        ) COMMENT='操作审计表，记录所有重要操作的审计日志'
        """,
        
        # 10. 权限变更日志表
        """
        CREATE TABLE IF NOT EXISTS 权限变更日志表 (
            日志ID VARCHAR(50) PRIMARY KEY COMMENT '日志唯一标识',
            变更类型 ENUM('权限授予', '权限撤销', '权限修改', '角色变更', '组织调整') NOT NULL COMMENT '变更类型',
            目标用户ID VARCHAR(50) COMMENT '权限变更的目标用户',
            目标角色ID VARCHAR(50) COMMENT '权限变更的目标角色',
            目标组织ID VARCHAR(50) COMMENT '权限变更的目标组织',
            权限变更内容 JSON COMMENT '具体的权限变更内容',
            变更前状态 JSON COMMENT '变更前的权限状态',
            变更后状态 JSON COMMENT '变更后的权限状态',
            变更原因 TEXT COMMENT '变更原因',
            操作者ID VARCHAR(50) NOT NULL COMMENT '执行变更的操作者',
            审批者ID VARCHAR(50) COMMENT '审批者ID（如需审批）',
            变更时间 TIMESTAMP DEFAULT CURRENT_TIMESTAMP COMMENT '变更时间',
            生效时间 TIMESTAMP DEFAULT CURRENT_TIMESTAMP COMMENT '变更生效时间',
            INDEX idx_change_type (变更类型),
            INDEX idx_target_user (目标用户ID),
            INDEX idx_target_role (目标角色ID),
            INDEX idx_target_org (目标组织ID),
            INDEX idx_operator (操作者ID),
            INDEX idx_change_time (变更时间)
        ) COMMENT='权限变更日志表，记录所有权限相关的变更历史'
        """
    ]
    
    # 执行建表语句
    for i, sql in enumerate(table_sqls, 1):
        try:
            execute_sql(sql, use_database=True)
            print(f"  ✓ 第{i}个表创建成功")
        except Exception as e:
            print(f"  ✗ 第{i}个表创建失败: {str(e)}")
            raise
    
    print("所有数据表创建完成！")

def insert_sample_data():
    """插入示例数据"""
    print("正在插入示例数据...")
    
    # 读取示例数据SQL文件
    try:
        with open('permission_sample_data.sql', 'r', encoding='utf-8') as f:
            sql_content = f.read()
        
        # 分割SQL语句并执行
        sql_statements = [stmt.strip() for stmt in sql_content.split(';') if stmt.strip() and not stmt.strip().startswith('--') and not stmt.strip().startswith('USE')]
        
        for i, sql in enumerate(sql_statements, 1):
            if sql:
                try:
                    execute_sql(sql, use_database=True)
                    print(f"  ✓ 第{i}条数据插入成功")
                except Exception as e:
                    print(f"  ✗ 第{i}条数据插入失败: {str(e)}")
                    print(f"  SQL: {sql[:100]}...")
                    # 继续执行其他语句
                    continue
        
        print("示例数据插入完成！")
        
    except FileNotFoundError:
        print("警告：找不到 permission_sample_data.sql 文件，跳过示例数据插入")

def validate_system():
    """验证系统"""
    print("正在验证权限系统...")
    
    # 验证查询
    validation_queries = [
        ("检查表是否创建成功", "SHOW TABLES"),
        ("检查组织数据", "SELECT COUNT(*) as 组织数量 FROM 组织树维度"),
        ("检查用户数据", "SELECT COUNT(*) as 用户数量 FROM 个人维度"),
        ("检查权限数据", "SELECT COUNT(*) as 权限数量 FROM 权限定义表"),
        ("检查角色数据", "SELECT COUNT(*) as 角色数量 FROM 角色定义表")
    ]
    
    for desc, sql in validation_queries:
        try:
            result = execute_sql(sql, use_database=True)
            print(f"  ✓ {desc}: {result}")
        except Exception as e:
            print(f"  ✗ {desc} 失败: {str(e)}")

def main():
    """主函数"""
    print("=" * 60)
    print("🚀 学霸神器数仓 - 权限系统建表脚本")
    print("=" * 60)
    print(f"开始时间: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print(f"目标数据库: {DB_CONFIG['host']}:{DB_CONFIG['port']}/{DATABASE_NAME}")
    print()
    
    try:
        # 步骤1：创建数据库
        create_database()
        print()
        
        # 步骤2：创建表
        create_tables()
        print()
        
        # 步骤3：插入示例数据
        insert_sample_data()
        print()
        
        # 步骤4：验证系统
        validate_system()
        print()
        
        print("🎉 权限系统建立完成！")
        print("=" * 60)
        
    except Exception as e:
        print(f"❌ 系统建立失败: {str(e)}")
        print("错误详情:")
        traceback.print_exc()
        sys.exit(1)

if __name__ == "__main__":
    main() 