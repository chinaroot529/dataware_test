#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
学霸神器数仓 - 权限系统示例数据插入脚本
"""

import pymysql
import traceback
from datetime import datetime, timedelta

# 数据库连接配置
DB_CONFIG = {
    'host': '10.10.0.117',
    'port': 6033,
    'user': 'root',
    'password': 'Xml123&45!',
    'database': 'data_ware_test',
    'charset': 'utf8mb4',
    'autocommit': True
}

def get_connection():
    """获取数据库连接"""
    return pymysql.connect(**DB_CONFIG)

def execute_sql(sql, params=None):
    """执行SQL语句"""
    try:
        conn = get_connection()
        cursor = conn.cursor()
        if params:
            cursor.execute(sql, params)
        else:
            cursor.execute(sql)
        result = cursor.fetchall()
        cursor.close()
        conn.close()
        return result
    except Exception as e:
        print(f"SQL执行错误: {str(e)}")
        print(f"SQL语句: {sql}")
        raise

def insert_organizations():
    """插入组织架构数据"""
    print("正在插入组织架构数据...")
    
    org_data = [
        ('1000', '博雅中学', '学校', None, '1000', 1, None, None, 1),
        ('1100', '高中部', '学部', '1000', '1000/1100', 2, '高中', None, 1),
        ('1110', '高一年级', '年级', '1100', '1000/1100/1110', 3, '高中', '高一', 1),
        ('1120', '高二年级', '年级', '1100', '1000/1100/1120', 3, '高中', '高二', 2),
        ('1130', '高三年级', '年级', '1100', '1000/1100/1130', 3, '高中', '高三', 3),
        ('1111', '高一一班', '班级', '1110', '1000/1100/1110/1111', 4, '高中', '高一', 1),
        ('1112', '高一二班', '班级', '1110', '1000/1100/1110/1112', 4, '高中', '高一', 2),
        ('1113', '高一三班', '班级', '1110', '1000/1100/1110/1113', 4, '高中', '高一', 3),
        ('1121', '高二一班', '班级', '1120', '1000/1100/1120/1121', 4, '高中', '高二', 1),
        ('1122', '高二二班', '班级', '1120', '1000/1100/1120/1122', 4, '高中', '高二', 2),
        ('1123', '高二三班', '班级', '1120', '1000/1100/1120/1123', 4, '高中', '高二', 3),
        ('1131', '高三一班', '班级', '1130', '1000/1100/1130/1131', 4, '高中', '高三', 1),
        ('1132', '高三二班', '班级', '1130', '1000/1100/1130/1132', 4, '高中', '高三', 2),
        ('1133', '高三三班', '班级', '1130', '1000/1100/1130/1133', 4, '高中', '高三', 3),
    ]
    
    sql = """INSERT INTO 组织树维度 (组织ID, 组织名称, 组织类型, 父节点ID, 组织路径, 组织层级, 学段, 年级, 排序号) 
             VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)"""
    
    for data in org_data:
        execute_sql(sql, data)
    
    print(f"  ✓ 插入了 {len(org_data)} 个组织记录")

def insert_roles():
    """插入角色数据"""
    print("正在插入角色数据...")
    
    role_data = [
        ('R001', '系统管理员', '系统角色', '拥有系统最高权限', True, 9),
        ('R002', '校长', '业务角色', '学校管理者，拥有学校级别权限', True, 8),
        ('R003', '学部主任', '业务角色', '学部管理者，拥有学部级别权限', True, 7),
        ('R004', '年级主任', '业务角色', '年级管理者，拥有年级级别权限', True, 6),
        ('R005', '班主任', '业务角色', '班级管理者，拥有班级级别权限', True, 5),
        ('R006', '任课教师', '业务角色', '普通任课教师', True, 4),
        ('R007', '代课教师', '业务角色', '临时代课教师', True, 3),
        ('R008', '实习教师', '业务角色', '实习期教师', True, 2),
        ('R009', '学生', '业务角色', '学生用户', True, 1),
    ]
    
    sql = """INSERT INTO 角色定义表 (角色ID, 角色名称, 角色类型, 角色描述, 是否系统内置, 权限级别) 
             VALUES (%s, %s, %s, %s, %s, %s)"""
    
    for data in role_data:
        execute_sql(sql, data)
    
    print(f"  ✓ 插入了 {len(role_data)} 个角色记录")

def insert_permissions():
    """插入权限数据"""
    print("正在插入权限数据...")
    
    permission_data = [
        ('P001', '查看题目', '数据权限', '题目', '查看', 'question:view', '查看题目内容和信息', True),
        ('P002', '编辑题目', '数据权限', '题目', '编辑', 'question:edit', '编辑题目内容', True),
        ('P003', '删除题目', '数据权限', '题目', '删除', 'question:delete', '删除题目', True),
        ('P004', '分享题目', '数据权限', '题目', '分享', 'question:share', '分享题目给其他用户', True),
        ('P005', '题目所有权', '数据权限', '题目', '管理', 'question:own', '题目所有权，可进行所有操作', True),
        ('P011', '查看试卷', '数据权限', '试卷', '查看', 'paper:view', '查看试卷内容', True),
        ('P012', '编辑试卷', '数据权限', '试卷', '编辑', 'paper:edit', '编辑试卷内容', True),
        ('P013', '删除试卷', '数据权限', '试卷', '删除', 'paper:delete', '删除试卷', True),
        ('P021', '查看成绩', '数据权限', '成绩', '查看', 'score:view', '查看学生成绩', True),
        ('P022', '编辑成绩', '数据权限', '成绩', '编辑', 'score:edit', '修改学生成绩', True),
        ('P031', '查看学生信息', '数据权限', '学生', '查看', 'student:view', '查看学生基本信息', True),
        ('P032', '管理学生', '数据权限', '学生', '管理', 'student:manage', '管理学生信息', True),
        ('P041', '查看班级信息', '数据权限', '班级', '查看', 'class:view', '查看班级信息', True),
        ('P042', '管理班级', '数据权限', '班级', '管理', 'class:manage', '管理班级信息', True),
    ]
    
    sql = """INSERT INTO 权限定义表 (权限ID, 权限名称, 权限类型, 资源类型, 操作类型, 权限代码, 权限描述, 是否系统内置) 
             VALUES (%s, %s, %s, %s, %s, %s, %s, %s)"""
    
    for data in permission_data:
        execute_sql(sql, data)
    
    print(f"  ✓ 插入了 {len(permission_data)} 个权限记录")

def insert_users():
    """插入用户数据"""
    print("正在插入用户数据...")
    
    user_data = [
        ('U001', '管理员', 'admin', '系统管理员', '男', None, None, 'A001', '正常'),
        ('U002', '教师', 'principal', '张校长', '男', None, '校长', 'P001', '正常'),
        ('U003', '教师', 'dept_director', '李主任', '女', None, '学部主任', 'D001', '正常'),
        ('U004', '教师', 'grade1_director', '王主任', '男', None, '年级主任', 'G001', '正常'),
        ('U005', '教师', 'grade2_director', '赵主任', '女', None, '年级主任', 'G002', '正常'),
        ('U006', '教师', 'class1_teacher', '刘老师', '女', '语文', '高级教师', 'T001', '正常'),
        ('U007', '教师', 'class2_teacher', '陈老师', '男', '数学', '中级教师', 'T002', '正常'),
        ('U008', '教师', 'math_teacher', '孙老师', '女', '数学', '高级教师', 'T003', '正常'),
        ('U009', '教师', 'english_teacher', '周老师', '男', '英语', '中级教师', 'T004', '正常'),
        ('U010', '教师', 'substitute_teacher', '吴老师', '女', '物理', '初级教师', 'T005', '正常'),
        ('U011', '学生', 'student1', '小明', '男', None, None, None, '正常'),
        ('U012', '学生', 'student2', '小红', '女', None, None, None, '正常'),
    ]
    
    sql = """INSERT INTO 个人维度 (用户ID, 用户类型, 用户名, 真实姓名, 性别, 主要学科, 职称, 工号, 账户状态) 
             VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)"""
    
    for data in user_data:
        execute_sql(sql, data)
    
    print(f"  ✓ 插入了 {len(user_data)} 个用户记录")

def insert_role_permissions():
    """插入角色权限关系"""
    print("正在插入角色权限关系...")
    
    # 系统管理员拥有所有权限
    admin_permissions = ['P001', 'P002', 'P003', 'P004', 'P005', 'P011', 'P012', 'P013', 
                        'P021', 'P022', 'P031', 'P032', 'P041', 'P042']
    
    # 任课教师基础权限
    teacher_permissions = ['P001', 'P002', 'P004', 'P011', 'P012', 'P021', 'P031']
    
    # 班主任权限
    head_teacher_permissions = ['P001', 'P002', 'P004', 'P011', 'P012', 'P021', 'P022', 'P031', 'P032', 'P041', 'P042']
    
    role_permission_data = []
    
    # 系统管理员权限
    for i, perm in enumerate(admin_permissions):
        role_permission_data.append((f'RP{i+1:03d}', 'R001', perm, True))
    
    # 任课教师权限
    for i, perm in enumerate(teacher_permissions):
        role_permission_data.append((f'RP{i+100:03d}', 'R006', perm, True))
    
    # 班主任权限
    for i, perm in enumerate(head_teacher_permissions):
        role_permission_data.append((f'RP{i+200:03d}', 'R005', perm, True))
    
    sql = """INSERT INTO 角色权限桥接表 (桥接ID, 角色ID, 权限ID, 是否继承) 
             VALUES (%s, %s, %s, %s)"""
    
    for data in role_permission_data:
        execute_sql(sql, data)
    
    print(f"  ✓ 插入了 {len(role_permission_data)} 个角色权限关系")

def insert_user_org_relations():
    """插入用户组织关系"""
    print("正在插入用户组织关系...")
    
    relation_data = [
        ('UR001', 'U001', '1000', '1000', '主要归属', 'R001', True),
        ('UR002', 'U002', '1000', '1000', '主要归属', 'R002', True),
        ('UR003', 'U003', '1100', '1000/1100', '主要归属', 'R003', True),
        ('UR004', 'U004', '1110', '1000/1100/1110', '主要归属', 'R004', True),
        ('UR005', 'U005', '1120', '1000/1100/1120', '主要归属', 'R004', True),
        ('UR006', 'U006', '1111', '1000/1100/1110/1111', '主要归属', 'R005', True),
        ('UR007', 'U007', '1112', '1000/1100/1110/1112', '主要归属', 'R005', True),
        ('UR008', 'U008', '1112', '1000/1100/1110/1112', '任课关系', 'R006', True),
        ('UR009', 'U008', '1123', '1000/1100/1120/1123', '任课关系', 'R006', False),
        ('UR010', 'U009', '1111', '1000/1100/1110/1111', '任课关系', 'R006', True),
        ('UR011', 'U009', '1113', '1000/1100/1110/1113', '任课关系', 'R006', False),
        ('UR012', 'U010', '1121', '1000/1100/1120/1121', '临时权限', 'R007', True),
        ('UR013', 'U011', '1111', '1000/1100/1110/1111', '主要归属', 'R009', True),
        ('UR014', 'U012', '1112', '1000/1100/1110/1112', '主要归属', 'R009', True),
    ]
    
    sql = """INSERT INTO 用户组织关系表 (关系ID, 用户ID, 组织ID, 组织路径, 关系类型, 角色ID, 是否主要角色) 
             VALUES (%s, %s, %s, %s, %s, %s, %s)"""
    
    for data in relation_data:
        execute_sql(sql, data)
    
    # 设置代课教师临时权限失效时间
    expire_sql = """UPDATE 用户组织关系表 SET 失效时间 = %s WHERE 关系ID = 'UR012'"""
    expire_time = datetime.now() + timedelta(days=90)  # 3个月后过期
    execute_sql(expire_sql, (expire_time,))
    
    print(f"  ✓ 插入了 {len(relation_data)} 个用户组织关系")

def insert_sample_acl():
    """插入示例ACL数据"""
    print("正在插入示例ACL数据...")
    
    acl_data = [
        ('ACL001', 'Q001', '题目', 'org', '1100', '查看', '1000/1100', False, '默认继承', '无需申请', 'U006'),
        ('ACL002', 'Q001', '题目', 'user', 'U006', '所有权', '1000/1100', True, '默认继承', '无需申请', 'U006'),
        ('ACL003', 'Q002', '题目', 'org', '1112', '查看', '1000/1100/1110/1112', False, '路径权限', '无需申请', 'U007'),
        ('ACL004', 'Q002', '题目', 'user', 'U007', '所有权', '1000/1100/1110/1112', True, '路径权限', '无需申请', 'U007'),
        ('ACL005', 'Q002', '题目', 'user', 'U008', '查看', '1000/1100/1120/1123', False, '申请获得', '待审批', 'U007'),
    ]
    
    sql = """INSERT INTO 题目操作ACL表 (ACL_ID, 资源ID, 资源类型, 授权对象类型, 授权对象ID, 权限类型, 权限范围, 是否可编辑原资源, 权限来源, 申请状态, 创建者ID) 
             VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)"""
    
    for data in acl_data:
        execute_sql(sql, data)
    
    print(f"  ✓ 插入了 {len(acl_data)} 个ACL记录")

def validate_data():
    """验证数据插入结果"""
    print("正在验证数据插入结果...")
    
    validation_queries = [
        ("组织数量", "SELECT COUNT(*) FROM 组织树维度"),
        ("角色数量", "SELECT COUNT(*) FROM 角色定义表"),
        ("权限数量", "SELECT COUNT(*) FROM 权限定义表"),
        ("用户数量", "SELECT COUNT(*) FROM 个人维度"),
        ("角色权限关系", "SELECT COUNT(*) FROM 角色权限桥接表"),
        ("用户组织关系", "SELECT COUNT(*) FROM 用户组织关系表"),
        ("ACL记录", "SELECT COUNT(*) FROM 题目操作ACL表"),
    ]
    
    for desc, sql in validation_queries:
        result = execute_sql(sql)
        count = result[0][0] if result else 0
        print(f"  ✓ {desc}: {count}")

def main():
    """主函数"""
    print("=" * 60)
    print("📊 权限系统示例数据插入脚本")
    print("=" * 60)
    print(f"开始时间: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print()
    
    try:
        insert_organizations()
        insert_roles()
        insert_permissions()
        insert_users()
        insert_role_permissions()
        insert_user_org_relations()
        insert_sample_acl()
        
        print()
        validate_data()
        
        print()
        print("🎉 示例数据插入完成！")
        print("=" * 60)
        
    except Exception as e:
        print(f"❌ 数据插入失败: {str(e)}")
        traceback.print_exc()

if __name__ == "__main__":
    main() 