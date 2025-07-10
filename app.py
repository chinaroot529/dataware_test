#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
学霸神器数仓 - Flask API 应用
提供权限验证和题目查询接口
"""

from flask import Flask, request, jsonify, render_template, session
from flask_cors import CORS
import pymysql
import hashlib
import json
from datetime import datetime
from functools import wraps

app = Flask(__name__)
app.secret_key = 'dataware_secret_key_2025'

# 配置session在跨域情况下工作
app.config['SESSION_COOKIE_SAMESITE'] = 'Lax'  # 开发环境使用Lax，生产环境可以改为None
app.config['SESSION_COOKIE_SECURE'] = False  # 开发环境设为False，生产环境应该设为True
app.config['SESSION_COOKIE_HTTPONLY'] = True

# 配置CORS支持跨域cookies
CORS(app, supports_credentials=True, origins=['*'])

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

def get_db_connection():
    """获取数据库连接"""
    return pymysql.connect(**DB_CONFIG)

def execute_query(sql, params=None):
    """执行查询语句"""
    try:
        conn = get_db_connection()
        cursor = conn.cursor(pymysql.cursors.DictCursor)
        if params:
            cursor.execute(sql, params)
        else:
            cursor.execute(sql)
        result = cursor.fetchall()
        cursor.close()
        conn.close()
        return result
    except Exception as e:
        print(f"数据库查询错误: {str(e)}")
        return []

def execute_update(sql, params=None):
    """执行更新语句"""
    try:
        conn = get_db_connection()
        cursor = conn.cursor()
        if params:
            cursor.execute(sql, params)
        else:
            cursor.execute(sql)
        result = cursor.rowcount
        cursor.close()
        conn.close()
        return result
    except Exception as e:
        print(f"数据库更新错误: {str(e)}")
        return 0

def login_required(f):
    """登录验证装饰器"""
    @wraps(f)
    def decorated_function(*args, **kwargs):
        if 'user_id' not in session:
            return jsonify({'error': '请先登录', 'code': 401}), 401
        return f(*args, **kwargs)
    return decorated_function

@app.route('/')
def index():
    """首页"""
    return render_template('index.html')

@app.route('/api/login', methods=['POST'])
def login():
    """用户登录接口"""
    data = request.get_json()
    username = data.get('username')
    password = data.get('password', '')  # 简化处理，实际应用需要密码验证
    
    # 查询用户信息
    sql = """
    SELECT u.用户ID, u.用户名, u.真实姓名, u.用户类型
    FROM 个人维度 u
    WHERE u.用户名 = %s AND u.账户状态 = '正常'
    """
    
    users = execute_query(sql, (username,))
    
    if users:
        user = users[0]
        session['user_id'] = user['用户ID']
        session['username'] = user['用户名']
        session['real_name'] = user['真实姓名']
        session['user_type'] = user['用户类型']
        
        return jsonify({
            'success': True,
            'message': '登录成功',
            'user': {
                'user_id': user['用户ID'],
                'username': user['用户名'],
                'real_name': user['真实姓名'],
                'user_type': user['用户类型']
            }
        })
    else:
        return jsonify({'success': False, 'message': '用户名不存在或账户已禁用'}), 400

@app.route('/api/logout', methods=['POST'])
def logout():
    """用户登出接口"""
    session.clear()
    return jsonify({'success': True, 'message': '登出成功'})

@app.route('/api/user/permissions')
@login_required
def get_user_permissions():
    """获取当前用户权限"""
    user_id = session['user_id']
    
    sql = """
    SELECT DISTINCT
        p.权限名称,
        p.权限代码,
        p.资源类型,
        p.操作类型,
        o.组织名称,
        uo.关系类型
    FROM 个人维度 u
    JOIN 用户组织关系表 uo ON u.用户ID = uo.用户ID
    JOIN 组织树维度 o ON uo.组织ID = o.组织ID
    JOIN 角色定义表 r ON uo.角色ID = r.角色ID
    JOIN 角色权限桥接表 rp ON r.角色ID = rp.角色ID
    JOIN 权限定义表 p ON rp.权限ID = p.权限ID
    WHERE u.用户ID = %s
    AND (uo.失效时间 IS NULL OR uo.失效时间 > NOW())
    AND (rp.失效时间 IS NULL OR rp.失效时间 > NOW())
    ORDER BY o.组织名称, p.权限代码
    """
    
    permissions = execute_query(sql, (user_id,))
    
    return jsonify({
        'success': True,
        'permissions': permissions
    })

@app.route('/api/questions')
@login_required
def get_questions():
    """获取题目列表"""
    user_id = session['user_id']
    
    # 获取查询参数
    subject = request.args.get('subject', '')
    grade = request.args.get('grade', '')
    difficulty = request.args.get('difficulty', '')
    question_type = request.args.get('type', '')
    page = int(request.args.get('page', 1))
    limit = int(request.args.get('limit', 20))
    
    # 检查用户是否为超级管理员或校长（拥有全权限）
    super_admin_sql = """
    SELECT 1
    FROM 个人维度 u
    LEFT JOIN 用户组织关系表 uo ON u.用户ID = uo.用户ID
    LEFT JOIN 角色定义表 r ON uo.角色ID = r.角色ID
    WHERE u.用户ID = %s
    AND (
        u.用户类型 = '管理员'  -- 系统管理员
        OR r.角色名称 = '校长'  -- 校长角色
    )
    """
    
    is_super_admin = execute_query(super_admin_sql, (user_id,))
    
    # 构建查询条件
    where_conditions = []
    params = []
    
    if is_super_admin:
        # 超级管理员和校长可以看到所有题目
        base_sql = """
        SELECT DISTINCT q.*
        FROM 题目库 q
        WHERE q.是否启用 = TRUE
        """
        params = []
    else:
        # 普通用户按ACL权限查看题目
        base_sql = """
        SELECT DISTINCT q.*
        FROM 题目库 q
        JOIN 用户组织关系表 uo
            ON uo.用户ID = %s
        WHERE q.是否启用 = TRUE
          AND (
                -- ① 路径继承：支持题目路径和用户路径互为前缀
                (
                    q.组织路径 LIKE CONCAT(uo.组织路径, '%%')
                    OR uo.组织路径 LIKE CONCAT(q.组织路径, '%%')
                )
                -- ② 或者 ACL 命中 (跨学段查看 / 可编辑 / 所有权)
                OR EXISTS (
                     SELECT 1 FROM 题目操作ACL表 acl
                      WHERE acl.资源ID = q.题目ID
                        AND acl.资源类型='题目'
                        AND acl.perm_level >= 0
                        AND (
                            (acl.授权对象类型='user' AND acl.授权对象ID = %s)
                            OR (acl.授权对象类型='org'
                                AND acl.授权对象ID IN
                                    (SELECT 组织ID FROM 用户组织关系表
                                      WHERE 用户ID = %s
                                        AND (失效时间 IS NULL OR 失效时间>NOW())))
                        )
                  )
              )
        """
        # three %s placeholders in SQL: JOIN … uo.用户ID = %s,
        # acl sub‑query user match, and org IN (SELECT … 用户ID = %s)
        params.extend([user_id, user_id, user_id])
    
    # 添加筛选条件
    if subject:
        where_conditions.append("AND q.学科 = %s")
        params.append(subject)
    
    if grade:
        where_conditions.append("AND q.年级 = %s")
        params.append(grade)
    
    if difficulty:
        where_conditions.append("AND q.难度级别 = %s")
        params.append(difficulty)
    
    if question_type:
        where_conditions.append("AND q.题目类型 = %s")
        params.append(question_type)
    
    # 组装完整SQL
    full_sql = base_sql + " " + " ".join(where_conditions)
    full_sql += " ORDER BY q.创建时间 DESC"
    
    # 分页
    offset = (page - 1) * limit
    full_sql += f" LIMIT {limit} OFFSET {offset}"
    

    questions = execute_query(full_sql, params)

    # 获取总数
    count_sql = base_sql.replace("SELECT DISTINCT q.*", "SELECT COUNT(DISTINCT q.题目ID)") + " " + " ".join(where_conditions)
    count_params = list(params)   # ensure same number for count query
    total_result = execute_query(count_sql, count_params)
    total = total_result[0]['COUNT(DISTINCT q.题目ID)'] if total_result else 0
    
    return jsonify({
        'success': True,
        'questions': questions,
        'total': total,
        'page': page,
        'limit': limit
    })

@app.route('/api/questions/<question_id>')
@login_required
def get_question_detail(question_id):
    """获取题目详情"""
    user_id = session['user_id']
    
    # 检查用户是否为超级管理员或校长（拥有全权限）
    super_admin_sql = """
    SELECT 1
    FROM 个人维度 u
    LEFT JOIN 用户组织关系表 uo ON u.用户ID = uo.用户ID
    LEFT JOIN 角色定义表 r ON uo.角色ID = r.角色ID
    WHERE u.用户ID = %s
    AND (
        u.用户类型 = '管理员'  -- 系统管理员
        OR r.角色名称 = '校长'  -- 校长角色
    )
    """
    
    is_super_admin = execute_query(super_admin_sql, (user_id,))
    
    # 超级管理员和校长无需检查权限
    if not is_super_admin:
        # 检查普通用户是否有权限查看此题目
        permission_sql = """
        SELECT 1
        FROM 题目操作ACL表 acl
        WHERE acl.资源ID = %s AND acl.资源类型 = '题目'
        AND acl.申请状态 IN ('无需申请', '已通过')
        AND (
            (acl.授权对象类型 = 'user' AND acl.授权对象ID = %s AND acl.权限类型 IN ('查看', '所有权'))
            OR 
            (acl.授权对象类型 = 'org' AND EXISTS (
                SELECT 1 FROM 用户组织关系表 uo 
                WHERE uo.用户ID = %s 
                AND uo.组织ID = acl.授权对象ID 
                AND (uo.失效时间 IS NULL OR uo.失效时间 > NOW())
            ) AND acl.权限类型 IN ('查看', '所有权'))
        )
        """
        
        has_permission = execute_query(permission_sql, (question_id, user_id, user_id))
        
        if not has_permission:
            return jsonify({'success': False, 'message': '无权限查看此题目'}), 403
    
    # 获取题目详情
    sql = """
    SELECT q.*, u.真实姓名 as 创建者姓名
    FROM 题目库 q
    LEFT JOIN 个人维度 u ON q.创建者ID = u.用户ID
    WHERE q.题目ID = %s
    """
    
    questions = execute_query(sql, (question_id,))
    
    if not questions:
        return jsonify({'success': False, 'message': '题目不存在'}), 404
    
    question = questions[0]
    
    # 记录查看操作到审计日志
    audit_sql = """
    INSERT INTO 操作审计表 (审计ID, 操作用户ID, 操作类型, 操作对象类型, 操作对象ID, 操作描述, 操作结果) 
    VALUES (%s, %s, '查看', '题目', %s, %s, '成功')
    """
    
    audit_id = f'AUDIT_{datetime.now().strftime("%Y%m%d%H%M%S")}_{user_id}'
    description = f'{session["real_name"]}查看题目{question["题目标题"]}'
    
    execute_update(audit_sql, (audit_id, user_id, question_id, description))
    
    return jsonify({
        'success': True,
        'question': question
    })

@app.route('/api/questions', methods=['POST'])
@login_required
def create_question():
    """创建新题目"""
    user_id = session['user_id']
    data = request.get_json()

    # 验证用户是否有创建题目的权限（教师及以上）
    creator_check_sql = """
    SELECT u.用户类型, r.角色名称
    FROM 个人维度 u
    LEFT JOIN 用户组织关系表 uo ON u.用户ID = uo.用户ID
    LEFT JOIN 角色定义表 r ON uo.角色ID = r.角色ID
    WHERE u.用户ID = %s
    """

    user_info = execute_query(creator_check_sql, (user_id,))
    if not user_info:
        return jsonify({'success': False, 'message': '无法获取用户信息'}), 403

    # 检查用户类型或角色是否允许创建题目
    can_create = False
    for info in user_info:
        user_type = info.get('用户类型', '')
        role_name = info.get('角色名称', '')

        if user_type in ('管理员', '教师'):
            can_create = True
            break
        elif role_name in ('系统管理员', '校长', '学部主任', '年级主任', '班主任', '任课教师'):
            can_create = True
            break

    if not can_create:
        return jsonify({'success': False, 'message': f'您没有创建题目的权限，当前角色：{user_info[0].get("角色名称", "未知")}'}), 403

    # 获取创建者所属主组织路径（取最短路径=学段层）
    org_path_sql = """
    SELECT o.组织路径
    FROM 用户组织关系表 uo
    JOIN 组织树维度 o ON uo.组织ID = o.组织ID
    WHERE uo.用户ID = %s
    ORDER BY LENGTH(o.组织路径)
    LIMIT 1
    """
    org_rows = execute_query(org_path_sql, (user_id,))
    org_path = org_rows[0]['组织路径'] if org_rows else '/unknown'
    # 将完整组织路径截到“学段”层（租户ID/学段ID）
    if org_path != '/unknown':
        segments = org_path.strip('/').split('/')
        # 期望格式: /tenant/phase[/grade[/class]]
        if len(segments) >= 2:
            org_path = '' + '/'.join(segments[:2])      # e.g. /1000/1100

    # 验证必填字段
    required_fields = ['题目标题', '题目内容', '题目类型', '学科', '年级']
    for field in required_fields:
        if not data.get(field):
            return jsonify({'success': False, 'message': f'请填写{field}'}), 400

    # 生成题目ID
    import time
    import random
    question_id = f'Q{int(time.time())}{random.randint(100, 999)}'

    try:
        # 插入题目
        insert_sql = """
        INSERT INTO 题目库 (
            题目ID, 题目标题, 题目内容, 题目类型, 学科, 年级, 组织路径,
            知识点, 难度级别, 答案, 解析,
            选项A, 选项B, 选项C, 选项D, 正确选项,
            创建者ID
        ) VALUES (
            %s, %s, %s, %s, %s, %s, %s,
            %s, %s, %s, %s,
            %s, %s, %s, %s, %s,
            %s
        )
        """

        params = [
            question_id,
            data.get('题目标题'),
            data.get('题目内容'),
            data.get('题目类型'),
            data.get('学科'),
            data.get('年级'),
            org_path,                       # new
            data.get('知识点', ''),
            data.get('难度级别', '中等'),
            data.get('答案', ''),
            data.get('解析', ''),
            data.get('选项A', ''),
            data.get('选项B', ''),
            data.get('选项C', ''),
            data.get('选项D', ''),
            data.get('正确选项', ''),
            user_id
        ]

        result = execute_update(insert_sql, params)

        if result > 0:
            # 为创建者添加所有权
            creator_acl_id = f'ACL_{question_id}_USER_{user_id}'
            creator_acl_sql = """
            INSERT INTO 题目操作ACL表 (
                ACL_ID, 资源ID, 资源类型, 授权对象类型, 授权对象ID,
                权限类型, 权限来源, 申请状态, 创建者ID
            ) VALUES (%s, %s, '题目', 'user', %s, '所有权', '默认继承', '无需申请', %s)
            """
            execute_update(creator_acl_sql, (creator_acl_id, question_id, user_id, user_id))

            # 组织路径已写入题目库，不再需要学段继承ACL
            # （移除 segment_sql、segments 循环等代码块）

            return jsonify({
                'success': True,
                'message': '题目创建成功',
                'question_id': question_id
            })
        else:
            return jsonify({'success': False, 'message': '题目创建失败'}), 500

    except Exception as e:
        print(f"创建题目错误: {str(e)}")
        print(f"题目数据: {data}")
        return jsonify({'success': False, 'message': f'题目创建失败: {str(e)}'}), 500

@app.route('/api/questions/<question_id>/permissions')
@login_required
def get_question_permissions(question_id):
    """获取题目权限信息"""
    user_id = session['user_id']
    
    sql = """
    SELECT 
        acl.*,
        CASE 
            WHEN acl.授权对象类型 = 'user' THEN u.真实姓名
            WHEN acl.授权对象类型 = 'org' THEN o.组织名称
            ELSE acl.授权对象ID
        END as 授权对象名称,
        creator.真实姓名 as 创建者姓名
    FROM 题目操作ACL表 acl
    LEFT JOIN 个人维度 u ON acl.授权对象类型 = 'user' AND acl.授权对象ID = u.用户ID
    LEFT JOIN 组织树维度 o ON acl.授权对象类型 = 'org' AND acl.授权对象ID = o.组织ID
    LEFT JOIN 个人维度 creator ON acl.创建者ID = creator.用户ID
    WHERE acl.资源ID = %s AND acl.资源类型 = '题目'
    ORDER BY acl.权限类型 DESC, acl.创建时间
    """
    
    permissions = execute_query(sql, (question_id,))
    
    return jsonify({
        'success': True,
        'permissions': permissions
    })

@app.route('/api/statistics/overview')
@login_required
def get_overview_statistics():
    """获取概览统计"""
    user_id = session['user_id']
    
    # 用户可访问的题目统计
    accessible_questions_sql = """
    SELECT 
        COUNT(DISTINCT q.题目ID) as total_questions,
        COUNT(DISTINCT CASE WHEN q.学科 = '数学' THEN q.题目ID END) as math_questions,
        COUNT(DISTINCT CASE WHEN q.学科 = '物理' THEN q.题目ID END) as physics_questions,
        COUNT(DISTINCT CASE WHEN q.学科 = '语文' THEN q.题目ID END) as chinese_questions,
        COUNT(DISTINCT CASE WHEN q.学科 = '英语' THEN q.题目ID END) as english_questions
    FROM 题目库 q
    LEFT JOIN 题目操作ACL表 acl ON q.题目ID = acl.资源ID AND acl.资源类型 = '题目'
    WHERE q.是否启用 = TRUE
    AND (
        (acl.授权对象类型 = 'user' AND acl.授权对象ID = %s AND acl.权限类型 IN ('查看', '所有权'))
        OR 
        (acl.授权对象类型 = 'org' AND EXISTS (
            SELECT 1 FROM 用户组织关系表 uo 
            WHERE uo.用户ID = %s 
            AND (
                uo.组织ID = acl.授权对象ID 
                OR uo.组织路径 LIKE CONCAT(acl.权限范围, '%')
                OR acl.权限范围 LIKE CONCAT(uo.组织路径, '%')
            )
            AND (uo.失效时间 IS NULL OR uo.失效时间 > NOW())
        ) AND acl.权限类型 IN ('查看', '所有权'))
    )
    AND acl.申请状态 IN ('无需申请', '已通过')
    """
    
    stats = execute_query(accessible_questions_sql, (user_id, user_id, user_id))
    
    # 用户权限统计
    permissions_sql = """
    SELECT COUNT(DISTINCT p.权限ID) as total_permissions
    FROM 个人维度 u
    JOIN 用户组织关系表 uo ON u.用户ID = uo.用户ID
    JOIN 角色权限桥接表 rp ON uo.角色ID = rp.角色ID
    JOIN 权限定义表 p ON rp.权限ID = p.权限ID
    WHERE u.用户ID = %s
    AND (uo.失效时间 IS NULL OR uo.失效时间 > NOW())
    AND (rp.失效时间 IS NULL OR rp.失效时间 > NOW())
    """
    
    perm_stats = execute_query(permissions_sql, (user_id,))
    
    return jsonify({
        'success': True,
        'statistics': {
            'questions': stats[0] if stats else {},
            'permissions': perm_stats[0] if perm_stats else {}
        }
    })

@app.route('/api/users')
@login_required
def get_users():
    """获取用户列表（仅管理员）"""
    user_type = session.get('user_type')
    
    if user_type != '管理员':
        return jsonify({'success': False, 'message': '权限不足'}), 403
    
    sql = """
    SELECT 
        u.用户ID, u.用户名, u.真实姓名, u.用户类型, u.账户状态,
        GROUP_CONCAT(DISTINCT o.组织名称) as 所属组织,
        GROUP_CONCAT(DISTINCT r.角色名称) as 角色列表
    FROM 个人维度 u
    LEFT JOIN 用户组织关系表 uo ON u.用户ID = uo.用户ID
    LEFT JOIN 组织树维度 o ON uo.组织ID = o.组织ID
    LEFT JOIN 角色定义表 r ON uo.角色ID = r.角色ID
    GROUP BY u.用户ID, u.用户名, u.真实姓名, u.用户类型, u.账户状态
    ORDER BY u.创建时间 DESC
    """
    
    users = execute_query(sql)
    
    return jsonify({
        'success': True,
        'users': users
    })

if __name__ == '__main__':
    print("=" * 60)
    print("🚀 学霸神器数仓 - Flask API 启动")
    print("=" * 60)
    print("访问地址: http://localhost:8080")
    print("API文档:")
    print("  POST /api/login          - 用户登录")
    print("  POST /api/logout         - 用户登出")
    print("  GET  /api/user/permissions - 获取用户权限")
    print("  GET  /api/questions      - 获取题目列表")
    print("  GET  /api/questions/{id} - 获取题目详情")
    print("  GET  /api/statistics/overview - 获取统计信息")
    print("=" * 60)
    
    app.run(debug=True, host='0.0.0.0', port=8080) 