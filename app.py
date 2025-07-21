#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
学霸神器数仓 - 统一权限 Demo(2025-07)
-------------------------------------------------
● 默认规则：题目写入时只记录【学段路径】→ 同学段全部可读
● 编辑 / 跨学段 / 私享   → 仅在 题目操作ACL表 写一行
     perm_level:0=view 1=edit 2=owner
● 超管 & 校长         → 全库可见
-------------------------------------------------
API 一览
  POST /api/login              登录(cookie)
  POST /api/logout             登出
  GET  /api/questions          题目列表（带筛选+分页）
  GET  /api/questions/<id>     题目详情（鉴权）
  POST /api/questions          创建题目
  GET  /api/questions/<id>/permissions 题目 ACL
  GET  /api/statistics/overview 个人可访问题目+权限统计
"""

from flask import Flask, request, jsonify, render_template, session
from flask_cors import CORS
import pymysql, time, random
from datetime import datetime
from functools import wraps

# ────────────────────────────────────────────────────────
# Flask & CORS 基础
# ────────────────────────────────────────────────────────
app = Flask(__name__)
app.secret_key = 'dataware_secret_key_2025'
app.config.update(
    SESSION_COOKIE_SAMESITE='Lax',
    SESSION_COOKIE_SECURE=False,
    SESSION_COOKIE_HTTPONLY=True,
)
CORS(app, supports_credentials=True, origins=['*'])

# ────────────────────────────────────────────────────────
# MySQL 连接 & 通用执行
# ────────────────────────────────────────────────────────
DB_CONFIG = {
    'host': '10.10.0.117',
    'port': 6033,
    'user': 'root',
    'password': 'Xml123&45!',
    'database': 'data_ware_test',
    'charset': 'utf8mb4',
    'autocommit': True
}

def query(sql, params=None, dict_=True):
    try:
        conn = pymysql.connect(**DB_CONFIG)
        cur  = conn.cursor(pymysql.cursors.DictCursor if dict_ else None)
        cur.execute(sql, params or ())
        res = cur.fetchall()
        cur.close(); conn.close()
        return res
    except Exception as e:
        print('[DB‑QUERY]', e)
        return []

def exec_(sql, params=None):
    try:
        conn = pymysql.connect(**DB_CONFIG)
        cur  = conn.cursor()
        cur.execute(sql, params or ())
        row = cur.rowcount
        cur.close(); conn.close()
        return row
    except Exception as e:
        print('[DB‑EXEC]', e)
        return 0

# ────────────────────────────────────────────────────────
# Decorator：登录校验
# ────────────────────────────────────────────────────────
def login_required(fn):
    @wraps(fn)
    def wrap(*a, **kw):
        if 'user_id' not in session:
            return jsonify({'error': '请先登录', 'code': 401}), 401
        return fn(*a, **kw)
    return wrap

# ────────────────────────────────────────────────────────
#   1. 认证
# ────────────────────────────────────────────────────────
@app.route('/api/login', methods=['POST'])
def login():
    """
    登录接口
    - 账户状态需为“正常”
    - 若需校验组织关系（如需限制某些组织），可在此添加相关校验
    """
    body = request.get_json(force=True)
    username = body.get('username')
    if not username:
        return jsonify({'success': False, 'message':'用户名必填'}), 400

    sql = """SELECT 用户ID, 用户名, 真实姓名, 用户类型
             FROM 个人维度
             WHERE 用户名=%s AND 账户状态='正常'"""
    u = query(sql, (username,))
    if not u:
        return jsonify({'success': False, 'message':'账号不存在或禁用'}), 400

    user = u[0]
    session.update({
        'user_id'  : user['用户ID'],
        'username' : user['用户名'],
        'real_name': user['真实姓名'],
        'user_type': user['用户类型']
    })
    return jsonify({'success':True,'user':user,'message':'登录成功'})

@app.route('/api/logout', methods=['POST'])
def logout():
    session.clear()
    return jsonify({'success':True,'message':'已登出'})

# ────────────────────────────────────────────────────────
#   用户信息 profile
# ────────────────────────────────────────────────────────
@app.route('/api/user/profile')
@login_required
def profile():
    return jsonify({
        'success': True,
        'user': {
            'user_id'  : session['user_id'],
            'username' : session['username'],
            'real_name': session['real_name'],
            'user_type': session['user_type']
        }
    })

# ────────────────────────────────────────────────────────
#   2. 通用帮助
# ────────────────────────────────────────────────────────
def is_super_admin(uid: str) -> bool:
    """
    只有“系统管理员” (平台级) 才算 super admin，跨租户无界限。
    校长与校级管理员仍应按本租户路径过滤。
    """
    sql = "SELECT 1 FROM 个人维度 WHERE 用户ID=%s AND 用户类型='系统管理员' LIMIT 1"
    return bool(query(sql, (uid,)))

# (Optional) helper: 校级管理员/校长
def is_tenant_admin(uid: str) -> bool:
    """校级管理员或校长：有 r.角色名称 IN ('校长','管理员')"""
    sql = """
    SELECT 1
    FROM 用户组织关系表 uo
    JOIN 角色定义表       r ON uo.角色ID = r.角色ID
    WHERE uo.用户ID = %s AND r.角色名称 IN ('校长','管理员')
    LIMIT 1"""
    return bool(query(sql, (uid,)))

# ────────────────────────────────────────────────────────
#   用户租户前缀
# ────────────────────────────────────────────────────────
def user_tenant_prefix(uid:str)->str:
    """
    Return tenant-level org id (第一段路径)；若用户尚未挂任何组织，就
    1) 以 T<用户ID> 作为租户根（如 Tother_teacher）
    2) 自动写入组织树维度 + 用户组织关系
    """
    sql = """SELECT o.组织路径
             FROM   用户组织关系表 uo
             JOIN   组织树维度 o ON o.组织ID=uo.组织ID
             WHERE  uo.用户ID=%s
             ORDER  BY LENGTH(o.组织路径)
             LIMIT 1"""
    rows = query(sql,(uid,))
    if rows:
        return rows[0]['组织路径'].strip('/').split('/')[0]

    # —— 未绑定任何组织：为其创建个人租户根 ——
    tenant = f"T{uid}"
    exec_("""INSERT IGNORE INTO 组织树维度(组织ID,组织名称,组织路径)
             VALUES(%s,%s,%s)""", (tenant, f"{tenant}根", tenant))
    exec_("""INSERT IGNORE INTO 用户组织关系表(用户ID,组织ID,组织路径)
             VALUES(%s,%s,%s)""", (uid, tenant, tenant))
    return tenant

# ────────────────────────────────────────────────────────
#   获取默认学段路径 helper
# ────────────────────────────────────────────────────────
def default_school_path(uid: str) -> str:
    """
    Return best‑effort default Org 路径 for this user.
    1. 若用户已绑定组织 → 取其最短路径的前两级 (租户 / 学段)
       - 若该两级节点存在则返回它
       - 否则回退到租户根
    2. 若用户尚未绑定任何组织 → 自动保证租户根节点存在并返回它
        (避免 IndexError: tuple index out of range)
    """
    sql_main = """
        SELECT o.组织路径
        FROM   用户组织关系表 uo
        JOIN   组织树维度     o ON o.组织ID = uo.组织ID
        WHERE  uo.用户ID = %s
        ORDER  BY LENGTH(o.组织路径)
        LIMIT 1
    """
    rows = query(sql_main, (uid,))
    if rows:
        path = rows[0]['组织路径']
        segs = path.strip('/').split('/')
        # 至少 2 段：租户 / 学段
        if len(segs) >= 2:
            candidate = '/'.join(segs[:2])
            if query("SELECT 1 FROM 组织树维度 WHERE 组织路径=%s LIMIT 1", (candidate,)):
                return candidate
        # 回退到租户根
        return segs[0]

    # —— 用户尚未绑定任何组织：自动创建租户根节点 ——
    tenant = user_tenant_prefix(uid)  # e.g. '2000'
    # 写入组织树根（如果不存在）
    exec_("""INSERT IGNORE INTO 组织树维度(组织ID,组织名称,组织路径)
             VALUES(%s,%s,%s)""", (tenant, f"{tenant}校", tenant))
    # 绑定用户 → 租户根
    exec_("""INSERT IGNORE INTO 用户组织关系表(用户ID,组织ID,组织路径)
             VALUES(%s,%s,%s)""", (uid, tenant, tenant))
    return tenant

def ensure_private_org(uid:str)->str:
    """
    Ensure a PRIVATE org node exists for this user and relation row is present.
    Returns the private path like '1000/PRIVATE/U006'.
    """
    tenant = user_tenant_prefix(uid)
    priv_path = f"{tenant}/PRIVATE/{uid}"
    priv_org_id = f"PRV_{uid}"
    # 1. 组织树
    exec_("""INSERT IGNORE INTO 组织树维度(组织ID,组织名称,组织路径)
             VALUES(%s,'私有',%s)""", (priv_org_id, priv_path))
    # 2. 用户组织关系
    exec_("""INSERT IGNORE INTO 用户组织关系表(用户ID,组织ID,组织路径)
             VALUES(%s,%s,%s)""", (uid, priv_org_id, priv_path))
    return priv_path

# ────────────────────────────────────────────────────────
#   组织树API
# ────────────────────────────────────────────────────────
@app.route('/api/org/tree')
@login_required
def org_tree():
    uid = session['user_id']
    tenant = user_tenant_prefix(uid)
    sql = """
        SELECT 组织ID, 组织名称, 组织路径
        FROM   组织树维度
        WHERE  REPLACE(组织路径,'/','') LIKE CONCAT(%s, '%%')
        ORDER  BY 组织路径
    """
    rows = query(sql, (tenant,))
    return jsonify({'success': True, 'data': rows})

# ────────────────────────────────────────────────────────
#   3. 题目列表  (路径继承  OR  ACL)
# ────────────────────────────────────────────────────────
@app.route('/api/questions')
@login_required
def get_questions():
    uid   = session['user_id']
    page  = int(request.args.get('page',1))
    limit = int(request.args.get('limit',20))
    offset= (page-1)*limit

    filters=[]
    params=[uid, uid, uid, uid]   # author compare + acl user + join + acl user

    for col,arg in (('学科','subject'),('年级','grade'),('难度级别','difficulty'),('题目类型','type')):
        v=request.args.get(arg,'')
        if v: filters.append(f"AND q.{col}=%s") or params.append(v)

    if is_super_admin(uid):
        base = "SELECT q.*,1 AS can_edit FROM 题目库 q WHERE q.是否启用=1"
        params=[]                 # 超管不需 uid
    else:
        base = """
        SELECT DISTINCT q.*,
               IF(q.创建者ID=%s
                  OR (acl.perm_level>=1
                      AND ((acl.授权对象类型='user' AND acl.授权对象ID=%s)
                           OR (acl.授权对象类型='org' AND acl.授权对象ID = uo.组织ID))),1,0) AS can_edit
        FROM 题目库 q
        JOIN 用户组织关系表 uo
          ON uo.用户ID=%s AND (uo.失效时间 IS NULL OR uo.失效时间>NOW())
        LEFT JOIN 题目操作ACL表 acl
          ON acl.资源ID=q.题目ID
         AND acl.perm_level>=0
         AND acl.审核状态='已通过'
        WHERE q.是否启用=1
          AND (
               REPLACE(q.组织路径,'/','') LIKE CONCAT(REPLACE(uo.组织路径,'/',''),CHAR(37))
            OR REPLACE(uo.组织路径,'/','') LIKE CONCAT(REPLACE(q.组织路径,'/',''),CHAR(37))
            OR ( (acl.授权对象类型='user' AND acl.授权对象ID=%s)
              OR (acl.授权对象类型='org'  AND acl.授权对象ID = uo.组织ID) )
          )
        """

    where  =" ".join(filters)
    sql_li = f"{base} {where} ORDER BY q.创建时间 DESC LIMIT {limit} OFFSET {offset}"
    sql_ct = f"{base.replace('SELECT DISTINCT q.*','SELECT COUNT(DISTINCT q.题目ID) AS ct')} {where}"

    rows   = query(sql_li, params)
    # 计算总数：超管分支的 sql_ct 可能没有 ct 别名，安全处理
    total_res = query(sql_ct, params)
    if total_res and 'ct' in total_res[0]:
        total = total_res[0]['ct']
    else:
        # fallback：直接用 rows 数量（超管 / 空过滤时）
        total = len(rows)
    return jsonify({'success':True,'questions':rows,'total':total,'page':page,'limit':limit})

# ────────────────────────────────────────────────────────
#   4. 题目详情  (同权限判定)
# ────────────────────────────────────────────────────────
@app.route('/api/questions/<qid>')
@login_required
def question_detail(qid):
    uid=session['user_id']
    if not is_super_admin(uid):
        chk="""
        SELECT 1
        FROM 题目库 q
        JOIN 用户组织关系表 uo ON uo.用户ID=%s
        LEFT JOIN 题目操作ACL表 acl
          ON acl.资源ID=q.题目ID AND acl.perm_level>=0 AND acl.审核状态='已通过'
        WHERE q.题目ID=%s
          AND ( REPLACE(q.组织路径,'/','') LIKE CONCAT(REPLACE(uo.组织路径,'/',''),CHAR(37))
                OR REPLACE(uo.组织路径,'/','') LIKE CONCAT(REPLACE(q.组织路径,'/',''),CHAR(37))
                OR ( (acl.授权对象类型='user' AND acl.授权对象ID=%s)
                  OR (acl.授权对象类型='org'  AND acl.授权对象ID = uo.组织ID) )
              ) LIMIT 1"""
        if not query(chk,(uid,qid,uid)):
            return jsonify({'success':False,'message':'无权限查看'}),403

    row=query("SELECT q.*,u.真实姓名 创建者姓名 FROM 题目库 q LEFT JOIN 个人维度 u ON u.用户ID=q.创建者ID WHERE q.题目ID=%s",(qid,))
    if not row: return jsonify({'success':False,'message':'题目不存在'}),404
    return jsonify({'success':True,'question':row[0]})

# ────────────────────────────────────────────────────────
#   5. 创建题目  (路径=学段, 默认启用)
# ────────────────────────────────────────────────────────
@app.route('/api/questions',methods=['POST'])
@login_required
def create_question():
    uid=session['user_id']; body=request.get_json(force=True)
    only_me = bool(body.get('仅自己可见'))

    # ① 计算默认归类路径（学段若存在，否则租户根）
    org_path = default_school_path(uid)  # e.g. 1000/1100  or  1000

    if only_me:
        org_path = ensure_private_org(uid)
    else:
        custom_path = body.get('组织路径') or ''
        if custom_path:
            chk = query("SELECT 1 FROM 组织树维度 WHERE 组织路径=%s LIMIT 1", (custom_path,))
            if chk:
                org_path = custom_path.strip('/')

    # ② 插题目
    qid=f"Q{int(time.time())}{random.randint(100,999)}"
    ins="""INSERT INTO 题目库(
            题目ID,题目标题,题目内容,题目类型,学科,年级,组织路径,
            难度级别,创建者ID,是否启用)
           VALUES(%s,%s,%s,%s,%s,%s,%s,%s,%s,1)"""
    exec_(ins,(qid,body['题目标题'],body['题目内容'],body['题目类型'],
               body['学科'],body['年级'],org_path,
               body.get('难度级别','中等'),uid))

    # ③ 插作者所有权 ACL
    acl="""INSERT INTO 题目操作ACL表(
           ACL_ID,资源ID,资源类型,授权对象类型,授权对象ID,perm_level,权限类型,权限来源,申请状态,创建者ID)
           VALUES(%s,%s,'题目','user',%s,2,'所有权','作者','无需申请',%s)"""
    exec_(acl,(f'ACL_{qid}_{uid}',qid,uid,uid))
    return jsonify({'success':True,'message':'题目创建成功','question_id':qid})

# ────────────────────────────────────────────────────────
#   5-b. 编辑 / 创建副本
# ────────────────────────────────────────────────────────
@app.route('/api/questions/<qid>', methods=['PUT'])
@login_required
def edit_question(qid):
    uid  = session['user_id']
    data = request.get_json(force=True)

    # ① 判断是否有覆盖权限
    chk_sql = """
        SELECT 1
        FROM 题目库 q
        LEFT JOIN 题目操作ACL表 acl
               ON acl.资源ID=q.题目ID AND acl.perm_level>=1
        WHERE q.题目ID=%s
          AND (q.创建者ID=%s
               OR (acl.授权对象类型='user' AND acl.授权对象ID=%s)
               OR (acl.授权对象类型='org' AND acl.授权对象ID IN
                     (SELECT 组织ID FROM 用户组织关系表
                      WHERE 用户ID=%s
                        AND (失效时间 IS NULL OR 失效时间>NOW()))))
        LIMIT 1"""
    can_overwrite = bool(query(chk_sql, (qid, uid, uid, uid)))

    # ② 覆盖 or Fork
    if can_overwrite and data.get('overwrite'):
        exec_("UPDATE 题目库 SET 题目内容=%s WHERE 题目ID=%s",
              (data['题目内容'], qid))
        return jsonify({'success': True, 'mode': 'overwrite'})

    new_qid = f"Q{int(time.time())}{random.randint(100,999)}"
    fork_sql = '''
        INSERT INTO 题目库
        (题目ID,题目标题,题目内容,题目类型,学科,年级,组织路径,
         难度级别,创建者ID,是否启用,parent_id)
        SELECT %s, 题目标题, %s, 题目类型, 学科, 年级, 组织路径,
               难度级别, %s, 1, 题目ID
        FROM 题目库 WHERE 题目ID=%s
    '''
    exec_(fork_sql, (new_qid, data['题目内容'], uid, qid))
    return jsonify({'success': True, 'mode': 'fork', 'question_id': new_qid})

# ────────────────────────────────────────────────────────
#   5-c. 申请编辑 / 审批
# ────────────────────────────────────────────────────────
@app.route('/api/questions/<qid>/request-edit', methods=['POST'])
@login_required
def request_edit(qid):
    uid = session['user_id']
    # 已存在待审核或已通过记录则不再重复
    exist = query("""SELECT 1 FROM 题目操作ACL表
                     WHERE 资源ID=%s AND perm_level=1
                       AND 授权对象类型='user' AND 授权对象ID=%s
                       AND 审核状态 IN ('待审核','已通过')""",
                  (qid, uid))
    if exist:
        return jsonify({'success': False, 'message': '已申请或已拥有编辑权'}), 400

    acl_id = f"REQ_{qid}_{uid}"
    ok = exec_("""
    INSERT INTO 题目操作ACL表
    (ACL_ID,资源ID,资源类型,授权对象类型,授权对象ID,
    perm_level,权限类型,权限来源,申请状态,
    创建者ID,申请者ID,申请时间,审核状态)
    VALUES(%s,%s,'题目','user',%s,
        1,'编辑','申请获得','待审批',
        %s,%s,NOW(),'待审核')
    """, (acl_id, qid, uid, uid, uid))

    if ok:
        return jsonify({'success': True, 'acl_id': acl_id})
    else:                         # 保证失败也返回
        return jsonify({'success': False,
                        'message': 'ACL 写入失败，请查看后台日志'}), 500

@app.route('/api/questions/<qid>/edit-requests', methods=['GET'])
@login_required
def list_edit_requests(qid):
    uid = session['user_id']
    # 必须是 owner 或 超管
    owner_chk = query("SELECT 1 FROM 题目库 WHERE 题目ID=%s AND 创建者ID=%s",(qid,uid))
    if not (owner_chk or is_super_admin(uid)):
        return jsonify({'success': False, 'message': '无权限查看'}), 403
    rows = query("""SELECT ACL_ID, 授权对象ID AS 申请人ID, 申请时间
                    FROM 题目操作ACL表
                    WHERE 资源ID=%s AND 审核状态='待审核'""",(qid,))
    return jsonify({'success': True, 'requests': rows})

@app.route('/api/questions/<qid>/edit-requests/<acl_id>/resolve', methods=['POST'])
@login_required
def resolve_edit_request(qid, acl_id):
    uid = session['user_id']
    action = request.get_json(force=True).get('action')
    if action not in ('approve','reject'):
        return jsonify({'success': False,'message':'action 必须为 approve / reject'}),400
    # owner / super admin check
    owner_chk = query("SELECT 1 FROM 题目库 WHERE 题目ID=%s AND 创建者ID=%s",(qid,uid))
    if not (owner_chk or is_super_admin(uid)):
        return jsonify({'success': False,'message':'无权限操作'}),403
    new_status = '已通过' if action=='approve' else '已拒绝'
    exec_("""UPDATE 题目操作ACL表
             SET 审核状态=%s, 申请状态=%s, 审批者ID=%s
             WHERE ACL_ID=%s AND 资源ID=%s""",
          (new_status,new_status,uid,acl_id,qid))
    return jsonify({'success': True})

# ────────────────────────────────────────────────────────
#   6. 题目 ACL 列表
# ────────────────────────────────────────────────────────
@app.route('/api/questions/<qid>/permissions')
@login_required
def q_acl(qid):
    sql="""SELECT acl.*,COALESCE(u.真实姓名,o.组织名称,acl.授权对象ID) 授权对象名称
           FROM 题目操作ACL表 acl
           LEFT JOIN 个人维度   u ON acl.授权对象类型='user' AND acl.授权对象ID=u.用户ID
           LEFT JOIN 组织树维度 o ON acl.授权对象类型='org'  AND acl.授权对象ID=o.组织ID
           WHERE acl.资源ID=%s"""
    return jsonify({'success':True,'permissions':query(sql,(qid,))})

# ────────────────────────────────────────────────────────
#   7. 概览统计 = 与列表同 WHERE
# ────────────────────────────────────────────────────────
@app.route('/api/statistics/overview')
@login_required
def stats():
    uid=session['user_id']
    if is_super_admin(uid):
        total=query("SELECT COUNT(*) ct FROM 题目库 WHERE 是否启用=1")[0]['ct']
        maths=query("SELECT COUNT(*) ct FROM 题目库 WHERE 是否启用=1 AND 学科='数学'")[0]['ct']
        phys=query("SELECT COUNT(*) ct FROM 题目库 WHERE 是否启用=1 AND 学科='物理'")[0]['ct']
    else:
        base="""FROM 题目库 q
                JOIN 用户组织关系表 uo ON uo.用户ID=%s
                LEFT JOIN 题目操作ACL表 acl ON acl.资源ID=q.题目ID AND acl.perm_level>=0 AND acl.审核状态='已通过'
                WHERE q.是否启用=1
                  AND (
                       REPLACE(q.组织路径,'/','') LIKE CONCAT(REPLACE(uo.组织路径,'/',''),CHAR(37))
                    OR REPLACE(uo.组织路径,'/','') LIKE CONCAT(REPLACE(q.组织路径,'/',''),CHAR(37))
                    OR ( (acl.授权对象类型='user' AND acl.授权对象ID=%s)
                      OR (acl.授权对象类型='org'  AND acl.授权对象ID = uo.组织ID) )
                  )"""
        params=(uid,uid)
        total=query(f"SELECT COUNT(DISTINCT q.题目ID) ct {base}",params)[0]['ct']
        maths=query(f"SELECT COUNT(DISTINCT q.题目ID) ct {base} AND q.学科='数学'",params)[0]['ct']
        phys=query(f"SELECT COUNT(DISTINCT q.题目ID) ct {base} AND q.学科='物理'",params)[0]['ct']
    return jsonify({'success':True,'statistics':{
        'questions':{
            'total_questions':total,
            'math_questions' :maths,
            'physics_questions':phys
        }
    }})

# ────────────────────────────────────────────────────────
# HTTP root
# ────────────────────────────────────────────────────────
@app.route('/')
def index(): return render_template('index.html')

# ────────────────────────────────────────────────────────
if __name__ == '__main__':
    print('🚀 Flask running on http://localhost:8080')
    app.run(debug=True,host='0.0.0.0',port=8080)