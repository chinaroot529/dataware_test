-- ===================================
-- 学霸神器数仓 - 权限系统验证查询
-- 验证各种权限场景是否按预期工作
-- ===================================

USE data_ware_test;

-- ===================================
-- 1. 基础权限验证查询
-- ===================================

-- 查询用户的所有权限（权限展开查询）
-- 示例：查看孙老师(U008)的所有权限
SELECT 
    u.真实姓名,
    u.用户类型,
    uo.组织路径,
    o.组织名称,
    r.角色名称,
    p.权限名称,
    p.权限代码,
    uo.关系类型,
    CASE 
        WHEN uo.失效时间 IS NULL THEN '长期有效'
        WHEN uo.失效时间 > NOW() THEN '有效'
        ELSE '已过期'
    END AS 权限状态
FROM 个人维度 u
JOIN 用户组织关系表 uo ON u.用户ID = uo.用户ID
JOIN 组织树维度 o ON uo.组织ID = o.组织ID
JOIN 角色定义表 r ON uo.角色ID = r.角色ID
JOIN 角色权限桥接表 rp ON r.角色ID = rp.角色ID
JOIN 权限定义表 p ON rp.权限ID = p.权限ID
WHERE u.用户ID = 'U008'  -- 孙老师
ORDER BY o.组织路径, p.权限代码;

-- ===================================
-- 2. 题目权限验证查询
-- ===================================

-- 查询用户可以访问的题目
-- 示例：查看孙老师可以访问哪些题目
SELECT DISTINCT
    acl.资源ID,
    acl.资源类型,
    acl.权限类型,
    acl.权限来源,
    acl.申请状态,
    CASE 
        WHEN acl.授权对象类型 = 'user' AND acl.授权对象ID = 'U008' THEN '个人权限'
        WHEN acl.授权对象类型 = 'org' AND EXISTS (
            SELECT 1 FROM 用户组织关系表 uo 
            WHERE uo.用户ID = 'U008' 
            AND (uo.组织ID = acl.授权对象ID OR uo.组织路径 LIKE CONCAT(acl.权限范围, '%'))
        ) THEN '组织权限'
        ELSE '无权限'
    END AS 权限来源类型
FROM 题目操作ACL表 acl
WHERE (
    -- 个人权限
    (acl.授权对象类型 = 'user' AND acl.授权对象ID = 'U008')
    OR 
    -- 组织权限
    (acl.授权对象类型 = 'org' AND EXISTS (
        SELECT 1 FROM 用户组织关系表 uo 
        WHERE uo.用户ID = 'U008' 
        AND (
            uo.组织ID = acl.授权对象ID 
            OR uo.组织路径 LIKE CONCAT(acl.权限范围, '%')
            OR acl.权限范围 LIKE CONCAT(uo.组织路径, '%')
        )
    ))
)
AND acl.申请状态 IN ('无需申请', '已通过')
AND (acl.失效时间 IS NULL OR acl.失效时间> NOW());

-- ===================================
-- 3. 跨班级任课权限验证
-- ===================================

-- 验证跨班级任课教师的权限范围
-- 孙老师教高一二班和高二三班，检查她的权限覆盖
SELECT 
    u.真实姓名,
    uo.组织路径,
    o.组织名称,
    uo.关系类型,
    r.角色名称,
    COUNT(DISTINCT p.权限ID) as 权限数量,
    GROUP_CONCAT(DISTINCT p.权限名称) as 权限列表
FROM 个人维度 u
JOIN 用户组织关系表 uo ON u.用户ID = uo.用户ID
JOIN 组织树维度 o ON uo.组织ID = o.组织ID
JOIN 角色定义表 r ON uo.角色ID = r.角色ID
JOIN 角色权限桥接表 rp ON r.角色ID = rp.角色ID
JOIN 权限定义表 p ON rp.权限ID = p.权限ID
WHERE u.用户ID = 'U008'  -- 孙老师
GROUP BY uo.组织路径, o.组织名称, uo.关系类型, r.角色名称
ORDER BY uo.组织路径;

-- ===================================
-- 4. 权限申请流程验证
-- ===================================

-- 查看所有待审批的权限申请
SELECT 
    pa.申请ID,
    申请者.真实姓名 as 申请者,
    pa.申请类型,
    pa.目标资源ID,
    pa.申请权限类型,
    pa.申请原因,
    pa.申请状态,
    pa.申请时间,
    -- 查找应该审批的人（资源所有者）
    COALESCE(审批者.真实姓名, '待确定') as 应审批者
FROM 权限申请表 pa
JOIN 个人维度 申请者 ON pa.申请者ID = 申请者.用户ID
LEFT JOIN 题目操作ACL表 acl ON pa.目标资源ID = acl.资源ID AND acl.权限类型 = '所有权'
LEFT JOIN 个人维度 审批者 ON acl.授权对象ID = 审批者.用户ID
WHERE pa.申请状态 = '待审批'
ORDER BY pa.申请时间;

-- ===================================
-- 5. 临时权限验证
-- ===================================

-- 查看所有临时权限及其有效期
SELECT 
    u.真实姓名,
    u.用户类型,
    o.组织名称,
    uo.关系类型,
    r.角色名称,
    uo.生效时间,
    uo.失效时间,
    CASE 
        WHEN uo.失效时间 IS NULL THEN '永久有效'
        WHEN uo.失效时间 > NOW() THEN CONCAT('还有', DATEDIFF(uo.失效时间, NOW()), '天到期')
        ELSE '已过期'
    END AS 权限状态,
    DATEDIFF(COALESCE(uo.失效时间, DATE_ADD(NOW(), INTERVAL 1 YEAR)), uo.生效时间) as 权限持续天数
FROM 用户组织关系表 uo
JOIN 个人维度 u ON uo.用户ID = u.用户ID
JOIN 组织树维度 o ON uo.组织ID = o.组织ID
JOIN 角色定义表 r ON uo.角色ID = r.角色ID
WHERE uo.关系类型 = '临时权限'
ORDER BY uo.失效时间;

-- ===================================
-- 6. 权限冲突检测
-- ===================================

-- 检测同一用户在不同组织中的权限冲突
SELECT 
    u.真实姓名,
    COUNT(DISTINCT uo.组织ID) as 组织数量,
    COUNT(DISTINCT uo.角色ID) as 角色数量,
    GROUP_CONCAT(DISTINCT o.组织名称) as 所属组织,
    GROUP_CONCAT(DISTINCT r.角色名称) as 担任角色,
    CASE 
        WHEN COUNT(DISTINCT uo.角色ID) > 1 THEN '存在多角色'
        ELSE '角色一致'
    END as 角色状态
FROM 个人维度 u
JOIN 用户组织关系表 uo ON u.用户ID = uo.用户ID
JOIN 组织树维度 o ON uo.组织ID = o.组织ID
JOIN 角色定义表 r ON uo.角色ID = r.角色ID
WHERE u.用户类型 = '教师'
AND (uo.失效时间 IS NULL OR uo.失效时间> NOW())
GROUP BY u.用户ID, u.真实姓名
HAVING COUNT(DISTINCT uo.组织ID) > 1  -- 只显示跨组织的用户
ORDER BY 组织数量 DESC;

-- ===================================
-- 7. 组织权限继承验证
-- ===================================

-- 验证组织权限继承关系
-- 查看每个组织层级可以访问下级组织资源的情况
WITH 组织层级权限 AS (
    SELECT 
        o1.组织ID,
        o1.组织名称,
        o1.组织层级,
        o1.组织路径,
        COUNT(DISTINCT acl.资源ID) as 可访问资源数,
        GROUP_CONCAT(DISTINCT acl.资源ID) as 可访问资源列表
    FROM 组织树维度 o1
    LEFT JOIN 题目操作ACL表 acl ON (
        acl.授权对象类型 = 'org' 
        AND (
            acl.授权对象ID = o1.组织ID
            OR acl.权限范围 LIKE CONCAT(o1.组织路径, '%')
        )
    )
    GROUP BY o1.组织ID, o1.组织名称, o1.组织层级, o1.组织路径
)
SELECT 
    组织层级,
    组织名称,
    组织路径,
    可访问资源数,
    可访问资源列表
FROM 组织层级权限
ORDER BY 组织层级, 组织路径;

-- ===================================
-- 8. 权限使用统计
-- ===================================

-- 统计权限使用情况
SELECT 
    p.权限名称,
    p.资源类型,
    p.操作类型,
    COUNT(DISTINCT rp.角色ID) as 关联角色数,
    COUNT(DISTINCT uo.用户ID) as 拥有用户数,
    GROUP_CONCAT(DISTINCT r.角色名称) as 关联角色
FROM 权限定义表 p
LEFT JOIN 角色权限桥接表 rp ON p.权限ID = rp.权限ID
LEFT JOIN 角色定义表 r ON rp.角色ID = r.角色ID
LEFT JOIN 用户组织关系表 uo ON r.角色ID = uo.角色ID
WHERE (rp.失效时间 IS NULL OR rp.失效时间 > NOW())
AND (uo.失效时间 IS NULL OR uo.失效时间 > NOW())
GROUP BY p.权限ID, p.权限名称, p.资源类型, p.操作类型
ORDER BY 拥有用户数 DESC;

-- ===================================
-- 9. 审计日志分析
-- ===================================

-- 分析用户操作行为
SELECT 
    u.真实姓名,
    u.用户类型,
    audit.操作类型,
    audit.操作对象类型,
    COUNT(*) as 操作次数,
    MAX(audit.操作时间) as 最后操作时间,
    COUNT(CASE WHEN audit.操作结果 = '成功' THEN 1 END) as 成功次数,
    COUNT(CASE WHEN audit.操作结果 = '失败' THEN 1 END) as 失败次数,
    ROUND(COUNT(CASE WHEN audit.操作结果 = '成功' THEN 1 END) * 100.0 / COUNT(*), 2) as 成功率
FROM 操作审计表 audit
JOIN 个人维度 u ON audit.操作用户ID = u.用户ID
WHERE audit.操作时间 >= DATE_SUB(NOW(), INTERVAL 30 DAY)  -- 最近30天
GROUP BY u.用户ID, u.真实姓名, u.用户类型, audit.操作类型, audit.操作对象类型
HAVING 操作次数 >= 1
ORDER BY 操作次数 DESC;

-- ===================================
-- 10. 权限问题诊断查询
-- ===================================

-- 诊断可能的权限问题
-- 1. 查找没有任何权限的用户
SELECT 
    u.用户ID,
    u.真实姓名,
    u.用户类型,
    u.账户状态,
    '没有组织关系' as 问题类型
FROM 个人维度 u
LEFT JOIN 用户组织关系表 uo ON u.用户ID = uo.用户ID
WHERE uo.用户ID IS NULL
AND u.账户状态 = '正常'

UNION ALL

-- 2. 查找权限已过期的用户
SELECT 
    u.用户ID,
    u.真实姓名,
    u.用户类型,
    u.账户状态,
    '权限已过期' as 问题类型
FROM 个人维度 u
JOIN 用户组织关系表 uo ON u.用户ID = uo.用户ID
WHERE uo.失效时间 < NOW()
AND u.账户状态 = '正常'

UNION ALL

-- 3. 查找角色权限配置异常的情况
SELECT 
    u.用户ID,
    u.真实姓名,
    u.用户类型,
    u.账户状态,
    '角色无权限' as 问题类型
FROM 个人维度 u
JOIN 用户组织关系表 uo ON u.用户ID = uo.用户ID
JOIN 角色定义表 r ON uo.角色ID = r.角色ID
LEFT JOIN 角色权限桥接表 rp ON r.角色ID = rp.角色ID
WHERE rp.角色ID IS NULL
AND u.账户状态 = '正常'
AND r.是否启用 = TRUE

ORDER BY 问题类型, 真实姓名; 