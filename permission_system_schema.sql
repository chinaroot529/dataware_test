-- ===================================
-- 学霸神器数仓 - 权限系统数据模型
-- 数据库：data_ware_test
-- 创建时间：2024
-- ===================================

-- 切换到目标数据库
USE data_ware_test;

-- ===================================
-- 1. 组织架构相关表
-- ===================================

-- 组织树维度表
CREATE TABLE 组织树维度 (
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
) COMMENT='组织树维度表，支持学校-学部-年级-班级的层级结构';

-- 角色定义表
CREATE TABLE 角色定义表 (
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
) COMMENT='角色定义表，定义系统中的所有角色类型';

-- ===================================
-- 2. 用户相关表
-- ===================================

-- 个人维度表
CREATE TABLE 个人维度 (
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
) COMMENT='个人维度表，存储所有用户的基本信息';

-- 用户组织关系表
CREATE TABLE 用户组织关系表 (
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
    INDEX idx_effective_time (生效时间, 失效时间),
    FOREIGN KEY (用户ID) REFERENCES 个人维度(用户ID),
    FOREIGN KEY (组织ID) REFERENCES 组织树维度(组织ID),
    FOREIGN KEY (角色ID) REFERENCES 角色定义表(角色ID)
) COMMENT='用户组织关系表，支持用户在多个组织中担任不同角色';

-- ===================================
-- 3. 权限相关表
-- ===================================

-- 权限定义表
CREATE TABLE 权限定义表 (
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
) COMMENT='权限定义表，定义系统中的所有权限类型';

-- 角色权限桥接表
CREATE TABLE 角色权限桥接表 (
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
    FOREIGN KEY (角色ID) REFERENCES 角色定义表(角色ID),
    FOREIGN KEY (权限ID) REFERENCES 权限定义表(权限ID),
    UNIQUE KEY uk_role_permission (角色ID, 权限ID)
) COMMENT='角色权限桥接表，定义角色拥有的权限';

-- ===================================
-- 4. 资源权限控制表
-- ===================================

-- 题目操作ACL表
CREATE TABLE 题目操作ACL表 (
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
) COMMENT='题目操作ACL表，控制对题目等资源的访问权限';

-- 权限申请表
CREATE TABLE 权限申请表 (
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
    INDEX idx_apply_time (申请时间),
    FOREIGN KEY (申请者ID) REFERENCES 个人维度(用户ID)
) COMMENT='权限申请表，记录所有权限申请和审批流程';

-- ===================================
-- 5. 审计和日志表
-- ===================================

-- 操作审计表
CREATE TABLE 操作审计表 (
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
    INDEX idx_operation_result (操作结果),
    FOREIGN KEY (操作用户ID) REFERENCES 个人维度(用户ID)
) COMMENT='操作审计表，记录所有重要操作的审计日志';

-- 权限变更日志表
CREATE TABLE 权限变更日志表 (
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
    INDEX idx_change_time (变更时间),
    FOREIGN KEY (目标用户ID) REFERENCES 个人维度(用户ID),
    FOREIGN KEY (目标角色ID) REFERENCES 角色定义表(角色ID),
    FOREIGN KEY (目标组织ID) REFERENCES 组织树维度(组织ID),
    FOREIGN KEY (操作者ID) REFERENCES 个人维度(用户ID)
) COMMENT='权限变更日志表，记录所有权限相关的变更历史'; 