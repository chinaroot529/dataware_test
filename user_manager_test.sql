/* ────────────────────────────────
   1. 组织树维度  (静态空间层级)
   ──────────────────────────────── */
CREATE TABLE xbsq_dwd_user_organization_dim (
    org_id            BIGINT PRIMARY KEY,
    org_name          VARCHAR(100) NOT NULL,
    org_type          ENUM('school','stage','grade','class','department') NOT NULL,
    parent_org_id     BIGINT,
    org_path          VARCHAR(255) NOT NULL,
    is_used           TINYINT(1) DEFAULT 1 COMMENT '1=启用 0=停用',
    created_time      DATETIME DEFAULT CURRENT_TIMESTAMP,
    updated_time      DATETIME DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
    UNIQUE KEY uk_org_path (org_path)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4
COMMENT='组织树维度  (静态空间层级)';

/* 示例组织层级：
   1000 = 博雅中学 (school)
   1100 = 高中部     (stage)
   1110 = 高一年级   (grade)
   1111 = 高一(1)班 模板 (class)
   1112 = 高一(2)班 模板 (class)
*/
INSERT INTO xbsq_dwd_user_organization_dim
(org_id,org_name,org_type,parent_org_id,org_path)
VALUES
(1000,'博雅中学','school',NULL,'1000'),
(1100,'高中部','stage',1000,'1000/1100'),
(1110,'高一年级','grade',1100,'1000/1100/1110'),
(1111,'高一(1)班','class',1110,'1000/1100/1110/1111'),
(1112,'高一(2)班','class',1110,'1000/1100/1110/1112');

/* ────────────────────────────────
   2. 角色定义表
   ──────────────────────────────── */
CREATE TABLE xbsq_dwd_user_role_def_dim (
    role_id               BIGINT PRIMARY KEY,
    role_name             VARCHAR(60) NOT NULL,
    role_type             ENUM('system','business','function') DEFAULT 'business',
    role_desc             VARCHAR(255),
    is_sys_role           TINYINT(1) DEFAULT 0,
    competence_scope      ENUM('global','school','stage','grade','class','personal','cross') DEFAULT 'class',
    competence_inherit_lv INT DEFAULT 0 COMMENT '0=不继承,-1=向下无限继承,正数=层级数',
    is_used               TINYINT(1) DEFAULT 1,
    effective_time        DATETIME DEFAULT CURRENT_TIMESTAMP
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4
COMMENT='角色定义表';

INSERT INTO xbsq_dwd_user_role_def_dim
(role_id,role_name,role_type,role_desc,is_sys_role,competence_scope)
VALUES
(1,'系统管理员','system','平台最高权限',1,'global'),
(10,'班主任','business','班级负责人',0,'class'),
(11,'任课老师','business','授课教师',0,'class'),
(20,'学生','business','在校学生',0,'class'),
(30,'家长','business','学生家长/监护人',0,'personal');

/* ────────────────────────────────
   3. 个人维度表
   ──────────────────────────────── */
CREATE TABLE xbsq_dwd_user_personal_dim (
    user_id            BIGINT PRIMARY KEY,
    name               VARCHAR(50) NOT NULL,
    gender             ENUM('M','F') DEFAULT 'M',
    birth_date         DATE,
    phone              VARCHAR(20),
    email              VARCHAR(100),
    subject_id         VARCHAR(30),
    registration_channel VARCHAR(50),
    registration_time  DATETIME DEFAULT CURRENT_TIMESTAMP,
    user_status        ENUM('active','frozen','deleted') DEFAULT 'active'
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4
COMMENT='个人维度表';

INSERT INTO xbsq_dwd_user_personal_dim
(user_id,name,gender,birth_date,phone,email,subject_id,registration_channel)
VALUES
(50001,'李老师','F','1985-03-01','13800000001','li.teacher@example.com','CHINESE','import'),
(50002,'张老师','M','1984-11-12','13800000002','zhang.teacher@example.com','MATH','import'),
(70001,'小明','M','2009-08-15','13900000001',NULL,NULL,'app'),
(70002,'小红','F','2009-12-01','13900000002',NULL,NULL,'app'),
(80001,'小明家长','F','1980-05-10','13700000001',NULL,NULL,'app');

/* ────────────────────────────────
   4. 用户-组织-角色桥接表  (可追溯历史)
   ──────────────────────────────── */
CREATE TABLE xbsq_dwd_user_role_org_bridge (
    user_id        BIGINT NOT NULL,
    role_id        BIGINT NOT NULL,
    org_id         BIGINT NOT NULL,
    org_path       VARCHAR(255) NOT NULL,
    effective_time DATETIME NOT NULL,
    PRIMARY KEY (user_id, role_id, org_id, effective_time),
    CONSTRAINT fk_uobr_user FOREIGN KEY (user_id) REFERENCES xbsq_dwd_user_personal_dim(user_id),
    CONSTRAINT fk_uobr_role FOREIGN KEY (role_id) REFERENCES xbsq_dwd_user_role_def_dim(role_id),
    CONSTRAINT fk_uobr_org  FOREIGN KEY (org_id)  REFERENCES xbsq_dwd_user_organization_dim(org_id)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4
COMMENT='用户-组织-角色桥接表  (可追溯历史)';

-- 示例：李老师属于 2024 学年高一(1)班(模板 org 1111)，担任班主任
INSERT INTO xbsq_dwd_user_role_org_bridge
VALUES
(50001,10,1111,'1000/1100/1110/1111','2024-08-20 00:00:00'),
(50002,11,1111,'1000/1100/1110/1111','2024-08-20 00:00:00'),
(70001,20,1111,'1000/1100/1110/1111','2024-08-25 00:00:00'),
(70002,20,1111,'1000/1100/1110/1111','2024-08-25 00:00:00'),
(80001,30,1111,'1000/1100/1110/1111','2024-08-25 00:00:00');

/* ────────────────────────────────
   5. 班级-组织桥接表 (动态实体班级)
   ──────────────────────────────── */
CREATE TABLE xbsq_dwd_user_class_org_bridge (
    class_id        VARCHAR(40) PRIMARY KEY,
    org_id          BIGINT NOT NULL,
    academic_period VARCHAR(8)  NOT NULL COMMENT '如 2024S1 / 2024S2',
    active_status   TINYINT(1) DEFAULT 1,
    created_time    DATETIME DEFAULT CURRENT_TIMESTAMP,
    CONSTRAINT fk_cob_org FOREIGN KEY (org_id) REFERENCES xbsq_dwd_user_organization_dim(org_id)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4
COMMENT='班级-组织桥接表 (动态实体班级)';

-- 生成两个学期实体班级
INSERT INTO xbsq_dwd_user_class_org_bridge
(class_id,org_id,academic_period,active_status)
VALUES
('CLS_1111_2024S1',1111,'2024S1',1),   -- 2024 上学期 高一(1)班
('CLS_1111_2024S2',1111,'2024S2',1),   -- 2024 下学期 高一(1)班
('CLS_1112_2024S1',1112,'2024S1',1);   -- 2024 上学期 高一(2)班

/* 当 2024S2 开学分班：可将 CLS_1111_2024S1 active_status=0 ，新插一条 2024S2 */

UPDATE xbsq_dwd_user_class_org_bridge
SET active_status=0
WHERE class_id='CLS_1111_2024S1';

/* ────────────────────────────────
   6. 班级成员明细表（可记录多角色）
   ──────────────────────────────── */
CREATE TABLE xbsq_dwd_user_class_member_detail (
    class_id     VARCHAR(40) NOT NULL,
    user_id      BIGINT      NOT NULL,
    role_id      BIGINT      NOT NULL,
    subject_id   VARCHAR(30),
    join_datetime DATETIME DEFAULT CURRENT_TIMESTAMP COMMENT '加入班级时间',
    leave_datetime DATETIME COMMENT '离开班级时间',
    PRIMARY KEY (class_id, user_id, role_id),
    CONSTRAINT fk_cmd_class FOREIGN KEY (class_id)
        REFERENCES xbsq_dwd_user_class_org_bridge(class_id),
    CONSTRAINT fk_cmd_user  FOREIGN KEY (user_id)
        REFERENCES xbsq_dwd_user_personal_dim(user_id),
    CONSTRAINT fk_cmd_role  FOREIGN KEY (role_id)
        REFERENCES xbsq_dwd_user_role_def_dim(role_id)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4
COMMENT='班级成员明细表（支持多角色）';

-- 把成员放入 2024S1 的高一(1)班
INSERT INTO xbsq_dwd_user_class_member_detail
(class_id,user_id,role_id,subject_id,join_datetime)
VALUES
('CLS_1111_2024S1',50001,10,NULL,'2024-08-20'),   -- 李老师班主任
('CLS_1111_2024S1',50002,11,'MATH','2024-08-20'), -- 张老师任课
('CLS_1111_2024S1',70001,20,NULL,'2024-08-25'),   -- 小明学生
('CLS_1111_2024S1',70002,20,NULL,'2024-08-25'),   -- 小红学生
('CLS_1111_2024S1',80001,30,NULL,'2024-08-25');   -- 家长

/* ────────────────────────────────
   验证示例：查询 2024S1 高一(1)班成员
──────────────────────────────── */

-- 1) 找到实体班级 ID
SELECT class_id
FROM xbsq_dwd_user_class_org_bridge
WHERE org_id=1111 AND academic_period='2024S1';

-- 2) 取成员
SELECT m.class_id,u.user_id,u.name,r.role_name
FROM xbsq_dwd_user_class_member_detail  m
JOIN xbsq_dwd_user_personal_dim         u ON m.user_id=u.user_id
JOIN xbsq_dwd_user_role_def_dim         r ON m.role_id=r.role_id
WHERE m.class_id='CLS_1111_2024S1';