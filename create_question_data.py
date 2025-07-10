#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
学霸神器数仓 - 题目数据模拟脚本
"""

import pymysql
import random
import json
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
        raise

def create_question_table():
    """创建题目表"""
    print("正在创建题目表...")
    
    sql = """
    CREATE TABLE IF NOT EXISTS 题目库 (
        题目ID VARCHAR(50) PRIMARY KEY COMMENT '题目唯一标识',
        题目标题 VARCHAR(200) NOT NULL COMMENT '题目标题',
        题目内容 TEXT NOT NULL COMMENT '题目具体内容',
        题目类型 ENUM('选择题', '填空题', '解答题', '判断题') NOT NULL COMMENT '题目类型',
        学科 VARCHAR(50) NOT NULL COMMENT '学科',
        年级 VARCHAR(20) NOT NULL COMMENT '适用年级',
        知识点 VARCHAR(200) COMMENT '涉及知识点',
        难度级别 ENUM('简单', '中等', '困难', '很难') DEFAULT '中等' COMMENT '难度级别',
        答案 TEXT COMMENT '标准答案',
        解析 TEXT COMMENT '答案解析',
        选项A VARCHAR(500) COMMENT '选择题选项A',
        选项B VARCHAR(500) COMMENT '选择题选项B',
        选项C VARCHAR(500) COMMENT '选择题选项C',
        选项D VARCHAR(500) COMMENT '选择题选项D',
        正确选项 VARCHAR(10) COMMENT '正确选项（A/B/C/D）',
        使用次数 INT DEFAULT 0 COMMENT '被使用次数',
        正确率 DECIMAL(5,2) DEFAULT 0.00 COMMENT '答题正确率',
        创建者ID VARCHAR(50) NOT NULL COMMENT '创建者ID',
        创建时间 TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        更新时间 TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
        是否启用 BOOLEAN DEFAULT TRUE COMMENT '是否启用',
        INDEX idx_subject (学科),
        INDEX idx_grade (年级),
        INDEX idx_difficulty (难度级别),
        INDEX idx_creator (创建者ID),
        INDEX idx_knowledge (知识点)
    ) COMMENT='题目库表，存储所有题目信息'
    """
    
    execute_sql(sql)
    print("✓ 题目表创建成功")

def generate_math_questions():
    """生成数学题目"""
    questions = [
        # 高一数学题目
        {
            '题目ID': 'Q001', '题目标题': '函数单调性判断', '学科': '数学', '年级': '高一',
            '题目内容': '判断函数f(x) = 2x + 3在定义域内的单调性',
            '题目类型': '选择题', '知识点': '函数单调性',
            '选项A': '单调递增', '选项B': '单调递减', '选项C': '先增后减', '选项D': '先减后增',
            '正确选项': 'A', '答案': 'A', '解析': 'f\'(x) = 2 > 0，所以函数单调递增',
            '难度级别': '简单', '创建者ID': 'U006'
        },
        {
            '题目ID': 'Q002', '题目标题': '二次函数最值', '学科': '数学', '年级': '高一',
            '题目内容': '求函数f(x) = x² - 4x + 3在区间[0,3]上的最小值',
            '题目类型': '解答题', '知识点': '二次函数',
            '答案': '最小值为-1', '解析': '配方得f(x) = (x-2)² - 1，顶点为(2,-1)，在区间[0,3]内',
            '难度级别': '中等', '创建者ID': 'U007'
        },
        {
            '题目ID': 'Q003', '题目标题': '三角函数值计算', '学科': '数学', '年级': '高一',
            '题目内容': '计算sin(π/6) + cos(π/3)的值',
            '题目类型': '填空题', '知识点': '三角函数',
            '答案': '1', '解析': 'sin(π/6) = 1/2, cos(π/3) = 1/2, 所以结果为1',
            '难度级别': '简单', '创建者ID': 'U008'
        },
        # 高二数学题目
        {
            '题目ID': 'Q004', '题目标题': '导数应用', '学科': '数学', '年级': '高二',
            '题目内容': '已知f(x) = x³ - 3x² + 2，求f(x)的极值',
            '题目类型': '解答题', '知识点': '导数应用',
            '答案': '极大值f(0)=2，极小值f(2)=-2', '解析': 'f\'(x) = 3x² - 6x = 3x(x-2)，令f\'(x)=0得x=0或x=2',
            '难度级别': '中等', '创建者ID': 'U008'
        },
        {
            '题目ID': 'Q005', '题目标题': '数列求和', '学科': '数学', '年级': '高二',
            '题目内容': '数列{aₙ}的前n项和为Sₙ = 2n² + n，求a₅',
            '题目类型': '选择题', '知识点': '数列',
            '选项A': '19', '选项B': '20', '选项C': '21', '选项D': '22',
            '正确选项': 'A', '答案': 'A', '解析': 'a₅ = S₅ - S₄ = (2×25+5) - (2×16+4) = 55 - 36 = 19',
            '难度级别': '中等', '创建者ID': 'U006'
        }
    ]
    return questions

def generate_physics_questions():
    """生成物理题目"""
    questions = [
        {
            '题目ID': 'Q006', '题目标题': '牛顿第二定律', '学科': '物理', '年级': '高一',
            '题目内容': '质量为2kg的物体在10N的力作用下，求其加速度',
            '题目类型': '解答题', '知识点': '牛顿第二定律',
            '答案': '5m/s²', '解析': '根据F=ma，a = F/m = 10/2 = 5m/s²',
            '难度级别': '简单', '创建者ID': 'U010'
        },
        {
            '题目ID': 'Q007', '题目标题': '功和能', '学科': '物理', '年级': '高一',
            '题目内容': '物体从高度h自由下落，着地时速度为v，求h与v的关系',
            '题目类型': '选择题', '知识点': '机械能',
            '选项A': 'h = v²/2g', '选项B': 'h = v²/g', '选项C': 'h = 2v²/g', '选项D': 'h = v/2g',
            '正确选项': 'A', '答案': 'A', '解析': '由机械能守恒mgh = ½mv²，得h = v²/2g',
            '难度级别': '中等', '创建者ID': 'U010'
        }
    ]
    return questions

def generate_chinese_questions():
    """生成语文题目"""
    questions = [
        {
            '题目ID': 'Q008', '题目标题': '古诗文默写', '学科': '语文', '年级': '高一',
            '题目内容': '《静夜思》中"举头望明月"的下一句是什么？',
            '题目类型': '填空题', '知识点': '古诗文',
            '答案': '低头思故乡', '解析': '李白《静夜思》经典名句',
            '难度级别': '简单', '创建者ID': 'U006'
        },
        {
            '题目ID': 'Q009', '题目标题': '修辞手法', '学科': '语文', '年级': '高一',
            '题目内容': '"春风又绿江南岸"中使用了什么修辞手法？',
            '题目类型': '选择题', '知识点': '修辞手法',
            '选项A': '拟人', '选项B': '比喻', '选项C': '夸张', '选项D': '对偶',
            '正确选项': 'A', '答案': 'A', '解析': '"绿"字用作动词，赋予春风人的动作，是拟人手法',
            '难度级别': '中等', '创建者ID': 'U009'
        }
    ]
    return questions

def generate_english_questions():
    """生成英语题目"""
    questions = [
        {
            '题目ID': 'Q010', '题目标题': '语法选择', '学科': '英语', '年级': '高一',
            '题目内容': 'I _____ to the library yesterday.',
            '题目类型': '选择题', '知识点': '时态',
            '选项A': 'go', '选项B': 'goes', '选项C': 'went', '选项D': 'going',
            '正确选项': 'C', '答案': 'C', '解析': 'yesterday表示过去时间，应用过去式went',
            '难度级别': '简单', '创建者ID': 'U009'
        }
    ]
    return questions

def insert_questions():
    """插入题目数据"""
    print("正在插入题目数据...")
    
    # 收集所有题目
    all_questions = []
    all_questions.extend(generate_math_questions())
    all_questions.extend(generate_physics_questions())
    all_questions.extend(generate_chinese_questions())
    all_questions.extend(generate_english_questions())
    
    # 为每个题目添加随机使用次数和正确率
    for question in all_questions:
        question['使用次数'] = random.randint(0, 100)
        question['正确率'] = round(random.uniform(0.3, 0.9), 2)
    
    # 插入数据
    sql = """
    INSERT INTO 题目库 (
        题目ID, 题目标题, 题目内容, 题目类型, 学科, 年级, 知识点, 难度级别,
        答案, 解析, 选项A, 选项B, 选项C, 选项D, 正确选项, 使用次数, 正确率, 创建者ID
    ) VALUES (
        %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s
    )
    """
    
    for question in all_questions:
        params = (
            question['题目ID'], question['题目标题'], question['题目内容'], question['题目类型'],
            question['学科'], question['年级'], question['知识点'], question['难度级别'],
            question['答案'], question['解析'], 
            question.get('选项A'), question.get('选项B'), question.get('选项C'), question.get('选项D'),
            question.get('正确选项'), question['使用次数'], question['正确率'], question['创建者ID']
        )
        execute_sql(sql, params)
    
    print(f"✓ 插入了 {len(all_questions)} 道题目")

def create_question_acl():
    """为题目创建对应的ACL权限"""
    print("正在创建题目ACL权限...")
    
    # 获取所有题目
    questions = execute_sql("SELECT 题目ID, 创建者ID, 学科, 年级 FROM 题目库")
    
    acl_data = []
    acl_id = 1
    
    for question in questions:
        题目ID, 创建者ID, 学科, 年级 = question
        
        # 创建者拥有所有权
        acl_data.append((
            f'ACL{acl_id:03d}', 题目ID, '题目', 'user', 创建者ID, '所有权',
            None, True, '默认继承', '无需申请', None, None, 创建者ID
        ))
        acl_id += 1
        
        # 根据年级设置组织权限
        if 年级 == '高一':
            # 高一题目，高一年级可查看
            acl_data.append((
                f'ACL{acl_id:03d}', 题目ID, '题目', 'org', '1110', '查看',
                '1000/1100/1110', False, '默认继承', '无需申请', None, None, 创建者ID
            ))
            acl_id += 1
        elif 年级 == '高二':
            # 高二题目，高二年级可查看
            acl_data.append((
                f'ACL{acl_id:03d}', 题目ID, '题目', 'org', '1120', '查看',
                '1000/1100/1120', False, '默认继承', '无需申请', None, None, 创建者ID
            ))
            acl_id += 1
        
        # 高中部都可以查看（学段共享）
        acl_data.append((
            f'ACL{acl_id:03d}', 题目ID, '题目', 'org', '1100', '查看',
            '1000/1100', False, '默认继承', '无需申请', None, None, 创建者ID
        ))
        acl_id += 1
    
    # 插入ACL数据
    sql = """
    INSERT IGNORE INTO 题目操作ACL表 (
        ACL_ID, 资源ID, 资源类型, 授权对象类型, 授权对象ID, 权限类型, 权限范围,
        是否可编辑原资源, 权限来源, 申请状态, 申请者ID, 审批者ID, 创建者ID
    ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
    """
    
    for data in acl_data:
        execute_sql(sql, data)
    
    print(f"✓ 创建了 {len(acl_data)} 条ACL权限记录")

def generate_more_questions():
    """生成更多题目数据达到几十道"""
    print("正在生成更多题目...")
    
    subjects = ['数学', '物理', '语文', '英语', '化学', '生物']
    grades = ['高一', '高二', '高三']
    types = ['选择题', '填空题', '解答题', '判断题']
    difficulties = ['简单', '中等', '困难']
    creators = ['U006', 'U007', 'U008', 'U009', 'U010']
    
    additional_questions = []
    
    for i in range(11, 51):  # Q011到Q050，共40道题
        题目ID = f'Q{i:03d}'
        学科 = random.choice(subjects)
        年级 = random.choice(grades)
        题目类型 = random.choice(types)
        难度级别 = random.choice(difficulties)
        创建者ID = random.choice(creators)
        
        if 学科 == '数学':
            知识点 = random.choice(['函数', '导数', '数列', '几何', '三角函数'])
            题目标题 = f'{知识点}相关问题{i}'
        elif 学科 == '物理':
            知识点 = random.choice(['力学', '电学', '光学', '热学'])
            题目标题 = f'{知识点}计算题{i}'
        elif 学科 == '语文':
            知识点 = random.choice(['古诗文', '现代文', '作文', '语法'])
            题目标题 = f'{知识点}练习{i}'
        elif 学科 == '英语':
            知识点 = random.choice(['语法', '词汇', '阅读', '写作'])
            题目标题 = f'{知识点}训练{i}'
        elif 学科 == '化学':
            知识点 = random.choice(['有机化学', '无机化学', '化学反应', '化学计算'])
            题目标题 = f'{知识点}题目{i}'
        else:  # 生物
            知识点 = random.choice(['细胞', '遗传', '生态', '进化'])
            题目标题 = f'{知识点}相关{i}'
        
        题目内容 = f'这是一道关于{知识点}的{题目类型}，适用于{年级}学生。'
        
        question = {
            '题目ID': 题目ID,
            '题目标题': 题目标题,
            '题目内容': 题目内容,
            '题目类型': 题目类型,
            '学科': 学科,
            '年级': 年级,
            '知识点': 知识点,
            '难度级别': 难度级别,
            '创建者ID': 创建者ID,
            '使用次数': random.randint(0, 200),
            '正确率': round(random.uniform(0.2, 0.95), 2)
        }
        
        if 题目类型 == '选择题':
            question.update({
                '选项A': f'选项A-{i}',
                '选项B': f'选项B-{i}',
                '选项C': f'选项C-{i}',
                '选项D': f'选项D-{i}',
                '正确选项': random.choice(['A', 'B', 'C', 'D']),
                '答案': random.choice(['A', 'B', 'C', 'D']),
                '解析': f'这是第{i}题的解析...'
            })
        else:
            question.update({
                '答案': f'第{i}题的答案',
                '解析': f'第{i}题的详细解析...'
            })
        
        additional_questions.append(question)
    
    # 插入数据
    sql = """
    INSERT IGNORE INTO 题目库 (
        题目ID, 题目标题, 题目内容, 题目类型, 学科, 年级, 知识点, 难度级别,
        答案, 解析, 选项A, 选项B, 选项C, 选项D, 正确选项, 使用次数, 正确率, 创建者ID
    ) VALUES (
        %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s
    )
    """
    
    for question in additional_questions:
        params = (
            question['题目ID'], question['题目标题'], question['题目内容'], question['题目类型'],
            question['学科'], question['年级'], question['知识点'], question['难度级别'],
            question['答案'], question['解析'], 
            question.get('选项A'), question.get('选项B'), question.get('选项C'), question.get('选项D'),
            question.get('正确选项'), question['使用次数'], question['正确率'], question['创建者ID']
        )
        execute_sql(sql, params)
    
    print(f"✓ 额外生成了 {len(additional_questions)} 道题目")

def validate_questions():
    """验证题目数据"""
    print("正在验证题目数据...")
    
    queries = [
        ("总题目数", "SELECT COUNT(*) FROM 题目库"),
        ("数学题目数", "SELECT COUNT(*) FROM 题目库 WHERE 学科='数学'"),
        ("物理题目数", "SELECT COUNT(*) FROM 题目库 WHERE 学科='物理'"),
        ("语文题目数", "SELECT COUNT(*) FROM 题目库 WHERE 学科='语文'"),
        ("英语题目数", "SELECT COUNT(*) FROM 题目库 WHERE 学科='英语'"),
        ("高一题目数", "SELECT COUNT(*) FROM 题目库 WHERE 年级='高一'"),
        ("高二题目数", "SELECT COUNT(*) FROM 题目库 WHERE 年级='高二'"),
        ("选择题数量", "SELECT COUNT(*) FROM 题目库 WHERE 题目类型='选择题'"),
        ("ACL权限数", "SELECT COUNT(*) FROM 题目操作ACL表 WHERE 资源类型='题目'"),
    ]
    
    for desc, sql in queries:
        result = execute_sql(sql)
        count = result[0][0] if result else 0
        print(f"  ✓ {desc}: {count}")

def main():
    """主函数"""
    print("=" * 60)
    print("📚 题目库数据生成脚本")
    print("=" * 60)
    print(f"开始时间: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print()
    
    try:
        create_question_table()
        insert_questions()
        generate_more_questions()
        create_question_acl()
        
        print()
        validate_questions()
        
        print()
        print("🎉 题目库数据生成完成！")
        print("=" * 60)
        
    except Exception as e:
        print(f"❌ 数据生成失败: {str(e)}")
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    main() 