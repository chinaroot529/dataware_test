#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
数据同步监控脚本
监控MySQL到Doris的数据同步状态
"""

import pymysql
import json
from datetime import datetime

def check_sync_status():
    """检查同步状态"""
    # MySQL配置
    mysql_config = {
        'host': '10.10.0.117',
        'port': 6033,
        'user': 'root',
        'password': 'Xml123&45!',
        'database': 'data_ware_test',
        'charset': 'utf8mb4'
    }
    
    # Doris配置
    doris_config = {
        'host': '192.168.99.6',
        'port': 19030,
        'user': 'root',
        'password': '',
        'database': 'ods',
        'charset': 'utf8mb4'
    }
    
    try:
        # 获取MySQL数据统计
        mysql_conn = pymysql.connect(**mysql_config)
        mysql_cursor = mysql_conn.cursor(pymysql.cursors.DictCursor)
        
        mysql_cursor.execute("SELECT COUNT(*) as count, MAX(snapshot_date) as max_date FROM abc_warning")
        mysql_stats = mysql_cursor.fetchone()
        
        # 获取Doris数据统计
        doris_conn = pymysql.connect(**doris_config)
        doris_cursor = doris_conn.cursor(pymysql.cursors.DictCursor)
        doris_cursor.execute("USE ods")
        
        doris_cursor.execute("SELECT COUNT(*) as count, MAX(snapshot_date) as max_date FROM abc_warning")
        doris_stats = doris_cursor.fetchone()
        
        # 生成报告
        report = {
            'timestamp': datetime.now().isoformat(),
            'mysql': {
                'count': mysql_stats['count'],
                'latest_date': str(mysql_stats['max_date'])
            },
            'doris': {
                'count': doris_stats['count'],
                'latest_date': str(doris_stats['max_date'])
            },
            'sync_status': {
                'count_match': mysql_stats['count'] == doris_stats['count'],
                'date_match': mysql_stats['max_date'] == doris_stats['max_date'],
                'lag_days': (mysql_stats['max_date'] - doris_stats['max_date']).days if mysql_stats['max_date'] and doris_stats['max_date'] else 0
            }
        }
        
        print(json.dumps(report, indent=2, ensure_ascii=False))
        
        mysql_conn.close()
        doris_conn.close()
        
        return report['sync_status']['count_match'] and report['sync_status']['date_match']
        
    except Exception as e:
        print(f"监控检查失败: {e}")
        return False

if __name__ == "__main__":
    check_sync_status()
