#-*- coding:utf-8 -*-
"""
数据库迁移：为 t_stock_daily 表添加 name 字段
"""
import sys
sys.path.insert(0, '.')

from db import get_conn

print("正在执行数据库迁移...")

try:
    with get_conn() as conn:
        with conn.cursor() as cur:
            # 检查 name 字段是否已存在
            cur.execute("""
                SELECT COUNT(*) as cnt 
                FROM information_schema.COLUMNS 
                WHERE TABLE_SCHEMA = 'alpha_scanner' 
                AND TABLE_NAME = 't_stock_daily' 
                AND COLUMN_NAME = 'name'
            """)
            result = cur.fetchone()
            
            if result['cnt'] == 0:
                # 添加 name 字段
                cur.execute("""
                    ALTER TABLE t_stock_daily 
                    ADD COLUMN name VARCHAR(20) COMMENT '股票名称' AFTER code
                """)
                print("✅ 成功添加 name 字段")
            else:
                print("ℹ️  name 字段已存在，跳过")
            
            # 修改 code 字段注释
            cur.execute("""
                ALTER TABLE t_stock_daily 
                MODIFY COLUMN code VARCHAR(12) NOT NULL COMMENT '股票代码，如 600036'
            """)
            print("✅ 更新 code 字段注释")

    print("✅ 数据库迁移完成")
    
    # 验证
    with get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute("DESC t_stock_daily")
            columns = cur.fetchall()
            print("\n当前表结构：")
            for col in columns:
                print(f"  {col['Field']:15} {col['Type']:20} {col['Comment']}")
                
except Exception as e:
    print(f"❌ 迁移失败: {e}")
    import traceback
    traceback.print_exc()
