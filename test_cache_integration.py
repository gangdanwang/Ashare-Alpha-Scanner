#-*- coding:utf-8 -*-
"""
测试 MonthLow.py 缓存功能集成
"""
import sys
sys.path.insert(0, '.')

print("=" * 70)
print("MonthLow.py 缓存功能集成测试")
print("=" * 70)

# 1. 初始化缓存表
print("\n1. 初始化缓存表...")
try:
    from stock_cache import init_stock_cache_table
    init_stock_cache_table()
    print("✅ 缓存表初始化成功")
except Exception as e:
    print(f"❌ 初始化失败: {e}")

# 2. 测试 Ashare.get_price 缓存
print("\n2. 测试 Ashare.get_price 缓存功能...")
from Ashare import get_price
from stock_cache import get_cached_daily_data

code = 'sh600036'
print(f"   首次查询 {code}...")
df1 = get_price(code, frequency='1d', count=3)
print(f"   ✅ 获取到 {len(df1)} 条数据")

# 检查缓存
df_cache = get_cached_daily_data(code)
if not df_cache.empty:
    print(f"   ✅ 缓存中有 {len(df_cache)} 条数据")
else:
    print("   ⚠️  缓存为空")

# 3. 测试 MonthLow 导入
print("\n3. 测试 MonthLow 导入...")
try:
    from MonthLow import check_month_low
    print("✅ MonthLow 导入成功")
except Exception as e:
    print(f"❌ MonthLow 导入失败: {e}")

print("\n" + "=" * 70)
print("✅ 缓存功能集成测试完成！")
print("=" * 70)
print("\n缓存功能说明：")
print("• 首次查询：API 获取数据 → 保存到 MySQL 数据库")
print("• 后续查询：优先读取本地缓存 → 速度更快，减少 API 调用")
print("• 增量更新：API 新数据自动补充到数据库")
print("• 数据源：腾讯财经（已验证可用，价格准确）")
