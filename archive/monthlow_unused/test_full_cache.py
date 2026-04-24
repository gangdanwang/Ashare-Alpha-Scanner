#-*- coding:utf-8 -*-
"""
完整的缓存工作流测试
"""
import sys
sys.path.insert(0, '.')

from Ashare import get_price
from stock_cache import get_cached_daily_data

code = 'sh600036'  # 招商银行

print("=" * 70)
print("完整缓存工作流测试")
print("=" * 70)

# 1. 第一次查询（应该从 API 获取并缓存）
print("\n【第一次查询】应该从 API 获取并缓存")
print("-" * 70)
df1 = get_price(code, frequency='1d', count=5)
print(f"获取到 {len(df1)} 条数据:")
print(df1)

# 2. 检查缓存
print("\n【检查缓存】")
print("-" * 70)
df_cache = get_cached_daily_data(code)
if not df_cache.empty:
    print(f"✅ 缓存中有 {len(df_cache)} 条数据")
    print(df_cache.tail())
else:
    print("❌ 缓存为空")

# 3. 第二次查询（应该优先使用缓存）
print("\n【第二次查询】应该优先使用缓存（速度更快）")
print("-" * 70)
df2 = get_price(code, frequency='1d', count=5)
print(f"获取到 {len(df2)} 条数据:")
print(df2)

# 4. 验证一致性
print("\n【验证】数据一致性")
print("-" * 70)
if df1.equals(df2):
    print("✅ 两次查询数据完全一致")
else:
    print("⚠️  数据不一致")
    print("第一次:")
    print(df1)
    print("第二次:")
    print(df2)

print("\n✅ 完整工作流测试完成！")
print("\n缓存工作流程：")
print("1. 首次查询 → API 获取 → 保存到数据库")
print("2. 后续查询 → 优先读取缓存 → 速度更快")
print("3. 增量更新 → API 新数据自动补充到数据库")
