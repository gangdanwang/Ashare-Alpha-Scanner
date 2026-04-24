#-*- coding:utf-8 -*-
"""
测试不同数据源的可用性
"""
import json
import requests

code = 'sh600036'

# 测试1：腾讯财经
print("=" * 60)
print("测试1：腾讯财经数据源")
print("=" * 60)
try:
    url = f'http://qt.gtimg.cn/q={code}'
    print(f"URL: {url}")
    response = requests.get(url, timeout=5)
    print(f"状态码: {response.status_code}")
    data = response.text
    if data:
        parts = data.split('~')
        if len(parts) > 3:
            print(f"股票名称: {parts[1]}")
            print(f"当前价格: {parts[3]}")
            print(f"✅ 腾讯财经数据源可用")
except Exception as e:
    print(f"❌ 腾讯财经失败: {e}")

# 测试2：新浪财经
print("\n" + "=" * 60)
print("测试2：新浪财经数据源")
print("=" * 60)
try:
    url = f'http://hq.sinajs.cn/list={code}'
    print(f"URL: {url}")
    headers = {'Referer': 'https://finance.sina.com.cn'}
    response = requests.get(url, headers=headers, timeout=5)
    print(f"状态码: {response.status_code}")
    data = response.text
    if data and '=' in data:
        parts = data.split(',')[0].split('=')[1].strip('"').split(',')
        if len(parts) > 3:
            print(f"股票名称: {parts[0]}")
            print(f"当前价格: {parts[3]}")
            print(f"✅ 新浪财经数据源可用")
except Exception as e:
    print(f"❌ 新浪财经失败: {e}")

# 测试3：东方财富（akshare使用的）
print("\n" + "=" * 60)
print("测试3：东方财富数据源")
print("=" * 60)
try:
    url = f'https://push2.eastmoney.com/api/qt/stock/get'
    params = {
        'secid': '1.600036',
        'fields': 'f43,f57,f58,f170',
        'fltt': 2,
        'invt': 2
    }
    print(f"URL: {url}")
    response = requests.get(url, params=params, timeout=10)
    print(f"状态码: {response.status_code}")
    data = response.json()
    if data.get('data'):
        print(f"当前价格: {data['data']['f43'] / 100}")
        print(f"✅ 东方财富数据源可用")
except Exception as e:
    print(f"❌ 东方财富失败: {e}")

print("\n" + "=" * 60)
print("总结：建议使用腾讯或新浪数据源（Ashare.py使用的）")
print("=" * 60)
