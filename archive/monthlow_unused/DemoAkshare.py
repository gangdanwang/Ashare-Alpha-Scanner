#-*- coding:utf-8 -*-
"""
DemoTencent.py - 腾讯财经数据源示例

使用腾讯财经 API 获取股票行情数据
优点：
  1. 不需要代理，直接可用
  2. 数据准确（与同花顺基本一致）
  3. 支持批量查询
"""
import requests
import pandas as pd
import time

# ============================================================
# 腾讯财经数据源
# ============================================================
def get_stock_quote(code: str) -> dict:
    """
    获取单只股票实时行情
    
    参数:
      code: 股票代码，如 'sh600036' 或 'sz000001'
    
    返回:
      dict: 包含股票行情信息的字典
    """
    url = f'http://qt.gtimg.cn/q={code}'
    
    try:
        response = requests.get(url, timeout=5)
        response.encoding = 'gbk'  # 腾讯返回的是 GBK 编码
        text = response.text
        
        if not text or '~' not in text:
            return None
        
        # 解析数据（腾讯格式：v_sh600036="1~招商银行~600036~39.90~...")
        parts = text.split('~')
        
        if len(parts) < 50:
            return None
        
        return {
            '代码': parts[2],
            '名称': parts[1],
            '当前价格': float(parts[3]),
            '昨收': float(parts[4]),
            '今开': float(parts[5]),
            '最高': float(parts[33]),
            '最低': float(parts[34]),
            '涨跌额': float(parts[31]),
            '涨跌幅(%)': float(parts[32]),
            '成交量(手)': int(float(parts[6]) * 100),
            '成交额(万)': round(float(parts[37]) / 10000, 2),
            '涨停价': float(parts[48]),
            '跌停价': float(parts[49]),
        }
    except Exception as e:
        print(f"❌ 获取 {code} 失败: {e}")
        return None


def get_stocks_batch(codes: list[str]) -> pd.DataFrame:
    """
    批量获取股票行情
    
    参数:
      codes: 股票代码列表，如 ['sh600036', 'sz000001']
    
    返回:
      pd.DataFrame: 行情数据表格
    """
    # 腾讯支持一次查询多只股票（用逗号分隔）
    code_str = ','.join(codes)
    url = f'http://qt.gtimg.cn/q={code_str}'
    
    results = []
    
    try:
        response = requests.get(url, timeout=10)
        response.encoding = 'gbk'
        text = response.text
        
        # 每只股票返回的数据用 ';' 分隔
        lines = text.split(';')
        
        for line in lines:
            if not line.strip() or '~' not in line:
                continue
            
            parts = line.split('~')
            if len(parts) < 50:
                continue
            
            results.append({
                '代码': parts[2],
                '名称': parts[1],
                '当前价格': float(parts[3]),
                '昨收': float(parts[4]),
                '今开': float(parts[5]),
                '最高': float(parts[33]),
                '最低': float(parts[34]),
                '涨跌额': float(parts[31]),
                '涨跌幅(%)': float(parts[32]),
                '成交量(手)': int(float(parts[6]) * 100),
                '成交额(万)': round(float(parts[37]) / 10000, 2),
            })
        
        df = pd.DataFrame(results)
        return df
        
    except Exception as e:
        print(f"❌ 批量获取失败: {e}")
        return pd.DataFrame()


# ============================================================
# 测试示例
# ============================================================
if __name__ == '__main__':
    print("=" * 70)
    print("📊 腾讯财经数据源示例")
    print("=" * 70)
    
    # ── 示例1：单只股票查询 ──
    print("\n【示例1】获取 600036 招商银行实时行情")
    print("-" * 70)
    
    stock = get_stock_quote('sh600036')
    if stock:
        print(f"股票名称: {stock['名称']}")
        print(f"股票代码: {stock['代码']}")
        print(f"当前价格: {stock['当前价格']}")
        print(f"涨跌幅:   {stock['涨跌幅(%)']}%")
        print(f"今开:     {stock['今开']}")
        print(f"最高:     {stock['最高']}")
        print(f"最低:     {stock['最低']}")
        print(f"昨收:     {stock['昨收']}")
        print(f"成交额:   {stock['成交额(万)']} 万")
    else:
        print("❌ 获取失败")
    
    # ── 示例2：批量查询 ──
    print("\n" + "=" * 70)
    print("【示例2】批量获取多只股票行情")
    print("-" * 70)
    
    stock_codes = [
        'sh600036',  # 招商银行
        'sh600519',  # 贵州茅台
        'sz000001',  # 平安银行
        'sh601318',  # 中国平安
        'sz000858',  # 五粮液
    ]
    
    df = get_stocks_batch(stock_codes)
    if not df.empty:
        with pd.option_context(
            'display.max_rows', None,
            'display.max_columns', None,
            'display.width', 150,
            'display.unicode.east_asian_width', True,
        ):
            print(df)
        
        print(f"\n✅ 共获取 {len(df)} 只股票行情")
    else:
        print("❌ 批量获取失败")
    
    # ── 示例3：从文件读取股票代码 ──
    print("\n" + "=" * 70)
    print("【示例3】从文件读取股票列表并查询")
    print("-" * 70)
    
    try:
        with open('my_stocks.txt', 'r', encoding='utf-8') as f:
            file_codes = []
            for line in f:
                line = line.strip()
                if not line or line.startswith('#'):
                    continue
                
                # 自动补充市场前缀
                if line.startswith('sh') or line.startswith('sz'):
                    file_codes.append(line)
                elif line.startswith('6'):
                    file_codes.append(f'sh{line}')
                else:
                    file_codes.append(f'sz{line}')
            
            if file_codes:
                print(f"📄 从 my_stocks.txt 读取到 {len(file_codes)} 只股票")
                print(f"   {', '.join(file_codes[:10])}{'...' if len(file_codes) > 10 else ''}")
                
                df_file = get_stocks_batch(file_codes)
                if not df_file.empty:
                    with pd.option_context(
                        'display.max_rows', 20,
                        'display.max_columns', None,
                        'display.width', 150,
                        'display.unicode.east_asian_width', True,
                    ):
                        print(df_file)
                    
                    print(f"\n✅ 共获取 {len(df_file)} 只股票行情")
    except FileNotFoundError:
        print("⚠️  my_stocks.txt 文件不存在，跳过此示例")
