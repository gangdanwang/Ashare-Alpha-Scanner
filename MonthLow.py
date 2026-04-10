#-*- coding:utf-8 -*-
"""
MonthLow.py - 近两月低点选股策略

功能：
1. 筛选沪深两个市场的主板股票（排除创业板、科创板、北交所）
2. 使用 Ashare.py 获取数据
3. 筛选规则：股票当前价格是近两个月日线价格的低点

筛选流程：
  第一步：按代码规则过滤（仅保留主板）
  第二步：过滤ST股票
  第三步：多线程筛选近两月低点股票
"""

import pandas as pd
from datetime import datetime, timedelta
from concurrent.futures import ThreadPoolExecutor, as_completed
import requests
from Ashare import get_price

# ============================================================
# 配置参数
# ============================================================
CONFIG = {
    # 股票池范围
    "stock_pool": {
        # 深证主板：000001 ~ 001999（原深主板）
        # 注意：002001~002999 是中小板（已合并到主板），300001+ 是创业板
        "sz_start": 1,
        "sz_end_exclusive": 3000,  # 只取 000xxx 和 001xxx 和 002xxx

        # 上证主板：600000 ~ 603999（688xxx 是科创板，排除）
        "sh_start": 600000,
        "sh_end_exclusive": 604000,
    },

    # 筛选规则
    "filter": {
        # 近 N 个交易日（约2个月，按交易日算）
        "lookback_days": 40,

        # 当前价格与近N日最低价的偏离阈值（%）
        "threshold_pct": 0.02,
    },

    # 性能配置
    "performance": {
        # 第一阶段：获取股票名称并发数
        "name_workers": 20,
        # 第二阶段：筛选低点并发数
        "screen_workers": 30,
        # 每批查询股票数量
        "batch_size": 100,
        # 请求间隔（秒）
        "sleep_between": 0.02,
    },
}


# ============================================================
# 1️⃣ 生成股票池（仅主板）
# ============================================================
def get_stock_list():
    """
    获取沪深两市主板股票代码列表。
    
    排除规则：
      - 深证：排除 300xxx（创业板）、301xxx
      - 上证：排除 688xxx（科创板）、689xxx
    """
    stocks = []

    cfg = CONFIG["stock_pool"]
    
    # 深证主板：000001 ~ 002999
    # 000xxx, 001xxx, 002xxx 都是主板（含原中小板）
    for i in range(cfg["sz_start"], min(cfg["sz_end_exclusive"], 3000)):
        stocks.append(f'sz{i:06d}')

    # 上证主板：600000 ~ 603999
    # 600xxx, 601xxx, 603xxx 是主板
    for i in range(cfg["sh_start"], min(cfg["sh_end_exclusive"], 604000)):
        stocks.append(f'sh{i}')

    return stocks


# ============================================================
# 2️⃣ 批量获取股票名称
# ============================================================
def get_stock_names(codes: list[str]) -> dict[str, dict]:
    """
    批量获取股票名称、价格、类型信息。
    返回 {code: {name: str, price: float, type: str}} 字典。
    
    type 字段说明（field[61]）：
      - GP-A: A股（普通股）
      - ZQ-KZZ: 可转债
      - ZQ: 债券
      - ZS: 指数
    """
    code_str = ','.join(codes)
    url = f'http://qt.gtimg.cn/q={code_str}'

    try:
        text = requests.get(url, timeout=5).text
        lines = text.split(';')
        result = {}

        for line in lines:
            if not line.strip():
                continue

            data = line.split('~')
            try:
                qcode = None
                head = line.split('=', 1)[0].strip()
                if head.startswith('v_'):
                    qcode = head[2:]

                # field[61] 是品种类型
                sec_type = data[61] if len(data) > 61 else ''

                if qcode and len(data) > 3:
                    result[qcode] = {
                        'name': data[1],
                        'price': float(data[3]) if data[3] else 0.0,
                        'type': sec_type,
                    }
            except:
                continue

        return result
    except:
        return {}


def filter_by_name(codes: list[str]) -> list[str]:
    """
    批量获取股票名称、价格、类型，过滤以下品种：
      - ST、*ST（名称含 ST）
      - 退市（名称含"退"）
      - 债券/可转债/指数（类型不是 GP-A）
      - 低价/问题股（价格 <= 1 元）
      - 无成交（价格为 0）
    返回符合条件的股票代码列表。
    """
    print("🔍 正在过滤 ST/退市/债券/低价股...")

    perf = CONFIG["performance"]
    batch_size = int(perf["batch_size"])
    batches = [codes[i:i + batch_size] for i in range(0, len(codes), batch_size)]

    all_info = {}
    with ThreadPoolExecutor(max_workers=int(perf["name_workers"])) as executor:
        futures = [executor.submit(get_stock_names, batch) for batch in batches]
        for future in as_completed(futures):
            try:
                all_info.update(future.result())
            except:
                pass

    # 过滤
    filtered = []
    excluded_st = 0       # ST股
    excluded_delist = 0   # 退市
    excluded_bond = 0     # 债券/可转债/指数
    excluded_low = 0      # 低价/问题

    for code in codes:
        info = all_info.get(code, {})
        name = info.get('name', '')
        price = info.get('price', 0.0)
        sec_type = info.get('type', '')

        # ST股票：名称包含 ST
        if 'ST' in name.upper():
            excluded_st += 1
            continue

        # 退市股：名称含"退"或"PT"
        if '退' in name or name.upper().startswith('PT'):
            excluded_delist += 1
            continue

        # 债券/可转债/指数：类型必须是 GP-A（A股）
        if sec_type != 'GP-A':
            excluded_bond += 1
            continue

        # 低价/问题股：价格 <= 1 元 或 价格为 0
        if price <= 1.0:
            excluded_low += 1
            continue

        filtered.append(code)

    total_excluded = excluded_st + excluded_delist + excluded_bond + excluded_low
    print(
        f"✅ 过滤完成：排除 {total_excluded} 只 "
        f"（ST:{excluded_st} 退市:{excluded_delist} 债券/指数:{excluded_bond} 低价:{excluded_low}），"
        f"剩余 {len(filtered)} 只"
    )
    return filtered


# ============================================================
# 3️⃣ 单只股票筛选逻辑
# ============================================================
def check_month_low(code: str) -> dict | None:
    """
    检查股票当前价格是否为近两个月日线价格的低点。
    """
    try:
        cfg = CONFIG["filter"]
        lookback = int(cfg["lookback_days"])
        threshold = float(cfg["threshold_pct"])

        # 获取近N日日线数据
        df = get_price(code, frequency='1d', count=lookback + 5)
        if df is None or df.empty or len(df) < lookback:
            return None

        # 取近N日数据
        df = df.tail(lookback).copy()

        # 当前价格（最新收盘价）
        current_price = df['close'].iloc[-1]

        # 近N日最低价
        period_low = df['low'].min()

        # 判断当前价格是否接近近N日最低价
        if current_price <= period_low * (1 + threshold):
            deviation = (current_price / period_low - 1) * 100

            return {
                'code': code,
                'current_price': round(current_price, 2),
                'period_low': round(period_low, 2),
                'deviation_pct': round(deviation, 2),
                'lookback_days': lookback,
            }

        return None

    except Exception:
        return None


# ============================================================
# 4️⃣ 批量筛选主函数
# ============================================================
def pick_month_low_stocks():
    """
    主函数：批量筛选近两月低点股票
    
    流程：
      1. 获取主板股票池
      2. 过滤 ST 股票
      3. 多线程筛选近两月低点
    """
    print("=" * 60)
    print("🔍 MonthLow - 近两月低点选股策略")
    print("=" * 60)

    # ── 第一步：获取主板股票池 ──
    stocks = get_stock_list()
    print(f"\n📊 第一步：主板股票池共 {len(stocks)} 只")

    # ── 第二步：过滤 ST 股票 ──
    stocks = filter_by_name(stocks)
    if not stocks:
        print("❌ 无符合条件的股票")
        return pd.DataFrame()

    # ── 第三步：多线程筛选近两月低点 ──
    print(f"\n🔍 第三步：开始筛选近两月低点股票...")

    perf = CONFIG["performance"]
    batch_size = int(perf["batch_size"])
    batches = [stocks[i:i + batch_size] for i in range(0, len(stocks), batch_size)]

    results = []
    total = len(stocks)
    start_time = datetime.now()

    def process_batch(batch):
        batch_results = []
        for code in batch:
            result = check_month_low(code)
            if result:
                batch_results.append(result)
        return batch_results

    completed = 0
    with ThreadPoolExecutor(max_workers=int(perf["screen_workers"])) as executor:
        futures = [executor.submit(process_batch, batch) for batch in batches]
        for future in as_completed(futures):
            batch_results = future.result()
            results.extend(batch_results)
            completed += batch_size
            elapsed = (datetime.now() - start_time).total_seconds()
            avg_speed = completed / elapsed if elapsed > 0 else 0
            remain = max(total - completed, 0) / avg_speed if avg_speed > 0 else 0
            pct = completed / total * 100

            print(
                f"\r⏳ 进度: {completed}/{total} ({pct:.1f}%) | "
                f"已通过: {len(results)} | "
                f"速度: {avg_speed:.0f}只/秒 | "
                f"剩余: {remain:.0f}秒",
                end="",
                flush=True
            )

    print()
    print(f"\n✅ 筛选完成，共找到 {len(results)} 只近两月低点股票\n")

    # ── 输出结果 ──
    if results:
        df_result = pd.DataFrame(results)
        df_result = df_result.sort_values('deviation_pct', ascending=True).reset_index(drop=True)
        df_result.index += 1

        df_display = pd.DataFrame({
            '代码': df_result['code'].str[2:],
            '当前价格': df_result['current_price'],
            '近2月最低价': df_result['period_low'],
            '偏离度(%)': df_result['deviation_pct'],
        })

        with pd.option_context(
            "display.max_rows", 100,
            "display.max_columns", None,
            "display.width", 120,
            "display.unicode.east_asian_width", True,
        ):
            print(df_display)

        print(f"\n📄 结果已排序（偏离度从小到大）")
        print(f"📅 筛选时间: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")

    return df_result if results else pd.DataFrame()


# ============================================================
# 5️⃣ 测试模式：指定股票代码
# ============================================================
def test_codes(codes: list[str]):
    """测试指定股票列表"""
    print("=" * 60)
    print("🧪 MonthLow - 测试模式")
    print("=" * 60)

    results = []
    for code in codes:
        print(f"\n🔍 检查 {code}...")
        result = check_month_low(code)
        if result:
            print(
                f"  ✅ 通过 | 当前价: {result['current_price']} | "
                f"近2月最低: {result['period_low']} | "
                f"偏离度: {result['deviation_pct']}%"
            )
            results.append(result)
        else:
            print("  ❌ 不满足条件")

    if results:
        print(f"\n✅ 共 {len(results)} 只股票满足条件")
    else:
        print("\n❌ 无股票满足条件")

    return results


# ============================================================
# 6️⃣ 执行入口
# ============================================================
if __name__ == '__main__':
    import argparse

    parser = argparse.ArgumentParser(description='MonthLow - 近两月低点选股')
    parser.add_argument('--code', action='append', default=[], help="测试指定代码，如 --code sz000001 --code sh600000")
    parser.add_argument('--codes', type=str, default=None, help='逗号分隔的测试代码')
    args = parser.parse_args()

    test_list = list(args.code)
    if args.codes:
        test_list.extend(args.codes.split(','))

    if test_list:
        test_codes([c.strip() for c in test_list if c.strip()])
    else:
        pick_month_low_stocks()
