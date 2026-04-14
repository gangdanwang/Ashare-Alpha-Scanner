#-*- coding:utf-8 -*-
"""
MonthLow.py - 近两月低点选股策略（含T日最低价筛选）

功能：
1. 筛选沪深两个市场的主板股票（排除创业板、科创板、北交所）
2. 使用 Ashare.py 获取数据
3. 筛选规则：T-1日股价创近N日新低的股票（N可配置）
4. 筛选T日最低价 > T-1日最低价的股票（含十字星）

筛选流程：
  第一阶段：按代码规则过滤（仅保留主板）- 带进度展示
  第二阶段：过滤ST/退市/债券/低价股 - 带进度展示（多线程）
  第三阶段：筛选T-1日创近N日新低的股票（N可配置）- 带实时进度条
  第四阶段：筛选T日最低价 > T-1日最低价 - 带进度展示

输出字段：
  代码、名称、当前价格、T日最低价、T-1日最低价、当前价格vs T-1日最低价(%)、T日最低价vs T-1日最低价(%)

配置参数：
  CONFIG["filter"]["lookback_days"] = 40  # 可调整为5日、10日、20日等

字段说明：
  当前价格vs T-1日最低价(%) = (当前价格 - T-1日最低价) / T-1日最低价 × 100%
  T日最低价vs T-1日最低价(%) = (T日最低价 - T-1日最低价) / T-1日最低价 × 100%
  正值表示高于T-1日最低价，负值表示低于T-1日最低价

使用方式：
  1. 全市场筛选：python MonthLow.py
  2. 测试指定股票：python MonthLow.py --code sz000001 --code sh600000
  3. 从文件筛选：python MonthLow.py --file stocks.txt

文件格式示例（stocks.txt）：
  sz000001
  sh600000
  000002（自动识别深市）
  600036（自动识别沪市）

版本历史：
  v1.0: 初始版本，三阶段筛选
  v2.0: 增加第四阶段（T日价格 > T-1日低点）
  v3.0: 增加各阶段进度展示，第四阶段增加收阳线筛选条件
  v4.0: 调整第四阶段为T日最低价 > T-1日最低价，输出字段优化
  v4.1: 第三阶段改为当前价格在近40日最低价±2%范围内
  v4.2: 输出表格增加偏离度(%)字段
  v4.3: 第三阶段改为筛选T-1日创近40日新低的股票
  v4.4: 偏离度字段移除，近N日新低改为可配置参数
  v4.5: 移除近N日最低价字段，增加当前价格vs T-1日最低价(%)字段
  v4.6: 增加T日最低价vs T-1日最低价(%)字段
  v4.7: 新增从文件读取股票代码列表并进行筛选的功能
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
        # 近 N 个交易日创出新低（可配置：5日、10日、20日、40日等）
        "lookback_days": 10,
    },

    # 性能配置
    "performance": {
        # 第一阶段：生成股票池批处理大小
        "phase1_batch_size": 500,
        # 第二阶段：获取股票名称并发数
        "name_workers": 20,
        # 第三阶段：筛选低点并发数
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
    
    返回：
      list[str]: 主板股票代码列表，格式为 'sz000001' 或 'sh600000'
    """
    print("🔍 正在生成主板股票池...")
    
    stocks = []
    cfg = CONFIG["stock_pool"]
    
    # 深证主板：000001 ~ 002999
    # 000xxx, 001xxx, 002xxx 都是主板（含原中小板）
    sz_count = 0
    sz_total = min(cfg["sz_end_exclusive"], 3000) - cfg["sz_start"]
    for i in range(cfg["sz_start"], min(cfg["sz_end_exclusive"], 3000)):
        stocks.append(f'sz{i:06d}')
        sz_count += 1
        if sz_count % 500 == 0 or sz_count == sz_total:
            pct = sz_count / sz_total * 100
            print(f"\r  📊 深证主板进度: {sz_count}/{sz_total} ({pct:.1f}%)", end="", flush=True)
    
    print()  # 换行
    
    # 上证主板：600000 ~ 603999
    # 600xxx, 601xxx, 603xxx 是主板
    sh_count = 0
    sh_total = min(cfg["sh_end_exclusive"], 604000) - cfg["sh_start"]
    for i in range(cfg["sh_start"], min(cfg["sh_end_exclusive"], 604000)):
        stocks.append(f'sh{i}')
        sh_count += 1
        if sh_count % 500 == 0 or sh_count == sh_total:
            pct = sh_count / sh_total * 100
            print(f"\r  📊 上证主板进度: {sh_count}/{sh_total} ({pct:.1f}%)", end="", flush=True)
    
    print()  # 换行
    print(f"✅ 股票池生成完成：深证主板 {sz_count} 只，上证主板 {sh_count} 只")
    
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
    
    参数：
      codes: 股票代码列表
    
    返回：
      list[str]: 符合条件的股票代码列表
    """
    print(f"🔍 开始过滤 {len(codes)} 只股票...")

    perf = CONFIG["performance"]
    batch_size = int(perf["batch_size"])
    batches = [codes[i:i + batch_size] for i in range(0, len(codes), batch_size)]

    all_info = {}
    completed = 0
    total = len(batches)
    
    # 多线程获取股票信息，带进度显示
    with ThreadPoolExecutor(max_workers=int(perf["name_workers"])) as executor:
        futures = [executor.submit(get_stock_names, batch) for batch in batches]
        for future in as_completed(futures):
            try:
                all_info.update(future.result())
                completed += 1
                pct = completed / total * 100
                print(
                    f"\r  📊 获取股票信息进度: {completed}/{total} ({pct:.1f}%) - "
                    f"已获取 {len(all_info)} 只股票",
                    end="",
                    flush=True
                )
            except:
                pass
    
    print()  # 换行
    print(f"✅ 股票信息获取完成，共获取 {len(all_info)} 只股票")
    print(f"🔍 开始过滤 ST/退市/债券/低价股...")

    # 过滤
    filtered = []
    excluded_st = 0       # ST股
    excluded_delist = 0   # 退市
    excluded_bond = 0     # 债券/可转债/指数
    excluded_low = 0      # 低价/问题
    
    total_codes = len(codes)
    processed = 0

    for code in codes:
        info = all_info.get(code, {})
        name = info.get('name', '')
        price = info.get('price', 0.0)
        sec_type = info.get('type', '')

        # ST股票：名称包含 ST
        if 'ST' in name.upper():
            excluded_st += 1
            processed += 1
            continue

        # 退市股：名称含"退"或"PT"
        if '退' in name or name.upper().startswith('PT'):
            excluded_delist += 1
            processed += 1
            continue

        # 债券/可转债/指数：类型必须是 GP-A（A股）
        if sec_type != 'GP-A':
            excluded_bond += 1
            processed += 1
            continue

        # 低价/问题股：价格 <= 1 元 或 价格为 0
        if price <= 1.0:
            excluded_low += 1
            processed += 1
            continue

        filtered.append(code)
        processed += 1
        
        # 每处理 500 只股票显示一次进度
        if processed % 500 == 0 or processed == total_codes:
            pct = processed / total_codes * 100
            print(
                f"\r  📊 过滤进度: {processed}/{total_codes} ({pct:.1f}%) - "
                f"已保留 {len(filtered)} 只",
                end="",
                flush=True
            )

    print()  # 换行
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
    检查T-1日股价是否创近N日新低。
    返回包含T日和T-1日价格信息。

    筛选条件：
      T-1日最低价 == 近N日最低价（即T-1日创了近N日新低）

    参数：
      code: 股票代码，格式为 'sz000001' 或 'sh600000'

    返回：
      dict: 包含股票信息的字典，如果不满足条件则返回 None
        - code: 股票代码
        - name: 股票名称
        - current_price: T日当前价格（最新收盘价）
        - t_low: T日最低价
        - period_low: 近N日最低价（含T-1日，不含T日）
        - lookback_days: 回看天数
        - t_1_price: T-1日收盘价
        - t_1_low: T-1日最低价
    """
    try:
        cfg = CONFIG["filter"]
        lookback = int(cfg["lookback_days"])

        # 获取近N日日线数据（多取几天用于获取T-1日数据和容错）
        df = get_price(code, frequency='1d', count=lookback + 10)
        if df is None or df.empty or len(df) < lookback + 1:
            return None

        # 取近N日数据（用于计算阶段低点，包含T-1日，不含T日）
        df_period = df.tail(lookback).copy()

        # 获取T日（最新）数据
        t_day = df.iloc[-1]
        current_price = t_day['close']
        t_low = t_day['low']  # T日最低价

        # 获取T-1日数据
        t_1_day = df.iloc[-2]
        t_1_price = t_1_day['close']
        t_1_low = t_1_day['low']

        # 近N日最低价（包含T-1日，不含T日）
        period_low = df_period['low'].min()

        # 筛选条件：T-1日最低价 == 近N日最低价（即T-1日创了近N日新低）
        # 允许有极小的误差（0.01元），因为可能存在四舍五入
        if abs(t_1_low - period_low) <= 0.01:
            return {
                'code': code,
                'name': '',  # 名称在第四阶段批量获取
                'current_price': round(current_price, 2),
                't_low': round(t_low, 2),
                'period_low': round(period_low, 2),
                'lookback_days': lookback,
                # T-1日数据
                't_1_price': round(t_1_price, 2),
                't_1_low': round(t_1_low, 2),
            }

        return None

    except Exception:
        return None


# ============================================================
# 4️⃣ 第四阶段筛选：T日最低价 > T-1日最低价
# ============================================================
def get_stock_names_batch(codes: list[str]) -> dict[str, str]:
    """
    批量获取股票名称
    
    参数：
      codes: 股票代码列表
    
    返回：
      dict: {code: name} 字典
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
                head = line.split('=', 1)[0].strip()
                qcode = head[2:] if head.startswith('v_') else None
                
                if qcode and len(data) > 1:
                    result[qcode] = data[1]
            except:
                continue
        
        return result
    except:
        return {}


def filter_t_low_above_t_1_low(results: list[dict]) -> list[dict]:
    """
    第四阶段筛选：T日最低价 > T-1日最低价（含十字星）
    
    筛选条件：
      t_low > t_1_low （T日最低价高于T-1日最低价）
    
    同时批量获取股票名称用于显示
    
    参数：
      results: 第三阶段筛选结果列表
    
    返回：
      list[dict]: 符合第四阶段条件的股票列表
    """
    print("\n" + "=" * 60)
    print("🔍 第四阶段：筛选T日最低价 > T-1日最低价的股票")
    print("=" * 60)
    print(f"🔍 开始筛选 {len(results)} 只股票...")
    print(f"📋 筛选条件：T日最低价 > T-1日最低价（含十字星）")
    print()
    
    # 批量获取股票名称
    print("📝 正在获取股票名称...")
    codes = [stock['code'] for stock in results]
    names_map = get_stock_names_batch(codes)
    
    # 为每只股票填充名称
    for stock in results:
        stock['name'] = names_map.get(stock['code'], '')
    
    print(f"✅ 股票名称获取完成")
    print()
    
    # 筛选 T日最低价 > T-1日最低价
    filtered = []
    excluded = 0
    total = len(results)
    processed = 0
    
    for stock in results:
        t_low = stock.get('t_low', stock['current_price'])  # 如果没有t_low，用current_price代替
        t_1_low = stock['t_1_low']
        
        # 条件：T日最低价 > T-1日最低价
        if t_low > t_1_low:
            filtered.append(stock)
        else:
            excluded += 1
        
        processed += 1
        
        # 每处理 100 只股票显示一次进度
        if processed % 100 == 0 or processed == total:
            pct = processed / total * 100
            print(
                f"\r  📊 筛选进度: {processed}/{total} ({pct:.1f}%) - "
                f"已保留 {len(filtered)} 只",
                end="",
                flush=True
            )
    
    print()  # 换行
    print(f"✅ 第四阶段完成：")
    print(f"   - 排除 {excluded} 只（T日最低价 <= T-1日最低价）")
    print(f"   - 剩余 {len(filtered)} 只")
    print("=" * 60)
    
    return filtered


# ============================================================
# 5️⃣ 批量筛选主函数
# ============================================================
def pick_month_low_stocks():
    """
    主函数：批量筛选近N日低点股票

    筛选流程：
      第一阶段：生成主板股票池（沪深两市主板，排除创业板、科创板）
      第二阶段：过滤 ST/退市/债券/低价股（多线程获取股票信息）
      第三阶段：筛选T-1日创近N日新低的股票（N可配置）
      第四阶段：筛选T日最低价 > T-1日最低价的股票（含十字星）

    返回：
      pd.DataFrame: 符合条件的股票列表，包含详细价格信息
    """
    print("=" * 60)
    print("🔍 MonthLow - 近两月低点选股策略")
    print("=" * 60)
    print(f"⏰ 开始时间: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")

    # ── 第一步：获取主板股票池 ──
    print("\n" + "=" * 60)
    print("📊 第一阶段：生成主板股票池")
    print("=" * 60)
    start_time = datetime.now()
    stocks = get_stock_list()
    elapsed = (datetime.now() - start_time).total_seconds()
    print(f"✅ 第一阶段完成：共 {len(stocks)} 只主板股票（耗时 {elapsed:.2f}秒）")

    # ── 第二步：过滤 ST 股票 ──
    print("\n" + "=" * 60)
    print("🔍 第二阶段：过滤 ST/退市/债券/低价股")
    print("=" * 60)
    start_time = datetime.now()
    stocks = filter_by_name(stocks)
    elapsed = (datetime.now() - start_time).total_seconds()
    if not stocks:
        print("❌ 无符合条件的股票，程序退出")
        return pd.DataFrame()
    print(f"✅ 第二阶段完成：剩余 {len(stocks)} 只股票（耗时 {elapsed:.2f}秒）")

    # ── 第三步：筛选T-1日创近N日新低的股票 ──
    print("\n" + "=" * 60)
    lookback = CONFIG["filter"]["lookback_days"]
    print(f"🔍 第三阶段：筛选T-1日创近{lookback}日新低的股票")
    print("=" * 60)
    print(f"📋 筛选条件：T-1日最低价 == 近{lookback}日最低价（创近{lookback}日新低）")

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
    elapsed = (datetime.now() - start_time).total_seconds()
    lookback = CONFIG["filter"]["lookback_days"]
    print(f"\n✅ 第三阶段完成：共找到 {len(results)} 只T-1日创近{lookback}日新低的股票（耗时 {elapsed:.2f}秒）\n")
    print("=" * 60)

    if not results:
        print("❌ 无股票满足前三阶段条件，程序退出")
        return pd.DataFrame()

    # ── 第四步：筛选T日最低价 > T-1日最低价 ──
    start_time = datetime.now()
    results = filter_t_low_above_t_1_low(results)
    elapsed = (datetime.now() - start_time).total_seconds()
    print(f"✅ 第四阶段耗时: {elapsed:.2f}秒")

    if not results:
        print("❌ 无股票满足第四阶段条件，程序退出")
        return pd.DataFrame()

    # ── 输出结果 ──
    print(f"\n📊 最终结果：共 {len(results)} 只股票满足所有条件\n")

    df_result = pd.DataFrame(results)
    
    # 计算当前价格与T-1日最低价的百分比
    # 百分比 = (当前价格 - T-1日最低价) / T-1日最低价 × 100%
    df_result['price_vs_t1_low_pct'] = ((df_result['current_price'] - df_result['t_1_low']) / df_result['t_1_low'] * 100).round(2)
    
    # 计算T日最低价与T-1日最低价的百分比差
    # 百分比差 = (T日最低价 - T-1日最低价) / T-1日最低价 × 100%
    df_result['t_low_vs_t1_low_pct'] = ((df_result['t_low'] - df_result['t_1_low']) / df_result['t_1_low'] * 100).round(2)

    # 按百分比从低到高排序
    df_result = df_result.sort_values('price_vs_t1_low_pct', ascending=True).reset_index(drop=True)
    df_result.index += 1

    # 构建显示表格：代码，名称，当前价格，T日最低价，T-1日最低价，当前价格vs T-1日最低价(%)，T日最低价vs T-1日最低价(%)
    df_display = pd.DataFrame({
        '代码': df_result['code'].str[2:],
        '名称': df_result['name'],
        '当前': df_result['current_price'],
        'T最低': df_result.get('t_low', df_result['current_price']),  # 兼容处理
        'T-1最低': df_result['t_1_low'],
        '当前vsT-1最低': df_result['price_vs_t1_low_pct'],
        'T最低vsT-1最低': df_result['t_low_vs_t1_low_pct'],
    })

    with pd.option_context(
        "display.max_rows", 100,
        "display.max_columns", None,
        "display.width", 140,
        "display.unicode.east_asian_width", True,
    ):
        print(df_display)

    print(f"\n📄 结果已排序（按当前价格升序）")
    print(f"🏁 筛选完成: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")

    return df_result


# ============================================================
# 5️⃣ 测试模式：指定股票代码
# ============================================================
def test_codes(codes: list[str]):
    """
    测试指定股票列表，显示详细的T日和T-1日价格信息
    
    参数：
      codes: 股票代码列表
    """
    print("=" * 60)
    print("🧪 MonthLow - 测试模式")
    print("=" * 60)
    lookback = CONFIG["filter"]["lookback_days"]
    print("📋 第三阶段条件：T-1日最低价 == 近{}日最低价（创近{}日新低）".format(lookback, lookback))
    print("📋 第四阶段条件：T日最低价 > T-1日最低价")
    print()

    results = []
    for code in codes:
        print(f"\n🔍 检查 {code}...")
        result = check_month_low(code)
        if result:
            # 检查是否满足第四阶段条件
            t_low = result.get('t_low', result['current_price'])
            t_1_low = result['t_1_low']
            condition_met = t_low > t_1_low

            # 计算当前价格与T-1日最低价的百分比
            pct_vs_t1_low = round((result['current_price'] - t_1_low) / t_1_low * 100, 2)

            # 状态显示
            status = "✅ 满足" if condition_met else "❌ 不满足"

            print(
                f"  {status} | "
                f"当前价: {result['current_price']} | "
                f"T日最低: {t_low} | "
                f"T-1日最低: {t_1_low} | "
                f"vs T-1最低: {pct_vs_t1_low}%"
            )
            results.append(result)
        else:
            print("  ❌ 不满足第三阶段条件（T-1日未创近{}日新低）".format(lookback))

    if results:
        passed = sum(1 for r in results if r.get('t_low', r['current_price']) > r['t_1_low'])
        lookback = results[0].get('lookback_days', 40)
        print(f"\n✅ 共 {len(results)} 只股票满足第三阶段条件（T-1日创近{lookback}日新低）")
        print(f"✅ 其中 {passed} 只股票满足第四阶段条件（T日最低价 > T-1日最低价）")
    else:
        print("\n❌ 无股票满足第三阶段条件")

    return results


# ============================================================
# 6️⃣ 从文件读取股票代码并进行完整筛选
# ============================================================
def filter_codes_from_file(file_path: str) -> pd.DataFrame:
    """
    从文件读取股票代码列表，进行完整的四阶段筛选，判断是否符合最终条件
    
    文件格式：每行一个股票代码，支持以下格式
      sz000001
      sh600000
      000001（自动识别，深市补sz，沪市补sh）
    
    参数：
      file_path: 包含股票代码的文件路径
    
    返回：
      pd.DataFrame: 符合所有条件的股票列表
    """
    print("=" * 60)
    print("📁 MonthLow - 指定股票列表筛选模式")
    print("=" * 60)
    print(f"📄 读取文件: {file_path}")
    
    # 读取文件
    try:
        with open(file_path, 'r', encoding='utf-8') as f:
            lines = f.readlines()
    except FileNotFoundError:
        print(f"❌ 文件不存在: {file_path}")
        return pd.DataFrame()
    except Exception as e:
        print(f"❌ 读取文件失败: {e}")
        return pd.DataFrame()
    
    # 解析股票代码
    codes = []
    for line in lines:
        line = line.strip()
        if not line or line.startswith('#'):  # 跳过空行和注释
            continue
        
        # 自动补充市场前缀
        if line.startswith('sz') or line.startswith('sh'):
            codes.append(line)
        elif line.startswith('0') or line.startswith('3'):
            # 深市
            codes.append(f'sz{line}')
        elif line.startswith('6'):
            # 沪市
            codes.append(f'sh{line}')
        else:
            print(f"⚠️  无法识别的股票代码: {line}")
    
    if not codes:
        print("❌ 文件中没有有效的股票代码")
        return pd.DataFrame()
    
    print(f"✅ 共读取 {len(codes)} 只股票代码")
    
    # 执行完整的四阶段筛选
    return filter_codes_list(codes)


def filter_codes_list(codes: list[str]) -> pd.DataFrame:
    """
    对指定的股票代码列表进行完整的四阶段筛选
    
    参数：
      codes: 股票代码列表
    
    返回：
      pd.DataFrame: 符合所有条件的股票列表
    """
    lookback = CONFIG["filter"]["lookback_days"]
    print(f"\n🔍 开始对 {len(codes)} 只股票进行四阶段筛选...")
    print(f"📋 第三阶段条件：T-1日最低价 == 近{lookback}日最低价（创近{lookback}日新低）")
    print(f"📋 第四阶段条件：T日最低价 > T-1日最低价")
    print()
    
    # ── 第一阶段 & 第二阶段：跳过（用户指定股票） ──
    print("✅ 第一、二阶段：跳过（使用指定股票列表）")
    
    # ── 第三阶段：筛选T-1日创近N日新低的股票 ──
    print("\n" + "=" * 60)
    print(f"🔍 第三阶段：筛选T-1日创近{lookback}日新低的股票")
    print("=" * 60)
    print(f"📋 筛选条件：T-1日最低价 == 近{lookback}日最低价（创近{lookback}日新低）")
    print()
    
    results = []
    total = len(codes)
    processed = 0
    
    for code in codes:
        result = check_month_low(code)
        if result:
            results.append(result)
        
        processed += 1
        
        # 每处理 10 只股票显示一次进度
        if processed % 10 == 0 or processed == total:
            pct = processed / total * 100
            print(
                f"\r  📊 筛选进度: {processed}/{total} ({pct:.1f}%) - "
                f"符合第三阶段: {len(results)} 只",
                end="",
                flush=True
            )
    
    print()
    print(f"\n✅ 第三阶段完成：共找到 {len(results)} 只股票创近{lookback}日新低")
    print("=" * 60)
    
    if not results:
        print("❌ 无股票满足第三阶段条件，程序退出")
        return pd.DataFrame()
    
    # ── 第四步：筛选T日最低价 > T-1日最低价 ──
    start_time = datetime.now()
    results = filter_t_low_above_t_1_low(results)
    elapsed = (datetime.now() - start_time).total_seconds()
    print(f"✅ 第四阶段耗时: {elapsed:.2f}秒")
    
    if not results:
        print("❌ 无股票满足第四阶段条件，程序退出")
        return pd.DataFrame()
    
    # ── 输出结果 ──
    print(f"\n📊 最终结果：共 {len(results)} 只股票满足所有条件\n")
    
    df_result = pd.DataFrame(results)
    
    # 计算当前价格与T-1日最低价的百分比
    # 百分比 = (当前价格 - T-1日最低价) / T-1日最低价 × 100%
    df_result['price_vs_t1_low_pct'] = ((df_result['current_price'] - df_result['t_1_low']) / df_result['t_1_low'] * 100).round(2)
    
    # 计算T日最低价与T-1日最低价的百分比差
    # 百分比差 = (T日最低价 - T-1日最低价) / T-1日最低价 × 100%
    df_result['t_low_vs_t1_low_pct'] = ((df_result['t_low'] - df_result['t_1_low']) / df_result['t_1_low'] * 100).round(2)
    
    # 按百分比从低到高排序
    df_result = df_result.sort_values('price_vs_t1_low_pct', ascending=True).reset_index(drop=True)
    df_result.index += 1
    
    # 构建显示表格：代码，名称，当前价格，T日最低价，T-1日最低价，当前价格vs T-1日最低价(%)，T日最低价vs T-1日最低价(%)
    df_display = pd.DataFrame({
        '代码': df_result['code'].str[2:],
        '名称': df_result['name'],
        '当前价格': df_result['current_price'],
        'T日最低价': df_result.get('t_low', df_result['current_price']),  # 兼容处理
        'T-1日最低价': df_result['t_1_low'],
        '当前价格vs T-1日最低价(%)': df_result['price_vs_t1_low_pct'],
        'T日最低价vs T-1日最低价(%)': df_result['t_low_vs_t1_low_pct'],
    })
    
    with pd.option_context(
        "display.max_rows", 100,
        "display.max_columns", None,
        "display.width", 160,
        "display.unicode.east_asian_width", True,
    ):
        print(df_display)
    
    print(f"\n📄 结果已排序（按当前价格vs T-1日最低价(%)从低到高）")
    print(f"🏁 筛选完成: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    
    # 输出未通过的股票
    if len(codes) > len(results):
        print(f"\n" + "=" * 60)
        print(f"📋 筛选结果汇总")
        print("=" * 60)
        print(f"📊 输入股票总数: {len(codes)}")
        print(f"❌ 未通过第三阶段: {len(codes) - len(results) - len([r for r in results if not r.get('t_low', r['current_price']) > r['t_1_low']])}")
        
        passed_count = sum(1 for r in results if r.get('t_low', r['current_price']) > r['t_1_low'])
        print(f"✅ 通过第三阶段: {len(results)}")
        print(f"✅ 通过第四阶段（最终符合）: {len(results)}")
        
        # 显示未通过的股票
        passed_codes = set(r['code'] for r in results)
        failed_codes = [c for c in codes if c not in passed_codes]
        if failed_codes:
            print(f"\n❌ 未通过筛选的股票代码:")
            for code in failed_codes:
                print(f"   {code}")
    
    return df_result


# ============================================================
# 7️⃣ 执行入口
# ============================================================
if __name__ == '__main__':
    import argparse

    parser = argparse.ArgumentParser(description='MonthLow - 近N日低点选股策略')
    parser.add_argument('--code', action='append', default=[], help="测试指定代码，如 --code sz000001 --code sh600000")
    parser.add_argument('--codes', type=str, default=None, help='逗号分隔的测试代码')
    parser.add_argument('--file', type=str, default=None, help='包含股票代码的文件路径（每行一个代码）')
    args = parser.parse_args()

    # 优先级：文件模式 > 测试代码模式 > 全市场筛选
    if args.file:
        # 从文件读取股票代码并进行筛选
        filter_codes_from_file(args.file)
    else:
        test_list = list(args.code)
        if args.codes:
            test_list.extend(args.codes.split(','))

        if test_list:
            test_codes([c.strip() for c in test_list if c.strip()])
        else:
            pick_month_low_stocks()
