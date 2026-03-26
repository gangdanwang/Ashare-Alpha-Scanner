import requests
import time
import argparse
import pandas as pd
from concurrent.futures import ThreadPoolExecutor
from Ashare import get_price
from Ddemo3_config import CONFIG as _DDEMO_CONFIG  # 保留兼容，不再使用
from Alpha2_config import CONFIG

# =============================
# 1️⃣ 批量获取腾讯数据（核心优化）
# =============================
def get_batch_realtime(codes):
    code_str = ','.join(codes)
    url = f'http://qt.gtimg.cn/q={code_str}'
    
    try:
        text = requests.get(url, timeout=5).text
        lines = text.split(';')
        
        results = []
        
        for line in lines:
            if not line.strip():
                continue
            
            # 示例行：v_sz002368="51~太极股份~002368~..."
            # 这里同时保留带市场前缀的 code（用于后续K线接口）
            qcode = None
            try:
                head = line.split('=', 1)[0].strip()
                if head.startswith('v_'):
                    qcode = head[2:]
            except:
                qcode = None

            data = line.split('~')
            
            try:
                results.append({
                    'qcode': qcode,
                    'code': data[2],
                    'name': data[1],
                    'price': float(data[3]),
                    '涨跌幅': float(data[32]),
                    '换手率': float(data[38]),
                    '流通市值': float(data[45]),
                    '量比': float(data[46]),
                })
            except:
                continue
        
        return results
    except:
        return []


# =============================
# 2️⃣ 股票池（精简版）
# =============================
def get_stock_list():
    stocks = []
    
    # 深证
    sz_start = int(CONFIG["stock_pool"]["sz_start"])
    sz_end_exclusive = int(CONFIG["stock_pool"]["sz_end_exclusive"])
    stocks += [f'sz{i:06d}' for i in range(sz_start, sz_end_exclusive)]
    
    # 上证
    sh_start = int(CONFIG["stock_pool"]["sh_start"])
    sh_end_exclusive = int(CONFIG["stock_pool"]["sh_end_exclusive"])
    stocks += [f'sh{i}' for i in range(sh_start, sh_end_exclusive)]
    
    return stocks


# =============================
# 3️⃣ 第一阶段筛选（极快）
# =============================
def fast_filter(info):
    try:
        cfg = CONFIG["fast_filter"]
        return (
            float(cfg["pct_change_min"]) < info['涨跌幅'] < float(cfg["pct_change_max"]) and
            float(cfg["volume_ratio_min"]) < info['量比'] < float(cfg["volume_ratio_max"]) and
            float(cfg["turnover_min"]) < info['换手率'] < float(cfg["turnover_max"]) and
            float(cfg["float_mktcap_min_yi"]) < info['流通市值'] < float(cfg["float_mktcap_max_yi"])
        )
    except:
        return False


# =============================
# 4️⃣ 第二阶段：均线结构筛选
#
# Step1 优先级判断（满足任一即可进入 Step2）：
#   优先级1（最高）：9:40后每根K线收盘价始终在累计分时均价上方
#   优先级2：9:40后价格围绕分时均价运行（偏离率标准差 <= 阈值）
#
# Step2 叠加条件（通过Step1后必须同时满足）：
#   条件A：现价高于当日分时均价 > min_price_above_intraday_ma
#   条件B：现价相对MA5乖离率 <= max_bias_to_ma5
# =============================
def after_14_filter(code):
    try:
        cfg = CONFIG["after_14_filter"]

        # ── 拉当日分钟K线 ──
        intraday_df = get_price(code, frequency=cfg["intraday_frequency"], count=int(cfg["intraday_count"]))
        if intraday_df is None or intraday_df.empty:
            return False
        intraday_df = intraday_df.sort_index()

        # 只取今日数据
        today = pd.Timestamp.now().normalize()
        df_today = intraday_df[intraday_df.index >= today].copy()
        if df_today.empty:
            return False

        # 累计分时均价（每根K线对应截至该时刻的均价，即 expanding mean）
        df_today['vwap'] = df_today['close'].expanding().mean()

        now_price    = df_today['close'].iloc[-1]
        intraday_ma  = df_today['close'].mean()   # 全天均价（用于 Step2 条件A）

        # ── Step1：优先级判断 ──
        start_time = cfg["priority_start_hhmm"]
        df_priority = df_today[df_today.index.strftime('%H:%M') >= start_time]

        priority_passed = False

        if not df_priority.empty:
            # 优先级1：9:40后每根K线收盘价始终高于当时的累计均价
            always_above = (df_priority['close'] > df_priority['vwap']).all()
            if always_above:
                priority_passed = True
            else:
                # 优先级2：偏离率标准差 <= 阈值（价格围绕均线运行）
                deviation = (df_priority['close'] - df_priority['vwap']) / df_priority['vwap']
                if deviation.std() <= float(cfg["max_vwap_deviation_std"]):
                    priority_passed = True

        if not priority_passed:
            return False

        # ── Step2 条件A：现价高于分时均价超过阈值 ──
        if intraday_ma <= 0:
            return False
        if (now_price / intraday_ma - 1) <= float(cfg["min_price_above_intraday_ma"]):
            return False

        # ── Step2 条件B：MA5 乖离率 ──
        daily_df = get_price(code, frequency='1d', count=int(cfg["daily_count"]))
        if daily_df is None or len(daily_df) < int(cfg["ma_window"]):
            return False
        daily_df = daily_df.sort_index()
        ma5 = daily_df['close'].rolling(int(cfg["ma_window"])).mean().iloc[-1]
        if pd.isna(ma5) or ma5 <= 0:
            return False

        return abs(now_price / ma5 - 1) <= float(cfg["max_bias_to_ma5"])

    except:
        return False


# =============================
# 5️⃣ 主函数（高性能）
# =============================
def pick_stocks_fast(test_code: str | None = None):
    # 快速测试：只跑单只股票（便于你随时替换代码验证）
    if test_code:
        test = get_batch_realtime([test_code])
        if not test:
            print(f"实时数据获取失败：{test_code}")
            return pd.DataFrame([])

        print("是否通过第一阶段：", fast_filter(test[0]))
        print("是否通过第二阶段：", after_14_filter(test_code))

        passed_1 = fast_filter(test[0])
        passed_2 = after_14_filter(test_code)

        if passed_1 and passed_2:
            row = dict(test[0])
            row['code'] = test_code
            return pd.DataFrame([row])

        return pd.DataFrame([])

    # 还原：全市场批量筛选
    stocks = get_stock_list()

    # 👉 分批（每批50个）
    perf = CONFIG["performance"]
    batch_size = int(perf["batch_size"])
    batches = [stocks[i:i + batch_size] for i in range(0, len(stocks), batch_size)]

    print(f"总批次数: {len(batches)}")

    # ===== 第一阶段（并发批量请求）=====
    with ThreadPoolExecutor(max_workers=int(perf["max_workers"])) as executor:
        results = executor.map(get_batch_realtime, batches)

    # 扁平化
    all_data = [item for sublist in results for item in sublist]

    print(f"第一阶段完成，数量: {len(all_data)}")

    # ===== 粗筛 =====
    filtered = [x for x in all_data if fast_filter(x)]
    print(f"粗筛后剩余: {len(filtered)}")

    # ===== 第二阶段（K线筛选）=====
    final = []
    total_stage2 = len(filtered)
    stage2_start = time.time()

    for idx, stock in enumerate(filtered, start=1):
        code = stock.get('qcode') or stock.get('code')

        if after_14_filter(code):
            final.append(stock)

        # 友好进度展示：已处理/总数/通过数/耗时/预计剩余
        elapsed = time.time() - stage2_start
        avg = elapsed / idx if idx else 0.0
        remain = max(total_stage2 - idx, 0) * avg
        pct = (idx / total_stage2 * 100.0) if total_stage2 else 100.0
        print(
            f"\r第二阶段进度: {idx}/{total_stage2} ({pct:.1f}%) | "
            f"已通过: {len(final)} | 已耗时: {elapsed:.1f}s | 预计剩余: {remain:.1f}s",
            end="",
            flush=True,
        )

        time.sleep(float(perf["sleep_between_stage2"]))

    if total_stage2:
        print()

    return pd.DataFrame(final)

def quick_test_codes(codes: list[str]) -> pd.DataFrame:
    codes = [c.strip() for c in codes if c and c.strip()]
    if not codes:
        return pd.DataFrame([])

    realtime = get_batch_realtime(codes)
    realtime_by_code = {f"{x.get('code', '')}".strip(): x for x in realtime}

    rows = []
    for c in codes:
        # 腾讯 realtime 返回 code 可能不带市场前缀，这里两种都兼容
        key1 = c.replace('sz', '').replace('sh', '')
        info = realtime_by_code.get(c) or realtime_by_code.get(key1)

        if not info:
            print(f"实时数据获取失败：{c}")
            rows.append({'code': c, 'passed_stage1': False, 'passed_stage2': False})
            continue

        passed_1 = fast_filter(info)
        print("是否通过第一阶段：", passed_1)

        passed_2 = after_14_filter(c)
        print("是否通过第二阶段：", passed_2)

        row = dict(info)
        row['code'] = c
        row['passed_stage1'] = passed_1
        row['passed_stage2'] = passed_2
        rows.append(row)

        time.sleep(float(CONFIG["performance"]["sleep_between_stage2"]))

    return pd.DataFrame(rows)


# =============================
# 6️⃣ 执行
# =============================
if __name__ == '__main__':
    start = time.time()

    parser = argparse.ArgumentParser()
    parser.add_argument('--code', action='append', default=[], help="可重复：--code sz002368 --code sh600681")
    parser.add_argument('--codes', type=str, default=None, help='逗号分隔：--codes "sz002368,sh600681"')
    args = parser.parse_args()

    codes = list(args.code or [])
    if args.codes:
        codes.extend(args.codes.split(','))

    if codes:
        df = quick_test_codes(codes)
    else:
        df = pick_stocks_fast(test_code=None)

    print("\n结果：")
    with pd.option_context(
        "display.max_rows", None,
        "display.max_columns", None,
        "display.width", 2000,
    ):
        print(df)

    # df.to_csv(CONFIG["output"]["csv_path"], index=False)

    print(f"\n耗时: {time.time() - start:.2f} 秒")