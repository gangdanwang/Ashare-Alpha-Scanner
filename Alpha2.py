import requests
import time
import argparse
import pandas as pd
from concurrent.futures import ThreadPoolExecutor
from Ashare import get_price
from Ddemo3_config import CONFIG as _DDEMO_CONFIG  # 保留兼容，不再使用
from Alpha2_config import CONFIG
from notifier import notify_results

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
# 4️⃣ 第二阶段：分时结构评分
#
# 对每只股票的 9:40 后分钟K线进行多维度打分：
#   +10  均线位置：全程在 vwap 上方
#   +8   回踩质量：回踩不破 or ≤3分钟快速收回
#   +6   趋势斜率：vwap 缓慢上行（非直线拉升）
#   +6   结构稳定：无单分钟跌幅 > 1%
#   -5   多次跌破均线（> 3次）
#   -6   下午才启动（14:00后 vwap 才开始上行）
#
# 基础门槛：MA5 乖离率 <= max_bias_to_ma5，不满足直接排除
# 返回 None 表示不满足基础门槛或数据异常
# =============================
def stage2_filter(code: str) -> dict | None:
    """
    第二阶段筛选：
      基础门槛：当前价格在分时均线（vwap）上方，否则排除
      条件1：当天价格在分时均线上方的占比率 >= min_above_vwap_pct
      条件2：当前价格相对 MA5 乖离率 <= max_bias_to_ma5
    返回 None 表示不满足条件，否则返回指标字典。
    """
    try:
        cfg = CONFIG["after_14_filter"]

        # ── 拉当日分钟K线 ──
        intraday_df = get_price(code, frequency=cfg["intraday_frequency"], count=int(cfg["intraday_count"]))
        if intraday_df is None or intraday_df.empty:
            return None
        intraday_df = intraday_df.sort_index()

        today = pd.Timestamp.now().normalize()
        df_today = intraday_df[intraday_df.index >= today].copy()
        if df_today.empty:
            return None

        df_today['vwap'] = df_today['close'].expanding().mean()

        start_hm = cfg["start_hhmm"]
        df = df_today[df_today.index.strftime('%H:%M') >= start_hm].copy()
        if len(df) < 5:
            return None

        now_price = df['close'].iloc[-1]
        now_vwap  = df['vwap'].iloc[-1]

        # ── 基础门槛：当前价格在分时均线上方 ──
        if now_price <= now_vwap:
            return None

        # ── 条件1：均线上方占比率 ──
        above_pct = (df['close'] > df['vwap']).mean()
        if above_pct < float(cfg["min_above_vwap_pct"]):
            return None

        # ── 条件2：MA5 乖离率 ──
        daily_df = get_price(code, frequency='1d', count=int(cfg["daily_count"]))
        if daily_df is None or len(daily_df) < int(cfg["ma_window"]):
            return None
        daily_df = daily_df.sort_index()
        ma5 = daily_df['close'].rolling(int(cfg["ma_window"])).mean().iloc[-1]
        if pd.isna(ma5) or ma5 <= 0:
            return None
        ma5_bias = abs(now_price / ma5 - 1)
        if ma5_bias > float(cfg["max_bias_to_ma5"]):
            return None

        return {
            'code':      code,
            'now_price': now_price,
            'above_pct': round(above_pct * 100, 1),
            'ma5_bias':  round(ma5_bias * 100, 2),
        }

    except:
        return None


# 保留函数名兼容主流程调用（全市场扫描时用）
def after_14_filter(code: str) -> bool:
    return stage2_filter(code) is not None


def _print_table(df: pd.DataFrame):
    with pd.option_context(
        "display.max_rows", None,
        "display.max_columns", None,
        "display.width", 160,
        "display.unicode.east_asian_width", True,
    ):
        print(df.to_string())


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

        passed_1 = fast_filter(test[0])
        print(f"第一阶段: {'✓' if passed_1 else '✗'}")

        result = score_stock(test_code)
        if result:
            print(f"第二阶段评分: {result['score']}")
            for dim, val in result['detail'].items():
                print(f"  {dim}: {val}")
            row = dict(test[0])
            row['code'] = test_code
            row['score'] = result['score']
            return pd.DataFrame([row])
        else:
            print("第二阶段: 不满足基础门槛")
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

    all_data = [item for sublist in results for item in sublist]
    filtered = [x for x in all_data if fast_filter(x)]

    # ── 第一阶段结果表格 ──
    print(f"\n【第一阶段】粗筛通过 {len(filtered)} 只\n")
    if filtered:
        df_stage1 = pd.DataFrame([{
            '代码':   x.get('qcode', x.get('code', '')),
            '名称':   x.get('name', '-'),
            '价格':   x.get('price', '-'),
            '涨跌幅': x.get('涨跌幅', '-'),
            '换手率': x.get('换手率', '-'),
            '量比':   x.get('量比', '-'),
        } for x in filtered])
        df_stage1.index += 1
        _print_table(df_stage1)

    # ===== 第二阶段筛选 =====
    print(f"\n【第二阶段】筛选中，共 {len(filtered)} 只...")
    top_n = int(CONFIG["after_14_filter"]["top_n"])
    passed = []
    total_stage2 = len(filtered)
    stage2_start = time.time()

    for idx, stock in enumerate(filtered, start=1):
        code = stock.get('qcode') or stock.get('code')
        result = stage2_filter(code)
        if result:
            passed.append({
                'qcode':     code,
                'name':      stock.get('name', '-'),
                'price':     result['now_price'],
                'above_pct': result['above_pct'],
                'ma5_bias':  result['ma5_bias'],
            })

        elapsed = time.time() - stage2_start
        avg = elapsed / idx if idx else 0.0
        remain = max(total_stage2 - idx, 0) * avg
        pct = (idx / total_stage2 * 100.0) if total_stage2 else 100.0
        print(
            f"\r进度: {idx}/{total_stage2} ({pct:.1f}%) | "
            f"已通过: {len(passed)} | 耗时: {elapsed:.1f}s | 剩余: {remain:.1f}s",
            end="", flush=True,
        )
        time.sleep(float(perf["sleep_between_stage2"]))

    if total_stage2:
        print()

    if not passed:
        print("第二阶段无满足条件的股票")
        return pd.DataFrame()

    df_passed = (pd.DataFrame(passed)
                 .sort_values('above_pct', ascending=False)
                 .head(top_n)
                 .reset_index(drop=True))
    df_passed.index += 1

    print(f"\n【第二阶段】通过 {len(df_passed)} 只\n")
    _print_table(df_passed.rename(columns={
        'qcode':     '代码',
        'name':      '名称',
        'price':     '价格',
        'above_pct': '均线上方占比(%)',
        'ma5_bias':  'MA5乖离率(%)',
    }))

    # 发送通知
    df1_notify = pd.DataFrame([{
        '代码': x.get('qcode', x.get('code', '')), '名称': x.get('name', '-'),
        '价格': x.get('price', '-'), '涨跌幅': x.get('涨跌幅', '-'),
        '换手率': x.get('换手率', '-'), '量比': x.get('量比', '-'),
    } for x in filtered])
    df2_notify = df_passed.rename(columns={
        'qcode': '代码', 'name': '名称', 'price': '价格',
        'above_pct': '均线上方占比(%)', 'ma5_bias': 'MA5乖离率(%)',
    })
    notify_results(df1_notify, df2_notify)

    return df_passed

def quick_test_codes(codes: list[str]) -> pd.DataFrame:
    codes = [c.strip() for c in codes if c and c.strip()]
    if not codes:
        return pd.DataFrame([])

    realtime = get_batch_realtime(codes)
    realtime_by_code = {f"{x.get('code', '')}".strip(): x for x in realtime}

    stage1_rows, stage2_rows = [], []

    for c in codes:
        key1 = c.replace('sz', '').replace('sh', '')
        info = realtime_by_code.get(c) or realtime_by_code.get(key1)

        if not info:
            print(f"实时数据获取失败：{c}")
            continue

        # 第一阶段
        if fast_filter(info):
            stage1_rows.append({
                '代码':   c,
                '名称':   info.get('name', '-'),
                '价格':   info.get('price', '-'),
                '涨跌幅': info.get('涨跌幅', '-'),
                '换手率': info.get('换手率', '-'),
                '量比':   info.get('量比', '-'),
            })

        # 第二阶段
        result = stage2_filter(c)
        if result:
            stage2_rows.append({
                '代码':          c,
                '名称':          info.get('name', '-') if info else '-',
                '价格':          result['now_price'],
                '均线上方占比(%)': result['above_pct'],
                'MA5乖离率(%)':  result['ma5_bias'],
            })

        time.sleep(float(CONFIG["performance"]["sleep_between_stage2"]))

    print("\n【第一阶段】粗筛通过\n")
    if stage1_rows:
        df1 = pd.DataFrame(stage1_rows)
        df1.index += 1
        _print_table(df1)
    else:
        print("无")

    print("\n【第二阶段】评分结果\n")
    if stage2_rows:
        df2 = pd.DataFrame(stage2_rows).sort_values('均线上方占比(%)', ascending=False)
        df2 = df2.reset_index(drop=True)
        df2.index += 1
        _print_table(df2)
    else:
        print("无满足条件的股票")

    df1_notify = pd.DataFrame(stage1_rows) if stage1_rows else pd.DataFrame()
    df2_notify = pd.DataFrame(stage2_rows) if stage2_rows else pd.DataFrame()
    notify_results(df1_notify, df2_notify)

    return df2_notify if stage2_rows else df1_notify


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
        quick_test_codes(codes)
    else:
        pick_stocks_fast(test_code=None)

    print(f"\n耗时: {time.time() - start:.2f} 秒")