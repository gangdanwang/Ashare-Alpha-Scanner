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
def score_stock(code: str) -> dict | None:
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

        # 累计分时均价（expanding mean = 截至每根K线的均价）
        df_today['vwap'] = df_today['close'].expanding().mean()

        # 只取 priority_start_hhmm 之后的数据参与评分
        start_hm = cfg["priority_start_hhmm"]
        df = df_today[df_today.index.strftime('%H:%M') >= start_hm].copy()
        if len(df) < 5:
            return None

        now_price = df['close'].iloc[-1]
        score = 0
        detail = {}

        # ── 基础门槛：MA5 乖离率 ──
        daily_df = get_price(code, frequency='1d', count=int(cfg["daily_count"]))
        if daily_df is None or len(daily_df) < int(cfg["ma_window"]):
            return None
        daily_df = daily_df.sort_index()
        ma5 = daily_df['close'].rolling(int(cfg["ma_window"])).mean().iloc[-1]
        if pd.isna(ma5) or ma5 <= 0:
            return None
        if abs(now_price / ma5 - 1) > float(cfg["max_bias_to_ma5"]):
            return None

        # ── 加分1：均线位置（全程在 vwap 上方）──
        always_above = (df['close'] > df['vwap']).all()
        if always_above:
            score += cfg["score_always_above_vwap"]
            detail['均线位置'] = f"+{cfg['score_always_above_vwap']} 全程在均线上方"
        else:
            detail['均线位置'] = "0"

        # ── 加分2：回踩质量 ──
        # 找出所有跌破 vwap 的位置，检查是否在 N 分钟内收回
        recover_win = int(cfg["pullback_recover_minutes"])
        closes = df['close'].values
        vwaps  = df['vwap'].values
        break_indices = [i for i in range(len(closes)) if closes[i] < vwaps[i]]
        break_count = len(break_indices)

        if break_count == 0:
            # 全程未破线，回踩质量满分
            score += cfg["score_pullback_quality"]
            detail['回踩质量'] = f"+{cfg['score_pullback_quality']} 未破线"
        else:
            # 检查每次破线后是否快速收回
            all_recover = all(
                any(closes[j] > vwaps[j] for j in range(i + 1, min(i + 1 + recover_win, len(closes))))
                for i in break_indices
            )
            if all_recover:
                score += cfg["score_pullback_quality"]
                detail['回踩质量'] = f"+{cfg['score_pullback_quality']} 快速收回"
            else:
                detail['回踩质量'] = "0 未能快速收回"

        # ── 加分3：趋势斜率（vwap 缓慢上行）──
        vwap_series = df['vwap'].values
        vwap_slope_up = vwap_series[-1] > vwap_series[0]   # 整体向上
        max_single_rise = df['close'].pct_change().max()
        slope_ok = vwap_slope_up and max_single_rise <= float(cfg["slope_max_single_pct"])
        if slope_ok:
            score += cfg["score_trend_slope"]
            detail['趋势斜率'] = f"+{cfg['score_trend_slope']} 缓慢上行"
        else:
            detail['趋势斜率'] = "0 直线拉升或下行"

        # ── 加分4：结构稳定性（无单分钟跌幅 > 阈值）──
        min_pct_change = df['close'].pct_change().min()
        stable = min_pct_change > -float(cfg["stability_drop_threshold"])
        if stable:
            score += cfg["score_stability"]
            detail['结构稳定'] = f"+{cfg['score_stability']} 无明显跳水"
        else:
            detail['结构稳定'] = f"0 存在跳水（最大跌幅{min_pct_change:.1%}）"

        # ── 扣分1：多次跌破均线 ──
        if break_count > int(cfg["break_vwap_max_times"]):
            score += cfg["penalty_break_vwap"]   # 负数
            detail['跌破均线'] = f"{cfg['penalty_break_vwap']} 跌破{break_count}次"
        else:
            detail['跌破均线'] = f"0 跌破{break_count}次"

        # ── 扣分2：下午才启动 ──
        late_hm = cfg["late_start_hhmm"]
        df_am = df[df.index.strftime('%H:%M') < late_hm]
        df_pm = df[df.index.strftime('%H:%M') >= late_hm]
        # 判断：上午 vwap 斜率 <= 0，下午才开始上行
        late_start = False
        if len(df_am) >= 2 and len(df_pm) >= 2:
            am_slope = df_am['vwap'].iloc[-1] - df_am['vwap'].iloc[0]
            pm_slope = df_pm['vwap'].iloc[-1] - df_pm['vwap'].iloc[0]
            if am_slope <= 0 and pm_slope > 0:
                late_start = True
        if late_start:
            score += cfg["penalty_late_start"]   # 负数
            detail['启动时间'] = f"{cfg['penalty_late_start']} 下午才启动"
        else:
            detail['启动时间'] = "0 上午已启动"

        return {
            'code':   code,
            'score':  score,
            'detail': detail,
            'now_price': now_price,
            'ma5':    round(ma5, 3),
        }

    except:
        return None


# 保留函数名兼容主流程调用（全市场扫描时用）
def after_14_filter(code: str) -> bool:
    result = score_stock(code)
    return result is not None and result['score'] > 0


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

    # 扁平化
    all_data = [item for sublist in results for item in sublist]

    print(f"第一阶段完成，数量: {len(all_data)}")

    # ===== 粗筛 =====
    filtered = [x for x in all_data if fast_filter(x)]
    print(f"粗筛后剩余: {len(filtered)}")

    # ===== 第二阶段（评分筛选）=====
    top_n = int(CONFIG["after_14_filter"]["top_n"])
    scored = []
    total_stage2 = len(filtered)
    stage2_start = time.time()

    for idx, stock in enumerate(filtered, start=1):
        code = stock.get('qcode') or stock.get('code')
        result = score_stock(code)
        if result and result['score'] > 0:
            stock['score'] = result['score']
            stock['detail'] = str(result['detail'])
            scored.append(stock)

        elapsed = time.time() - stage2_start
        avg = elapsed / idx if idx else 0.0
        remain = max(total_stage2 - idx, 0) * avg
        pct = (idx / total_stage2 * 100.0) if total_stage2 else 100.0
        print(
            f"\r第二阶段进度: {idx}/{total_stage2} ({pct:.1f}%) | "
            f"已通过: {len(scored)} | 已耗时: {elapsed:.1f}s | 预计剩余: {remain:.1f}s",
            end="", flush=True,
        )
        time.sleep(float(perf["sleep_between_stage2"]))

    if total_stage2:
        print()

    if not scored:
        return pd.DataFrame()

    df_scored = pd.DataFrame(scored).sort_values('score', ascending=False).head(top_n)
    df_scored = df_scored.reset_index(drop=True)
    df_scored.index += 1
    return df_scored

def quick_test_codes(codes: list[str]) -> pd.DataFrame:
    codes = [c.strip() for c in codes if c and c.strip()]
    if not codes:
        return pd.DataFrame([])

    realtime = get_batch_realtime(codes)
    realtime_by_code = {f"{x.get('code', '')}".strip(): x for x in realtime}

    rows = []
    for c in codes:
        key1 = c.replace('sz', '').replace('sh', '')
        info = realtime_by_code.get(c) or realtime_by_code.get(key1)

        if not info:
            print(f"实时数据获取失败：{c}")
            rows.append({'code': c, 'passed_stage1': False, 'score': None})
            continue

        passed_1 = fast_filter(info)
        print(f"[{c}] 第一阶段: {'✓' if passed_1 else '✗'}")

        result = score_stock(c)
        if result:
            score = result['score']
            print(f"[{c}] 第二阶段评分: {score}")
            for dim, val in result['detail'].items():
                print(f"       {dim}: {val}")
        else:
            score = None
            print(f"[{c}] 第二阶段: 不满足基础门槛或数据异常")

        row = dict(info)
        row['code'] = c
        row['passed_stage1'] = passed_1
        row['score'] = score
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