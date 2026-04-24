"""
定时任务：每天 14:45 自动运行选股，结果落库并发送通知
"""
import schedule
import time
import pandas as pd
from datetime import date
from Alpha2 import get_stock_list, get_batch_realtime, fast_filter, stage2_filter, CONFIG
from concurrent.futures import ThreadPoolExecutor
from db import init_db, upsert_scan_results
from notifier import notify_results


def run_scan():
    trade_date = date.today().strftime("%Y-%m-%d")
    print(f"\n[{trade_date}] 开始选股扫描...")

    stocks    = get_stock_list()
    perf      = CONFIG["performance"]
    batch_size = int(perf["batch_size"])
    batches   = [stocks[i:i + batch_size] for i in range(0, len(stocks), batch_size)]

    # 第一阶段
    with ThreadPoolExecutor(max_workers=int(perf["max_workers"])) as ex:
        results = ex.map(get_batch_realtime, batches)
    all_data = [item for sub in results for item in sub]
    filtered = [x for x in all_data if fast_filter(x)]
    print(f"第一阶段通过: {len(filtered)} 只")

    # 第二阶段
    top_n  = int(CONFIG["after_14_filter"]["top_n"])
    passed = []
    for stock in filtered:
        code   = stock.get('qcode') or stock.get('code')
        result = stage2_filter(code)
        if result:
            passed.append({
                'code':      code,
                'name':      stock.get('name', '-'),
                'price':     result['now_price'],
                'above_pct': result['above_pct'],
                'ma5_bias':  result['ma5_bias'],
            })
        time.sleep(float(perf["sleep_between_stage2"]))

    passed_sorted = sorted(passed, key=lambda x: x['above_pct'], reverse=True)[:top_n]
    print(f"第二阶段通过: {len(passed_sorted)} 只")

    # 落库
    upsert_scan_results(trade_date, passed_sorted)
    print(f"落库成功：{len(passed_sorted)} 条")

    # 通知
    df1 = pd.DataFrame([{
        '代码': x.get('qcode', x.get('code', '')), '名称': x.get('name', '-'),
        '价格': x.get('price', '-'), '涨跌幅': x.get('涨跌幅', '-'),
        '换手率': x.get('换手率', '-'), '量比': x.get('量比', '-'),
    } for x in filtered])
    df2 = pd.DataFrame(passed_sorted).rename(columns={
        'code': '代码', 'name': '名称', 'price': '价格',
        'above_pct': '均线上方占比(%)', 'ma5_bias': 'MA5乖离率(%)',
    }) if passed_sorted else pd.DataFrame()
    notify_results(df1, df2)


if __name__ == '__main__':
    init_db()
    print("数据库初始化完成")

    schedule.every().day.at("14:45").do(run_scan)
    print("定时任务已启动，每天 14:45 执行选股...")

    # 支持立即执行一次（调试用）
    import sys
    if len(sys.argv) > 1 and sys.argv[1] == '--now':
        run_scan()
    else:
        while True:
            schedule.run_pending()
            time.sleep(30)
