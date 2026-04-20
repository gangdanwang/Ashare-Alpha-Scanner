"""
Flask Web 后端：人工二次筛选 + 模拟买入
"""
import sys, os, subprocess, threading, queue, time, requests
sys.path.insert(0, os.path.dirname(os.path.dirname(__file__)))

from flask import Flask, jsonify, request, render_template, Response
from datetime import date
from db import get_scan_results, update_selection, insert_mock_trades, get_mock_trades, get_recent_dates, \
               get_month_low_results, get_month_low_dates, \
               upsert_watchlist, get_watchlist, get_watchlist_dates, delete_watchlist_item, \
               insert_position, get_positions, sell_position, update_stop_loss

app = Flask(__name__)

# 启动时确保所有表已创建
with app.app_context():
    try:
        from db import init_db
        init_db()
    except Exception as _e:
        print(f'[warn] init_db: {_e}')

# 全局扫描状态
_scan_state = {
    'running': False,
    'log': queue.Queue(),
}

PYTHON = sys.executable
ALPHA2 = os.path.join(os.path.dirname(os.path.dirname(__file__)), 'Alpha2.py')
MONTH_LOW = os.path.join(os.path.dirname(os.path.dirname(__file__)), 'MonthLow.py')

# MonthLow 扫描状态
_ml_state = {
    'running': False,
    'log': queue.Queue(),
}


@app.route('/')
def index():
    return render_template('index.html')


@app.route('/api/dates')
def api_dates():
    return jsonify(get_recent_dates(20))


@app.route('/api/scan/<trade_date>')
def api_scan(trade_date):
    return jsonify(get_scan_results(trade_date))


@app.route('/api/select', methods=['POST'])
def api_select():
    data = request.json
    update_selection(data['id'], data['selected'])
    return jsonify({'ok': True})


@app.route('/api/mock_buy', methods=['POST'])
def api_mock_buy():
    data       = request.json
    trade_date = data.get('trade_date', date.today().strftime('%Y-%m-%d'))
    ids        = data.get('ids')  # 可选：指定 record id 列表
    trades     = insert_mock_trades(trade_date, ids=ids)
    return jsonify({'ok': True, 'trades': trades})


@app.route('/api/trades/<trade_date>')
def api_trades(trade_date):
    return jsonify(get_mock_trades(trade_date))


@app.route('/api/market_overview')
def api_market_overview():
    def safe_float(v):
        try: return float(v)
        except: return 0.0

    all_items = []
    page = 1
    headers = {'Referer': 'http://quote.eastmoney.com/'}
    while True:
        url = (f'http://push2.eastmoney.com/api/qt/clist/get'
               f'?pn={page}&pz=500&po=1&np=1'
               f'&ut=bd1d9ddb04089700cf9c27f6f7426281'
               f'&fltt=2&invt=2&fid=f3'
               f'&fs=m:0+t:6,m:0+t:13,m:0+t:80,m:1+t:2,m:1+t:23'
               f'&fields=f2,f3,f18')
        try:
            resp = requests.get(url, timeout=10, headers=headers)
            text = resp.text.strip()
            if not text:
                break  # 非交易时间返回空，直接退出
            d = resp.json().get('data') or {}
        except Exception as e:
            return jsonify({'error': str(e)}), 500
        items = d.get('diff', [])
        all_items.extend(items)
        total = d.get('total', 0)
        if not items or (total and len(all_items) >= total):
            break
        page += 1

    up = down = flat = lu = ld = 0
    for x in all_items:
        pct    = safe_float(x.get('f3', 0))
        price  = safe_float(x.get('f2', 0))
        yclose = safe_float(x.get('f18', 0))
        if pct > 0:   up   += 1
        elif pct < 0: down += 1
        else:         flat += 1
        if yclose > 0:
            if price >= round(yclose * 1.10, 2): lu += 1
            if price <= round(yclose * 0.90, 2): ld += 1

    return jsonify({'up': up, 'down': down, 'flat': flat,
                    'limit_up': lu, 'limit_down': ld, 'total': len(all_items)})


@app.route('/position')
def position_page():
    import time
    return render_template('position.html', v=int(time.time()))


@app.route('/api/position/buy', methods=['POST'])
def api_position_buy():
    """模拟买入：每只股票 1 万元，按手（100股）向下取整"""
    data = request.json or {}
    stocks = data.get('stocks', [])
    budget = float(data.get('budget', 10000))
    results = []
    for s in stocks:
        price = float(s.get('current_price', 0))
        if price <= 0:
            results.append({'code': s['code'], 'ok': False, 'msg': '价格无效'})
            continue
        shares = int(budget / price / 100) * 100
        if shares <= 0:
            shares = 100
        amount = round(shares * price, 2)
        row = {
            'code':            s['code'],
            'name':            s.get('name', ''),
            'buy_date':        date.today().strftime('%Y-%m-%d'),
            'buy_price':       round(price, 3),
            'shares':          shares,
            'amount':          amount,
            't_1_low':         s.get('t_1_low'),
            't_low':           s.get('t_low'),
            'price_vs_t1_pct': s.get('price_vs_t1_pct'),
            't_low_vs_t1_pct': s.get('t_low_vs_t1_pct'),
            'lookback_days':   s.get('lookback_days', 20),
        }
        res = insert_position(row)
        results.append({'code': s['code'], **res})
    return jsonify({'ok': True, 'results': results})


@app.route('/api/position/list')
def api_position_list():
    status = request.args.get('status')
    rows = get_positions(status)
    # 序列化 date 类型
    for r in rows:
        for k in ['buy_date', 'sell_date', 'created_at', 'updated_at']:
            if r.get(k) and not isinstance(r[k], str):
                r[k] = str(r[k])
    return jsonify(rows)


@app.route('/api/position/sell', methods=['POST'])
def api_position_sell():
    data = request.json or {}
    res = sell_position(data['code'], float(data['sell_price']), data['sell_type'])
    return jsonify(res)


@app.route('/api/position/update_stop_loss', methods=['POST'])
def api_position_update_stop_loss():
    data = request.json or {}
    res = update_stop_loss(data['code'], float(data['stop_loss']))
    return jsonify(res)
    import time
    return render_template('watchlist.html', v=int(time.time()))


@app.route('/month_low')
def month_low():
    import time
    return render_template('month_low.html', v=int(time.time()))


@app.route('/api/watchlist/add', methods=['POST'])
def api_watchlist_add():
    data = request.json or {}
    rows = data.get('rows', [])
    add_date = data.get('add_date', date.today().strftime('%Y-%m-%d'))
    if not rows:
        return jsonify({'ok': False, 'msg': '没有选中的股票'})
    upsert_watchlist(add_date, rows)
    return jsonify({'ok': True, 'count': len(rows)})


@app.route('/api/watchlist/dates')
def api_watchlist_dates():
    return jsonify(get_watchlist_dates(20))


@app.route('/api/watchlist/<add_date>')
def api_watchlist_list(add_date):
    return jsonify(get_watchlist(add_date))


@app.route('/api/watchlist/delete', methods=['POST'])
def api_watchlist_delete():
    data = request.json or {}
    delete_watchlist_item(data['code'], data['add_date'])
    return jsonify({'ok': True})


@app.route('/api/watchlist/rescreen', methods=['POST'])
def api_watchlist_rescreen():
    """
    对自选股列表进行第四阶段再次筛选。
    实时重新获取当日价格，返回每只股票是否通过。
    """
    import sys, os
    sys.path.insert(0, os.path.dirname(os.path.dirname(__file__)))
    from MonthLow import _get_time_period
    from stock_cache import get_cached_daily_data
    from Ashare import get_price as ashare_get_price
    from datetime import datetime as dt

    data = request.json or {}
    add_date = data.get('add_date')
    if not add_date:
        return jsonify({'ok': False, 'msg': '缺少 add_date'})

    stocks = get_watchlist(add_date)
    if not stocks:
        return jsonify({'ok': False, 'msg': '该日期无自选股'})

    period = _get_time_period()
    codes_with_prefix = [('sh' if r['code'].startswith('6') else 'sz') + r['code'] for r in stocks]

    # 获取 T 日数据（按时间段）
    t_day_map = {}  # {code_with_prefix: {low, close}}

    if period == 'pre':
        # 盘前：用缓存最新一条（T-1 日）
        for code, r in zip(codes_with_prefix, stocks):
            df = get_cached_daily_data(r['code'])
            if df is not None and not df.empty:
                t_day_map[code] = {
                    'low':   round(float(df['low'].iloc[-1]), 2),
                    'close': round(float(df['close'].iloc[-1]), 2),
                }

    elif period == 'in':
        # 盘中：实时 API
        import requests as req
        code_str = ','.join(codes_with_prefix)
        try:
            text = req.get(f'http://qt.gtimg.cn/q={code_str}', timeout=10).text
            for line in text.split(';'):
                if not line.strip(): continue
                parts = line.split('~')
                try:
                    head = line.split('=', 1)[0].strip()
                    qcode = head[2:] if head.startswith('v_') else None
                    if qcode and len(parts) > 34:
                        t_day_map[qcode] = {
                            'low':   float(parts[34]) if parts[34] else 0.0,
                            'close': float(parts[3])  if parts[3]  else 0.0,
                        }
                except Exception:
                    continue
        except Exception as e:
            return jsonify({'ok': False, 'msg': f'实时数据获取失败: {e}'})

    else:
        # 收盘后：日线 iloc[-1]
        for code in codes_with_prefix:
            try:
                df = ashare_get_price(code, frequency='1d', count=2)
                if df is not None and not df.empty:
                    t_day_map[code] = {
                        'low':   round(float(df['low'].iloc[-1]), 2),
                        'close': round(float(df['close'].iloc[-1]), 2),
                    }
            except Exception:
                continue

    # 逐只判断
    results = []
    for code_pfx, r in zip(codes_with_prefix, stocks):
        td = t_day_map.get(code_pfx, {})
        t_low   = td.get('low', 0.0)
        t_close = td.get('close', 0.0)
        t_1_low = float(r['t_1_low']) if r['t_1_low'] else 0.0

        if t_low <= 0 or t_1_low <= 0:
            passed = False
        else:
            passed = t_low > t_1_low

        results.append({
            'code':          r['code'],
            'passed':        passed,
            't_low':         round(t_low, 2),
            'current_price': round(t_close, 2),
            'period':        period,
        })

    return jsonify({'ok': True, 'results': results, 'period': period})


@app.route('/month_low')


@app.route('/api/month_low/dates')
def api_ml_dates():
    return jsonify(get_month_low_dates(20))


@app.route('/api/month_low/<scan_date>')
def api_ml_results(scan_date):
    return jsonify(get_month_low_results(scan_date))


@app.route('/api/month_low/run', methods=['POST'])
def api_ml_run():
    if _ml_state['running']:
        return jsonify({'ok': False, 'msg': '扫描正在进行中'})

    data = request.json or {}
    lookback_days = int(data.get('lookback_days', 10))
    if not (1 <= lookback_days <= 250):
        return jsonify({'ok': False, 'msg': 'lookback_days 需在 1~250 之间'})

    def _run():
        _ml_state['running'] = True
        while not _ml_state['log'].empty():
            _ml_state['log'].get_nowait()
        try:
            proc = subprocess.Popen(
                [PYTHON, MONTH_LOW, '--lookback', str(lookback_days)],
                stdout=subprocess.PIPE,
                stderr=subprocess.STDOUT,
                text=True, bufsize=1,
            )
            for line in proc.stdout:
                _ml_state['log'].put(line.rstrip())
            proc.wait()
            _ml_state['log'].put(f'__DONE__:{proc.returncode}')
        except Exception as e:
            _ml_state['log'].put(f'[错误] {e}')
            _ml_state['log'].put('__DONE__:1')
        finally:
            _ml_state['running'] = False

    threading.Thread(target=_run, daemon=True).start()
    return jsonify({'ok': True})


@app.route('/api/month_low/log')
def api_ml_log():
    def stream():
        yield 'data: __START__\n\n'
        while True:
            try:
                line = _ml_state['log'].get(timeout=30)
                yield f'data: {line}\n\n'
                if line.startswith('__DONE__'):
                    break
            except queue.Empty:
                yield 'data: __HEARTBEAT__\n\n'
    return Response(stream(), mimetype='text/event-stream')


@app.route('/api/month_low/status')
def api_ml_status():
    return jsonify({'running': _ml_state['running']})
    return jsonify({'running': _scan_state['running']})


@app.route('/api/run_scan', methods=['POST'])
def api_run_scan():
    if _scan_state['running']:
        return jsonify({'ok': False, 'msg': '扫描正在进行中'})
    
    def _run():
        _scan_state['running'] = True
        # 清空旧日志
        while not _scan_state['log'].empty():
            _scan_state['log'].get_nowait()
        try:
            proc = subprocess.Popen(
                [PYTHON, ALPHA2],
                stdout=subprocess.PIPE,
                stderr=subprocess.STDOUT,
                text=True,
                bufsize=1,
            )
            for line in proc.stdout:
                _scan_state['log'].put(line.rstrip())
            proc.wait()
            _scan_state['log'].put(f'__DONE__:{proc.returncode}')
        except Exception as e:
            _scan_state['log'].put(f'[错误] {e}')
            _scan_state['log'].put('__DONE__:1')
        finally:
            _scan_state['running'] = False

    threading.Thread(target=_run, daemon=True).start()
    return jsonify({'ok': True})


@app.route('/api/scan_log')
def api_scan_log():
    """SSE 实时推送扫描日志"""
    def stream():
        yield 'data: __START__\n\n'
        while True:
            try:
                line = _scan_state['log'].get(timeout=30)
                yield f'data: {line}\n\n'
                if line.startswith('__DONE__'):
                    break
            except queue.Empty:
                yield 'data: __HEARTBEAT__\n\n'
    return Response(stream(), mimetype='text/event-stream')


if __name__ == '__main__':
    app.run(host='0.0.0.0', port=5001, debug=True)
