"""
Flask Web 后端：人工二次筛选 + 模拟买入
"""
import sys, os, subprocess, threading, queue, time, requests
sys.path.insert(0, os.path.dirname(os.path.dirname(__file__)))

from flask import Flask, jsonify, request, render_template, Response
from datetime import date
from db import get_scan_results, update_selection, insert_mock_trades, get_mock_trades, get_recent_dates

app = Flask(__name__)

# 全局扫描状态
_scan_state = {
    'running': False,
    'log': queue.Queue(),
}

PYTHON = sys.executable
ALPHA2 = os.path.join(os.path.dirname(os.path.dirname(__file__)), 'Alpha2.py')


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
    trades     = insert_mock_trades(trade_date)
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
            d = requests.get(url, timeout=10, headers=headers).json()['data']
        except Exception as e:
            return jsonify({'error': str(e)}), 500
        items = d.get('diff', [])
        all_items.extend(items)
        if len(all_items) >= d['total'] or not items:
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


@app.route('/api/scan_status')
def api_scan_status():
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
