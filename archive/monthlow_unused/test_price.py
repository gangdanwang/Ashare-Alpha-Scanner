"""
test_price.py - 测试获取指定股票 T 日和 T-1 日的最高价、最低价

数据来源：
  T 日（今日实时）：腾讯行情接口 qt.gtimg.cn
    - field[33] = 今日最高价
    - field[34] = 今日最低价
    - field[3]  = 当前价格
    - field[4]  = 昨日收盘价（即 T-1 日收盘）

  T-1 日（历史日线）：Ashare get_price 日线接口
    - df.iloc[-1] = T-1 日（最新一条历史日线，不含今日）
    - high / low 字段

用法：
  python test_price.py                          # 默认测试 sh600036
  python test_price.py sh600036 sz000001        # 测试多只
"""

import sys
import requests

sys.path.insert(0, '/Users/mrfan/dev/ai-dev/Ashare/Ashare')
from Ashare import get_price


def get_today_high_low(codes: list[str]) -> dict[str, dict]:
    """
    通过腾讯实时接口获取 T 日最高价和最低价。
    返回 {code: {high, low, current, prev_close}}
    """
    code_str = ','.join(codes)
    url = f'http://qt.gtimg.cn/q={code_str}'
    result = {}
    try:
        text = requests.get(url, timeout=5).text
        for line in text.split(';'):
            if not line.strip():
                continue
            data = line.split('~')
            try:
                head = line.split('=', 1)[0].strip()
                qcode = head[2:] if head.startswith('v_') else None
                if qcode and len(data) > 34:
                    result[qcode] = {
                        'name':       data[1],
                        'current':    float(data[3])  if data[3]  else None,
                        'prev_close': float(data[4])  if data[4]  else None,
                        'high':       float(data[33]) if data[33] else None,  # T 日最高
                        'low':        float(data[34]) if data[34] else None,  # T 日最低
                    }
            except Exception:
                continue
    except Exception as e:
        print(f'实时接口请求失败: {e}')
    return result


def get_t1_high_low(code: str) -> dict | None:
    """
    通过 Ashare 日线接口获取 T-1 日最高价和最低价。

    判断逻辑：
      - 日线最后一条日期 == 今日 → T-1 日取 iloc[-2]（盘中或收盘后）
      - 日线最后一条日期 != 今日 → T-1 日取 iloc[-1]（盘前，今日K线未生成）
    """
    from datetime import datetime
    try:
        df = get_price(code, frequency='1d', count=3)
        if df is None or len(df) < 2:
            return None
        today = datetime.now().date()
        if df.index[-1].date() == today:
            row = df.iloc[-2]   # 盘中或收盘后：最后一条是今日，取倒数第二
            date_str = str(df.index[-2].date())
        else:
            row = df.iloc[-1]   # 盘前：最后一条就是上一交易日
            date_str = str(df.index[-1].date())
        return {
            'date':  date_str,
            'high':  round(float(row['high']), 2),
            'low':   round(float(row['low']),  2),
            'close': round(float(row['close']), 2),
            'open':  round(float(row['open']),  2),
        }
    except Exception as e:
        print(f'日线接口请求失败 ({code}): {e}')
        return None


def test(codes: list[str]):
    print('=' * 60)
    print('股票价格测试：T 日实时 + T-1 日历史')
    print('=' * 60)

    # 批量获取 T 日实时数据
    today_data = get_today_high_low(codes)

    for code in codes:
        print(f'\n【{code}】')

        # T 日
        td = today_data.get(code)
        if td:
            print(f'  T 日（今日实时）')
            print(f'    名称:     {td["name"]}')
            print(f'    当前价:   {td["current"]}')
            print(f'    最高价:   {td["high"]}')
            print(f'    最低价:   {td["low"]}')
            print(f'    昨收价:   {td["prev_close"]}')
        else:
            print(f'  T 日：获取失败')

        # T-1 日
        t1 = get_t1_high_low(code)
        if t1:
            print(f'  T-1 日（{t1["date"]} 历史日线）')
            print(f'    开盘价:   {t1["open"]}')
            print(f'    最高价:   {t1["high"]}')
            print(f'    最低价:   {t1["low"]}')
            print(f'    收盘价:   {t1["close"]}')
        else:
            print(f'  T-1 日：获取失败')

    print('\n' + '=' * 60)


if __name__ == '__main__':
    codes = sys.argv[1:] if len(sys.argv) > 1 else ['sh600036']
    test(codes)
