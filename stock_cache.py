#-*- coding:utf-8 -*-
"""
stock_cache.py - 股票日线数据本地缓存模块

功能：
1. 将 API 获取的股票日线数据保存到 MySQL 数据库
2. 查询时优先读取本地缓存，缺失时再查 API
3. API 返回的数据自动增量补充到数据库

使用方式：
    from stock_cache import get_cached_price
    # 用法与 Ashare.get_price() 完全一致
    df = get_cached_price('sh600036', count=40, frequency='1d')
"""
import pandas as pd
import requests
import time
import threading
from datetime import datetime, timedelta
from db import get_conn, init_db
from Ashare import get_price as _ashare_get_price

# 全局写入锁：序列化数据库写入操作，避免死锁
_db_write_lock = threading.Lock()

# 最新交易日缓存（进程级，避免每只股票都查一次指数）
_latest_trade_date_cache: str | None = None
_latest_trade_date_lock = threading.Lock()


def _is_trading_hours() -> bool:
    """
    判断当前是否处于交易时间（工作日 09:00 ~ 15:00）。
    交易时间内当天日线数据是实时的，不应使用缓存。
    """
    now = datetime.now()
    if now.weekday() >= 5:          # 周六/周日
        return False
    t = now.time()
    return datetime.strptime('09:00', '%H:%M').time() <= t <= datetime.strptime('15:00', '%H:%M').time()

# ============================================================
# 数据库表初始化
# ============================================================
STOCK_DAILY_DDL = """
CREATE TABLE IF NOT EXISTS t_stock_daily (
    id          BIGINT AUTO_INCREMENT PRIMARY KEY,
    code        VARCHAR(12) NOT NULL COMMENT '股票代码，如 600036',
    name        VARCHAR(20) COMMENT '股票名称',
    trade_date  DATE        NOT NULL COMMENT '交易日期',
    open        DECIMAL(10,3) COMMENT '开盘价',
    close       DECIMAL(10,3) COMMENT '收盘价',
    high        DECIMAL(10,3) COMMENT '最高价',
    low         DECIMAL(10,3) COMMENT '最低价',
    volume      BIGINT      COMMENT '成交量',
    created_at  DATETIME DEFAULT CURRENT_TIMESTAMP,
    updated_at  DATETIME DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
    UNIQUE KEY uk_code_date (code, trade_date),
    INDEX idx_code (code),
    INDEX idx_date (trade_date)
) COMMENT '股票日线数据缓存';
"""


def init_stock_cache_table():
    """初始化股票日线缓存表"""
    with get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute(STOCK_DAILY_DDL)


# ============================================================
# 本地缓存查询
# ============================================================
def get_cached_daily_data(code: str, start_date: str = None, end_date: str = None) -> pd.DataFrame:
    """
    从本地数据库获取股票日线数据
    
    参数：
      code: 股票代码，如 'sh600036'
      start_date: 开始日期，如 '2024-01-01'
      end_date: 结束日期，如 '2024-04-10'
    
    返回：
      pd.DataFrame: 包含 time, open, close, high, low, volume 的日线数据
    """
    sql = "SELECT trade_date as time, open, close, high, low, volume FROM t_stock_daily WHERE code = %s"
    params = [code]
    
    if start_date:
        sql += " AND trade_date >= %s"
        params.append(start_date)
    
    if end_date:
        sql += " AND trade_date <= %s"
        params.append(end_date)
    
    sql += " ORDER BY trade_date ASC"
    
    try:
        with get_conn() as conn:
            with conn.cursor() as cur:
                cur.execute(sql, params)
                rows = cur.fetchall()
                
                if not rows:
                    return pd.DataFrame()
                
                df = pd.DataFrame(rows)
                df['time'] = pd.to_datetime(df['time'])
                df.set_index(['time'], inplace=True)
                df.index.name = ''
                
                # 确保数值类型正确
                for col in ['open', 'close', 'high', 'low', 'volume']:
                    df[col] = pd.to_numeric(df[col], errors='coerce')
                
                return df
    except Exception as e:
        print(f"⚠️  读取本地缓存失败: {e}")
        return pd.DataFrame()


# ============================================================
# 本地缓存写入
# ============================================================
def _get_stock_name_from_api(code: str) -> str:
    """
    从腾讯财经接口获取股票名称
    
    参数：
      code: 股票代码，如 'sh600036'
    
    返回：
      str: 股票名称，如 '招商银行'
    """
    try:
        url = f'http://qt.gtimg.cn/q={code}'
        text = requests.get(url, timeout=5).text
        if '~' in text:
            parts = text.split('~')
            if len(parts) > 1:
                return parts[1]
    except:
        pass
    return ''


def save_daily_data_to_cache(code: str, df: pd.DataFrame, name: str = ''):
    """
    将股票日线数据保存到本地数据库（增量更新，重复数据自动覆盖）

    参数：
      code: 股票代码，如 'sh600036' 或 '600036'
      df: 包含 open, close, high, low, volume 的 DataFrame，索引为 time
      name: 股票名称（可选）

    返回：
      int: 成功保存的记录数
    """
    if df is None or df.empty:
        return 0

    # 去掉代码前缀（sh/sz）
    clean_code = code
    if clean_code.startswith('sh'):
        clean_code = clean_code[2:]
    elif clean_code.startswith('sz'):
        clean_code = clean_code[2:]

    # 获取股票名称（如果没有传入，尝试从腾讯接口获取）
    if not name:
        try:
            name = _get_stock_name_from_api(code)
        except:
            name = ''

    # 准备数据
    df_copy = df.copy()

    # 重置索引，将 time 作为列
    if 'time' not in df_copy.columns:
        df_copy = df_copy.reset_index()
        # 处理可能的多重索引或命名问题
        if 'index' in df_copy.columns:
            df_copy.rename(columns={'index': 'time'}, inplace=True)
        elif df_copy.columns[0] == '' or df_copy.columns[0] is None:
            df_copy.rename(columns={df_copy.columns[0]: 'time'}, inplace=True)

    # 清理数据
    records = []
    for _, row in df_copy.iterrows():
        trade_date = row.get('time')
        if pd.isna(trade_date):
            continue

        # 转换为日期字符串
        if isinstance(trade_date, pd.Timestamp):
            trade_date = trade_date.strftime('%Y-%m-%d')
        else:
            trade_date = str(trade_date).split(' ')[0]

        try:
            records.append({
                'code': clean_code,
                'name': name,
                'trade_date': trade_date,
                'open': float(row.get('open', 0)),
                'close': float(row.get('close', 0)),
                'high': float(row.get('high', 0)),
                'low': float(row.get('low', 0)),
                'volume': int(float(row.get('volume', 0))),
            })
        except Exception as e:
            print(f"⚠️  处理数据行失败: {e}, 数据: {row.to_dict()}")
            continue

    if not records:
        print(f"⚠️  没有有效记录可保存: {code}")
        return 0

    # 批量插入或更新（不加全局锁，依赖 ON DUPLICATE KEY 的原子性）
    sql = """
        INSERT INTO t_stock_daily (code, name, trade_date, open, close, high, low, volume)
        VALUES (%(code)s, %(name)s, %(trade_date)s, %(open)s, %(close)s, %(high)s, %(low)s, %(volume)s)
        ON DUPLICATE KEY UPDATE
            name=VALUES(name), open=VALUES(open), close=VALUES(close), high=VALUES(high),
            low=VALUES(low), volume=VALUES(volume), updated_at=NOW()
    """

    max_retries = 3
    for attempt in range(max_retries):
        try:
            with get_conn() as conn:
                with conn.cursor() as cur:
                    cur.executemany(sql, records)
            return len(records)
        except Exception as e:
            error_code = getattr(e, 'args', [None])[0] if hasattr(e, 'args') else None
            if error_code in (1213, 1205) and attempt < max_retries - 1:
                time.sleep(0.1 * (attempt + 1))
                continue
            print(f"⚠️  数据库保存失败 ({clean_code}): {e}")
            return 0

    return 0


# ============================================================
# 对外统一接口（兼容 Ashare.get_price 用法）
# ============================================================
def _latest_trade_date() -> str:
    """
    返回最近的交易日日期字符串（YYYY-MM-DD）。
    结果在进程内缓存，多线程安全，只查一次指数接口。
    """
    global _latest_trade_date_cache
    if _latest_trade_date_cache is not None:
        return _latest_trade_date_cache
    with _latest_trade_date_lock:
        if _latest_trade_date_cache is not None:
            return _latest_trade_date_cache
        try:
            df = _ashare_get_price('sh000001', frequency='1d', count=1)
            if df is not None and not df.empty:
                _latest_trade_date_cache = df.index[-1].strftime('%Y-%m-%d')
                return _latest_trade_date_cache
        except Exception:
            pass
        _latest_trade_date_cache = datetime.now().strftime('%Y-%m-%d')
        return _latest_trade_date_cache


def get_cached_price(code: str, end_date='', count=10, frequency='1d', fields=[]) -> pd.DataFrame:
    """
    获取股票日线数据（优先本地缓存，缺失或数据不是最新时自动回源 API）
    用法与 Ashare.get_price() 完全一致。
    """
    # 规范化代码
    xcode = code.replace('.XSHG','').replace('.XSHE','')
    if 'XSHG' in code:
        xcode = 'sh' + xcode
    elif 'XSHE' in code:
        xcode = 'sz' + xcode
    elif not xcode.startswith(('sh', 'sz')):
        xcode = ('sh' if xcode.startswith('6') else 'sz') + xcode

    # 计算查询日期范围
    if end_date:
        end_dt = pd.to_datetime(end_date) if isinstance(end_date, str) else end_date
    else:
        end_dt = datetime.now()

    start_dt = end_dt - timedelta(days=count * 2 + 10)
    start_date_str = start_dt.strftime('%Y-%m-%d')
    end_date_str   = end_dt.strftime('%Y-%m-%d')

    # ── 第一步：查本地缓存 ──
    df_cache = get_cached_daily_data(xcode, start_date_str, end_date_str)

    # ── 第二步：判断缓存是否足够且是最新 ──
    cache_ok = False
    if len(df_cache) >= count:
        latest_trade = _latest_trade_date()
        cache_latest = df_cache.index[-1].strftime('%Y-%m-%d')
        # 缓存最新日期 >= 最近交易日即视为有效（历史数据不会变动）
        cache_ok = (cache_latest >= latest_trade)

    if cache_ok:
        return df_cache.tail(count)

    # ── 第三步：缓存不足或不是最新，调 Ashare API 回源 ──
    try:
        df_api = _ashare_get_price(xcode, end_date=end_date, count=count * 2, frequency=frequency)
        if df_api is None or df_api.empty:
            return df_cache.tail(count) if not df_cache.empty else pd.DataFrame()

        # 写入缓存（静默）
        save_daily_data_to_cache(xcode, df_api)

        # 重新从缓存读取
        df_merged = get_cached_daily_data(xcode, start_date_str, end_date_str)
        if not df_merged.empty:
            return df_merged.tail(count)
        return df_api.tail(count)

    except Exception:
        return df_cache.tail(count) if not df_cache.empty else pd.DataFrame()
