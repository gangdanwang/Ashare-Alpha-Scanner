"""
数据库操作模块（MySQL）
"""
import pymysql
from datetime import date, datetime
from contextlib import contextmanager

DB_CONFIG = {
    "host":    "127.0.0.1",
    "port":    3306,
    "user":    "root",
    "password":"123456",
    "db":      "alpha_scanner",
    "charset": "utf8mb4",
    "cursorclass": pymysql.cursors.DictCursor,
}

DDL = """
CREATE DATABASE IF NOT EXISTS alpha_scanner DEFAULT CHARACTER SET utf8mb4;

CREATE TABLE IF NOT EXISTS t_scan_result (
    id          BIGINT AUTO_INCREMENT PRIMARY KEY,
    trade_date  DATE        NOT NULL COMMENT 'T日',
    code        VARCHAR(12) NOT NULL COMMENT '股票代码',
    name        VARCHAR(20) COMMENT '股票名称',
    price       DECIMAL(10,3) COMMENT '收盘价',
    above_pct   DECIMAL(6,2) COMMENT '均线上方占比%',
    ma5_bias    DECIMAL(6,2) COMMENT 'MA5乖离率%',
    selected    TINYINT DEFAULT 0 COMMENT '人工筛选：0待选 1选中 2排除',
    created_at  DATETIME DEFAULT CURRENT_TIMESTAMP,
    updated_at  DATETIME DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
    UNIQUE KEY uk_date_code (trade_date, code)
) COMMENT '每日选股结果';

CREATE TABLE IF NOT EXISTS t_mock_trade (
    id          BIGINT AUTO_INCREMENT PRIMARY KEY,
    trade_date  DATE        NOT NULL COMMENT 'T日',
    code        VARCHAR(12) NOT NULL COMMENT '股票代码',
    name        VARCHAR(20) COMMENT '股票名称',
    buy_price   DECIMAL(10,3) COMMENT '买入价格',
    shares      INT COMMENT '买入股数',
    amount      DECIMAL(12,2) COMMENT '实际买入金额',
    budget      DECIMAL(12,2) DEFAULT 10000 COMMENT '预算',
    status      VARCHAR(20) DEFAULT 'pending' COMMENT 'pending/bought',
    created_at  DATETIME DEFAULT CURRENT_TIMESTAMP,
    UNIQUE KEY uk_date_code (trade_date, code)
) COMMENT '模拟买入记录';
"""


@contextmanager
def get_conn():
    conn = pymysql.connect(**DB_CONFIG)
    try:
        yield conn
        conn.commit()
    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()


def init_db():
    """初始化数据库和表结构"""
    # 先不指定 db 连接，创建数据库
    cfg = {**DB_CONFIG}
    cfg.pop("db")
    conn = pymysql.connect(**cfg)
    try:
        with conn.cursor() as cur:
            cur.execute("CREATE DATABASE IF NOT EXISTS alpha_scanner DEFAULT CHARACTER SET utf8mb4")
        conn.commit()
    finally:
        conn.close()

    with get_conn() as conn:
        with conn.cursor() as cur:
            for stmt in [s.strip() for s in DDL.split(';') if s.strip()]:
                if stmt.upper().startswith('CREATE TABLE'):
                    cur.execute(stmt)


def upsert_scan_results(trade_date: str, rows: list[dict]):
    """
    将选股结果写入数据库，同一天同一股票覆盖更新。
    rows: [{'code','name','price','above_pct','ma5_bias'}, ...]
    """
    if not rows:
        return
    sql = """
        INSERT INTO t_scan_result (trade_date, code, name, price, above_pct, ma5_bias)
        VALUES (%s, %s, %s, %s, %s, %s)
        ON DUPLICATE KEY UPDATE
            name=VALUES(name), price=VALUES(price),
            above_pct=VALUES(above_pct), ma5_bias=VALUES(ma5_bias),
            selected=0, updated_at=NOW()
    """
    with get_conn() as conn:
        with conn.cursor() as cur:
            cur.executemany(sql, [
                (trade_date, r['code'], r['name'], r['price'], r['above_pct'], r['ma5_bias'])
                for r in rows
            ])


def get_scan_results(trade_date: str) -> list[dict]:
    with get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute(
                "SELECT * FROM t_scan_result WHERE trade_date=%s ORDER BY above_pct DESC",
                (trade_date,)
            )
            return cur.fetchall()


def update_selection(record_id: int, selected: int):
    """人工筛选：selected=1选中 2排除"""
    with get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute(
                "UPDATE t_scan_result SET selected=%s WHERE id=%s",
                (selected, record_id)
            )


def insert_mock_trades(trade_date: str, budget: float = 10000.0, ids: list = None) -> list[dict]:
    """
    模拟买入。ids 不为空时只买入指定 record id；否则买入 selected=1 的全部。
    返回买入记录列表。
    """
    with get_conn() as conn:
        with conn.cursor() as cur:
            if ids:
                fmt = ','.join(['%s'] * len(ids))
                cur.execute(
                    f"SELECT * FROM t_scan_result WHERE trade_date=%s AND id IN ({fmt})",
                    [trade_date] + list(ids)
                )
            else:
                cur.execute(
                    "SELECT * FROM t_scan_result WHERE trade_date=%s AND selected=1",
                    (trade_date,)
                )
            stocks = cur.fetchall()

        trades = []
        for s in stocks:
            price  = float(s['price'])
            shares = int(budget / price / 100) * 100   # 按手（100股）取整
            if shares <= 0:
                shares = 100
            amount = round(shares * price, 2)
            trades.append({
                'trade_date': trade_date,
                'code':       s['code'],
                'name':       s['name'],
                'buy_price':  price,
                'shares':     shares,
                'amount':     amount,
                'budget':     budget,
            })

        if trades:
            sql = """
                INSERT INTO t_mock_trade
                    (trade_date, code, name, buy_price, shares, amount, budget, status)
                VALUES (%s,%s,%s,%s,%s,%s,%s,'bought')
                ON DUPLICATE KEY UPDATE
                    buy_price=VALUES(buy_price), shares=VALUES(shares),
                    amount=VALUES(amount), status='bought'
            """
            with conn.cursor() as cur:
                cur.executemany(sql, [
                    (t['trade_date'], t['code'], t['name'],
                     t['buy_price'], t['shares'], t['amount'], t['budget'])
                    for t in trades
                ])

        return trades


def get_mock_trades(trade_date: str) -> list[dict]:
    with get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute(
                "SELECT * FROM t_mock_trade WHERE trade_date=%s ORDER BY created_at DESC",
                (trade_date,)
            )
            return cur.fetchall()


def get_recent_dates(n: int = 10) -> list[str]:
    with get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute(
                "SELECT DISTINCT trade_date FROM t_scan_result ORDER BY trade_date DESC LIMIT %s",
                (n,)
            )
            return [str(r['trade_date']) for r in cur.fetchall()]
