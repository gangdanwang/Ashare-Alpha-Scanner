import requests
import time
import datetime
import argparse
import pandas as pd
from concurrent.futures import ThreadPoolExecutor
from Ashare import get_price
from Ddemo3_config import CONFIG as DDEMO_CONFIG
from Alpha3_config import CONFIG

HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/120.0.0.0 Safari/537.36"
    ),
    "Referer": "https://quote.eastmoney.com/",
}

FS_MAP = {
    "industry": "m:90+t:2",
    "concept":  "m:90+t:3",
    "region":   "m:90+t:1",
}

TYPE_LABEL = {"industry": "行业", "concept": "概念", "region": "地域"}


# =============================
# 1. 拉板块列表（含板块代码 f12）
# =============================
def _fetch_sector_list(sector_type: str) -> list[dict]:
    fs = FS_MAP.get(sector_type, "m:90+t:3")
    url = (
        "http://push2.eastmoney.com/api/qt/clist/get"
        "?pn=1&pz=300&po=1&np=1"
        "&ut=bd1d9ddb04089700cf9c27f6f7426281"
        "&fltt=2&invt=2&fid=f3"
        f"&fs={fs}"
        "&fields=f2,f3,f4,f6,f8,f12,f14"
    )
    try:
        items = requests.get(url, headers=HEADERS, timeout=10).json()["data"]["diff"]
    except Exception as e:
        print(f"[{sector_type}] 板块列表请求失败: {e}")
        return []

    result = []
    for item in items:
        result.append({
            "板块类型":   TYPE_LABEL.get(sector_type, sector_type),
            "板块代码":   item.get("f12", ""),
            "板块名称":   item.get("f14", "-"),
            "涨跌幅(%)":  item.get("f3", 0),
            "最新价":     item.get("f2", "-"),
            "涨跌额":     item.get("f4", 0),
            "成交额(亿)": round(item.get("f6", 0) / 1e8, 2),
            "换手率(%)":  item.get("f8", 0),
        })
    return result


# =============================
# 2. 拉今日涨停池，返回 {股票代码: 连板数} 映射
# =============================
def _fetch_zt_pool() -> dict[str, int]:
    today = datetime.date.today().strftime("%Y%m%d")
    url = (
        f"http://push2ex.eastmoney.com/getTopicZTPool"
        f"?ut=7eea3edcaed734bea9cbfc24409ed989"
        f"&dpt=wz.ztzt&Pageindex=0&pagesize=500&sort=fbt:asc&date={today}"
    )
    try:
        pool = requests.get(url, headers=HEADERS, timeout=10).json()["data"]["pool"]
        return {item["c"]: item.get("lbc", 1) for item in pool}
    except Exception as e:
        print(f"涨停池请求失败: {e}")
        return {}


# =============================
# 3. 拉单个板块内所有个股代码
# =============================
def _fetch_bk_stocks(bk_code: str) -> set[str]:
    url = (
        f"http://push2.eastmoney.com/api/qt/clist/get"
        f"?pn=1&pz=500&po=1&np=1"
        f"&ut=bd1d9ddb04089700cf9c27f6f7426281"
        f"&fltt=2&invt=2&fid=f3"
        f"&fs=b:{bk_code}"
        f"&fields=f12"
    )
    try:
        diff = requests.get(url, headers=HEADERS, timeout=10).json()["data"]["diff"]
        return {item["f12"] for item in diff}
    except Exception:
        return set()


# =============================
# 4. 主函数
# =============================
def get_sector_rank(top_n: int = None, sector_types: list = None) -> pd.DataFrame:
    cfg1 = CONFIG["layer1"]
    if sector_types is None:
        sector_types = cfg1["sector_types"]
    if top_n is None:
        top_n = cfg1["top_n"]

    # 拉各类型板块列表
    all_sectors = []
    for t in sector_types:
        all_sectors.extend(_fetch_sector_list(t))

    if not all_sectors:
        return pd.DataFrame()

    all_keywords = cfg1["style_keywords"] + cfg1["defensive_keywords"] + cfg1["commodity_keywords"]

    df = pd.DataFrame(all_sectors)
    df = df.sort_values("涨跌幅(%)", ascending=False)
    mask = df["板块名称"].apply(lambda n: not any(k in n for k in all_keywords))
    df = df[mask]
    df = df.drop_duplicates(subset=["板块名称"], keep="first")
    df = df.drop_duplicates(subset=["涨跌幅(%)", "最新价", "涨跌额", "成交额(亿)"], keep="first")
    top_df = df.head(top_n).copy()

    # 拉涨停池
    zt_map = _fetch_zt_pool()
    if not zt_map:
        top_df["涨停数"] = "-"
        top_df["连板数"] = "-"
        top_df = top_df.reset_index(drop=True)
        top_df.index += 1
        return top_df

    # 并发拉各板块内个股，统计涨停/连板
    bk_codes = top_df["板块代码"].tolist()
    with ThreadPoolExecutor(max_workers=10) as ex:
        stock_sets = list(ex.map(_fetch_bk_stocks, bk_codes))

    zt_counts, lbc_counts = [], []
    for stocks in stock_sets:
        zt_in_bk = {c: zt_map[c] for c in stocks if c in zt_map}
        zt_counts.append(len(zt_in_bk))
        lbc_counts.append(sum(1 for v in zt_in_bk.values() if v >= 2))

    top_df["涨停数"] = zt_counts
    top_df["连板数"] = lbc_counts
    # 保留板块代码供后续层使用，展示时再按需 drop
    top_df = top_df.reset_index(drop=True)
    top_df.index += 1
    return top_df


def _print_df(df: pd.DataFrame):
    # 展示时隐藏内部字段
    show = df.drop(columns=["板块代码"], errors="ignore")
    with pd.option_context(
        "display.max_rows", None,
        "display.max_columns", None,
        "display.width", 140,
        "display.unicode.east_asian_width", True,
    ):
        print(show.to_string())


# =============================
# 第二层筛选：量化条件精选
# =============================
def second_filter(df: pd.DataFrame, top_n: int = None) -> pd.DataFrame:
    cfg2 = CONFIG["layer2"]
    if top_n is None:
        top_n = cfg2["top_n"]

    d = df[df["涨停数"] != "-"].copy()
    d["涨停数"] = d["涨停数"].astype(int)
    d["连板数"] = d["连板数"].astype(int)

    cond = (
        (d["涨停数"] >= cfg2["zt_min"]) &
        (d["换手率(%)"] > cfg2["hs_min"]) & (d["换手率(%)"] < cfg2["hs_max"]) &
        (d["连板数"] >= cfg2["lbc_min"]) & (d["连板数"] <= cfg2["lbc_max"]) &
        (d["成交额(亿)"] > cfg2["amount_min"])
    )
    result = d[cond].head(top_n).copy()
    result = result.reset_index(drop=True)
    result.index += 1
    return result


# =============================
# 第三层筛选：板块内个股精选
# =============================

def _fetch_bk_stocks_realtime(bk_code: str) -> list[dict]:
    """拉板块内个股实时行情（涨跌幅/量比/换手率/流通市值）。"""
    url = (
        f"http://push2.eastmoney.com/api/qt/clist/get"
        f"?pn=1&pz=500&po=1&np=1"
        f"&ut=bd1d9ddb04089700cf9c27f6f7426281"
        f"&fltt=2&invt=2&fid=f3"
        f"&fs=b:{bk_code}"
        # f3=涨跌幅 f8=换手率 f9=量比 f12=代码 f13=市场(0深/1沪) f14=名称 f21=流通市值
        f"&fields=f3,f8,f9,f12,f13,f14,f21"
    )
    try:
        diff = requests.get(url, headers=HEADERS, timeout=10).json()["data"]["diff"]
        return diff or []
    except Exception:
        return []


def third_filter(df2: pd.DataFrame) -> pd.DataFrame:
    cfg3 = CONFIG["layer3"]

    rows = []
    for _, sector in df2.iterrows():
        bk_code = sector.get("板块代码", "")
        bk_name = sector.get("板块名称", "")
        if not bk_code:
            continue

        stocks = _fetch_bk_stocks_realtime(bk_code)
        for s in stocks:
            market = s.get("f13", -1)
            if market not in cfg3["markets"]:
                continue
            try:
                pct  = float(s.get("f3", 0))
                hs   = float(s.get("f8", 0))
                lbr  = float(s.get("f9", 0))
                ltsz = float(s.get("f21", 0)) / 1e8
            except (TypeError, ValueError):
                continue

            if not (
                cfg3["pct_min"]    < pct  < cfg3["pct_max"] and
                cfg3["vr_min"]     < lbr  < cfg3["vr_max"] and
                cfg3["hs_min"]     < hs   < cfg3["hs_max"] and
                cfg3["mktcap_min"] < ltsz < cfg3["mktcap_max"]
            ):
                continue

            market_label = "上证" if market == 1 else "深证"
            prefix = "sh" if market == 1 else "sz"
            rows.append({
                "所属板块":    bk_name,
                "市场":        market_label,
                "代码":        f"{prefix}{s.get('f12', '')}",
                "名称":        s.get("f14", "-"),
                "涨跌幅(%)":   pct,
                "量比":        lbr,
                "换手率(%)":   hs,
                "流通市值(亿)": round(ltsz, 2),
            })

    if not rows:
        return pd.DataFrame()

    result = pd.DataFrame(rows).sort_values(["所属板块", "涨跌幅(%)"], ascending=[True, False])
    result = result.reset_index(drop=True)
    result.index += 1
    return result


# =============================
# 主入口
# =============================
if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="A股板块涨幅排行")
    parser.add_argument(
        "--type", dest="sector_types", action="append", default=None,
        choices=["industry", "concept", "region"],
        help="板块类型（可多次指定）: industry=行业, concept=概念, region=地域 (默认: concept+industry)",
    )
    parser.add_argument(
        "--top", dest="top_n", type=int, default=10,
        help="显示前 N 名 (默认: 10)",
    )
    args = parser.parse_args()

    sector_types = args.sector_types or CONFIG["layer1"]["sector_types"]
    top_n = args.top_n or CONFIG["layer1"]["top_n"]
    label = "+".join(TYPE_LABEL.get(t, t) for t in sector_types)
    print(f"\n正在获取【{label}板块】涨幅排行 Top {top_n} ...\n")

    start = time.time()
    df = get_sector_rank(top_n=top_n, sector_types=sector_types)

    # ── 第一层结果 ──
    cfg2 = CONFIG["layer2"]
    cfg3 = CONFIG["layer3"]
    print(f"【第一层筛选】涨幅 Top {top_n}（已剔除风格/防御/期货板块，去重）\n")
    if df.empty:
        print("未获取到数据")
    else:
        _print_df(df)

    # ── 第二层结果 ──
    print(f"\n【第二层筛选】涨停数≥{cfg2['zt_min']} & 换手率{cfg2['hs_min']}~{cfg2['hs_max']}% & 连板数{cfg2['lbc_min']}~{cfg2['lbc_max']} & 成交额>{cfg2['amount_min']}亿 → Top {cfg2['top_n']}\n")
    if df.empty:
        print("无数据")
        df2 = pd.DataFrame()
    else:
        df2 = second_filter(df, top_n=3)
        if df2.empty:
            print("暂无满足条件的板块")
        else:
            _print_df(df2)

    # ── 第三层结果 ──
    print(f"\n【第三层筛选】第二层板块内个股 & 涨跌幅{cfg3['pct_min']}~{cfg3['pct_max']}% & 量比{cfg3['vr_min']}~{cfg3['vr_max']} & 换手率{cfg3['hs_min']}~{cfg3['hs_max']}% & 流通市值{cfg3['mktcap_min']}~{cfg3['mktcap_max']}亿\n")
    if df2.empty:
        print("无数据（第二层无结果）")
    else:
        df3 = third_filter(df2)
        if df3.empty:
            print("暂无满足条件的个股")
        else:
            _print_df(df3)

    print(f"\n耗时: {time.time() - start:.2f} 秒")