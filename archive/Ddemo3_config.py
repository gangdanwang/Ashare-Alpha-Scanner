"""
`Ddemo3.py` 策略与运行参数配置。

目标：
- 把脚本里“写死”的阈值/范围/性能参数集中管理；
- 你后续只需要改这里，不用改业务逻辑代码。

说明：
- 本配置不依赖第三方库（用 Python 文件做配置，便于写注释）。
- 金额单位：如果字段来自腾讯 `qt.gtimg.cn`，`流通市值` 在本项目按“亿元”理解。
"""

CONFIG: dict = {
    # =============================
    # 股票池配置
    # =============================
    "stock_pool": {
        # 深证股票代码范围：sz000001 ~ sz003999（不一定都存在；用于构造查询池）
        "sz_start": 1,
        "sz_end_exclusive": 4000,

        # 上证股票代码范围：sh600000 ~ sh604999
        "sh_start": 600000,
        "sh_end_exclusive": 605000,
    },

    # =============================
    # 第一阶段：实时粗筛（fast_filter）
    # =============================
    "fast_filter": {
        # 涨跌幅区间（单位：%）
        "pct_change_min": 3.0,
        "pct_change_max": 7.0,

        # 量比区间（无单位）：左开右开，例如 1.8 < 量比 < 3.5
        "volume_ratio_min": 1.8,
        "volume_ratio_max": 3.5,

        # 换手率区间（单位：%）
        "turnover_min": 3.0,
        "turnover_max": 12.0,

        # 流通市值区间（单位：亿元，来自腾讯字段 data[45]）
        "float_mktcap_min_yi": 50.0,
        "float_mktcap_max_yi": 300.0,
    },

    # =============================
    # 第二阶段：14:00 策略（after_14_filter）
    # =============================
    "after_14_filter": {
        # K线周期：传给 get_price() 的 frequency
        "frequency": "5m",

        # 拉取K线数量：传给 get_price() 的 count
        "count": 50,

        # K线最少行数：不足直接判 False（避免数据太少）
        "min_total_rows": 10,

        # 只取 >= 该时间点之后的数据段（HH:MM）
        "cutoff_hhmm": "14:30",

        # 14:00 第一根到当前最后一根的涨幅阈值（例如 0.012 = 1.2%）
        "rise_from_14_min": 0.012,

        # 分时均线（14:00 后 5 分钟 K 线收盘价的简单均价）
        "intraday_ma": {
            # 是否要求当前价在分时均线之上
            "require_price_above_average": True,
            # 现价相对分时均线的最低倍数：1.0 表示严格高于均线（实现为 现价 > 均线）；
            # 若设为 1.005 表示至少高于均线 0.5%
            "min_price_to_ma_ratio": 1.003,
        },

        # 分时数据最少条数（14:00 之后至少要有这么多根K线）
        "min_rows_after_cutoff": 2,

        # 日线均线过滤：用于“当前价 > MA5/MA10”与乖离率控制
        "daily_ma_filter": {
            # 日线回看根数（需覆盖最大均线窗口）
            "daily_count": 30,

            # 短/长均线窗口
            "ma_short_window": 5,
            "ma_long_window": 10,

            # 是否要求当前价 > MA短线
            "require_price_above_ma_short": True,
            # 是否要求当前价 > MA长线
            "require_price_above_ma_long": True,

            # 是否要求均线多头：MA短线 > MA长线（例如 MA5 > MA10）
            "require_ma_short_above_ma_long": True,

            # 乖离率上限（相对 MA短线，0.05 = 5%）
            "max_bias_to_ma_short": 0.05,
        },
    },

    # =============================
    # 性能与节流配置
    # =============================
    "performance": {
        # 实时接口每批查询数量（腾讯接口一次 q=code1,code2...）
        "batch_size": 50,

        # 并发线程数（第一阶段批量拉实时用）
        "max_workers": 10,

        # 第二阶段逐个跑 K 线时的 sleep（秒），用于节流/避免请求过快
        "sleep_between_stage2": 0.05,
    },

    # =============================
    # 输出配置
    # =============================
    "output": {
        "csv_path": "选股结果_高性能.csv",
    },
}

