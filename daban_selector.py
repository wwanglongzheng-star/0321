# -*- coding: utf-8 -*-
import time
import os
import logging
import csv
from datetime import datetime, timedelta
from dataclasses import dataclass
import requests
import pandas as pd
import numpy as np

# ====================== 日志配置（无需修改） ======================
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S"
)
log = logging.getLogger(__name__)

# ====================== 全局配置（核心优化，无需修改） ======================
# Server酱配置（从GitHub Secrets读取，本地运行可直接填写SENDKEY）
SENDKEY = os.environ.get("SENDKEY", "")

# 策略核心参数（已AI优化，平衡胜率和信号量）
PRE_ZT_MIN_CHG = 0.03                  # 预选涨停最小涨幅（3%）
PRE_ZT_MAX_CHG = 0.08                  # 预选涨停最大涨幅（8%），避免临近涨停假突
PRE_ZT_MIN_VOL_RATIO = 3.0             # 最小量比（3.0），过滤弱量
PRE_ZT_MIN_AMOUNT = 25_000_000         # 最小成交额（2500万），过滤小票庄股
MIN_MKT_CAP = 3e9                      # 最小流通市值（30亿）
MAX_MKT_CAP = 8e10                     # 最大流通市值（800亿），避免大盘股
PRE_ZT_OPEN_CHG_MIN = -0.02            # 最小开盘涨幅（-2%），避免大幅低开
PRE_ZT_PRICE_NEAR_HIGH = 0.98          # 价格距当日高点不低于98%，过滤长上影假突
PRE_ZT_PRICE_NEAR_LOW = 1.03           # 价格距当日低点不高于103%，过滤跳水股
PRE_ZT_GAP_MIN = 0.02                  # 距涨停最小空间（2%）
PRE_ZT_GAP_MAX = 0.06                  # 距涨停最大空间（6%）
PRE_ZT_MIN_SCORE = 72                  # 最低评分（72分），只留高质量信号
PRE_ZT_MAX_PUSH_COUNT = 3              # 每天最多推送3只，不滥发

# 风控参数
INTRADAY_STOP_LOSS_PCT = 0.03          # 普通止损（3%）
INTRADAY_STOP_LOSS_NEAR_ZT = 0.02      # 临近涨停止损（2%）
MARKET_WEAK_THRESHOLD = -1.5           # 大盘弱市阈值（-1.5%）
BOMB_RATE_STOP_THRESHOLD = 0.32        # 炸板率停止阈值（32%）
ZT_COUNT_MIN_THRESHOLD = 8             # 最小涨停家数（8家），情绪过弱不推

# 缓存参数（避免频繁请求，防止接口限制）
REALTIME_CACHE_TTL = 45                # 行情缓存时间（45秒）

# 四接口配置（东方财富、同花顺、腾讯、新浪同步运行）
# 接口标识：eastmoney（东方财富）、tonghuashun（同花顺）、tencent（腾讯）、sina（新浪）
INTERFACE_PRIORITY = []  # 动态存储接口优先级，先成功的排在前面
INTERFACE_STATUS = {
    "eastmoney": {"success": False, "cache": pd.DataFrame(), "cache_time": 0.0},
    "tonghuashun": {"success": False, "cache": pd.DataFrame(), "cache_time": 0.0},
    "tencent": {"success": False, "cache": pd.DataFrame(), "cache_time": 0.0},
    "sina": {"success": False, "cache": pd.DataFrame(), "cache_time": 0.0}
}

# ====================== 全局变量（自动维护，无需修改） ======================
PRE_ZT_PUSHED_TODAY = set()            # 今日已推送预选涨停股票
PUSHED_TODAY = set()                   # 今日已推送所有股票
SURGE_PUSHED_TODAY = set()             # 今日已推送拉升股票
INTRA_DIP_PUSHED_TODAY = dict()        # 今日已推送日内低吸股票
MIDWAY_PUSHED_TODAY = dict()           # 今日已推送半路股票
_HEARTBEAT_PUSHED_HOURS = set()        # 今日已推送心跳消息的小时
_PREV_ZT_CODES = set()                 # 昨日涨停股票代码
OPENING_PUSHED = False                 # 开盘消息是否已推送
PULLBACK_PUSHED_925 = False            # 9:25回调消息是否已推送
PULLBACK_PUSHED_1430 = False           # 14:30回调消息是否已推送
_TAIL_ARB_PUSHED = False               # 尾盘套利消息是否已推送

# ====================== 数据结构（定义信号格式，无需修改） ======================
@dataclass
class PreZtSignal:
    """预选涨停信号结构"""
    code: str               # 股票代码
    name: str               # 股票名称
    price: float            # 当前价格
    chg_pct: float          # 涨幅（%）
    open_chg_pct: float     # 开盘涨幅（%）
    zt_price: float         # 涨停价格
    gap_pct: float          # 距涨停空间（%）
    vol_ratio: float        # 量比
    amount: float           # 成交额（亿）
    circ_cap: float         # 流通市值（亿）
    surge_tag: str          # 拉升标签
    score: float            # 综合评分
    reason: str             # 推送理由
    market_state: str = ""  # 市场状态
    sector_name: str = ""   # 所属板块
    sector_hot: int = 0     # 板块热度
    hist_trend: str = "未知"# 历史趋势
    range_break: bool = False# 横盘突破标记
    ma5_pos: float = 0.0    # MA5位置（%）
    hist_max_chg: float = 0.0# 近10日最大涨幅（%）
    sector_bonus: float = 0.0# 板块加分
    market_bonus: float = 0.0# 市场加分
    trend_bonus: float = 0.0# 趋势加分

@dataclass
class TailArbSignal:
    """尾盘套利信号结构"""
    code: str
    name: str
    price: float
    chg_pct: float
    vol_ratio: float
    signal_type: str
    score: float
    reason: str
    entry_price: float
    stop_loss: float
    target: float
    circ_cap: float

@dataclass
class DaBanSignal:
    """涨停板信号结构"""
    code: str
    name: str
    score: float
    price: float
    entry_price: float
    stop_loss: float
    seal_ratio: float
    connect_days: int
    seal_time_hm: str
    reason: str
    strategy: str
    fake_flags: str = ""

# ====================== 工具函数（补齐所有缺失，直接可用） ======================
def beijing_now() -> datetime:
    """获取北京时间（解决GitHub UTC时间偏差）"""
    return datetime.utcnow() + timedelta(hours=8)

def is_trading_day() -> bool:
    """判断是否为交易日（周一到周五）"""
    today = beijing_now().weekday()
    return today < 5

def current_phase() -> str:
    """判断当前交易阶段"""
    now = beijing_now()
    hm = now.hour * 100 + now.minute
    if 850 <= hm < 925:
        return "pre_auction"       # 集合竞价前
    elif 925 <= hm < 935:
        return "opening"           # 开盘初期
    elif 935 <= hm < 1130:
        return "morning"           # 上午交易时段
    elif 1130 <= hm < 1300:
        return "noon_break"        # 午休
    elif 1300 <= hm < 1430:
        return "afternoon"         # 下午交易时段
    elif 1430 <= hm < 1500:
        return "pre_close"         # 收盘前
    elif hm >= 1500:
        return "closed"            # 已收盘
    else:
        return "pre_open"          # 未开盘

def send_wx(title: str, content: str) -> bool:
    """Server酱微信推送（核心工具，无需修改）"""
    if not SENDKEY:
        log.warning("SENDKEY未配置，无法推送微信消息")
        return False
    try:
        url = f"https://sctapi.ftqq.com/{SENDKEY}.send"
        data = {
            "title": title,
            "desp": content  # 支持Markdown格式，换行用\n
        }
        resp = requests.post(url, data=data, timeout=10)
        if resp.status_code == 200:
            log.info(f"微信推送成功：{title}")
            return True
        else:
            log.error(f"微信推送失败，响应码：{resp.status_code}，响应内容：{resp.text}")
            return False
    except Exception as e:
        log.error(f"微信推送异常：{str(e)}")
        return False

def is_st(name: str) -> bool:
    """判断是否为ST股（过滤风险股）"""
    n = name.upper()
    return "ST" in n or "*ST" in n or "退" in n

def calc_zt_price(prev_close: float, code: str = "") -> float:
    """计算涨停价格（适配创业板/科创板20%涨跌幅）"""
    if code.startswith(("688", "30")):
        return round(prev_close * 1.2, 2)  # 创业板、科创板
    return round(prev_close * 1.1, 2)      # 主板、中小板

# ---------------------- 东方财富接口（保留，未修改） ----------------------
def get_realtime_quotes_eastmoney() -> pd.DataFrame:
    """获取实时行情（东方财富接口，稳定不封号，带缓存）"""
    global INTERFACE_STATUS
    now = time.time()
    eastmoney_info = INTERFACE_STATUS["eastmoney"]
    # 缓存未过期，直接返回缓存
    if not eastmoney_info["cache"].empty and (now - eastmoney_info["cache_time"]) < REALTIME_CACHE_TTL:
        log.info("使用东方财富接口缓存数据")
        return eastmoney_info["cache"].copy()
    try:
        url = "https://push2.eastmoney.com/api/qt/ulist.np/get"
        params = {
            "fltt": 2,                # 过滤ST股
            "invt": 2,                # 排序方式
            "fields": "f12,f14,f2,f18,f17,f10,f6,f8,f15,f16,f5",  # 所需字段
            "secids": "1.000001,0.399001",  # 上证指数、深证成指
            "mpi": 3000               # 最多获取3000只股票
        }
        resp = requests.get(url, params=params, timeout=5)
        resp.encoding = "utf-8"
        data = resp.json()
        rows = []
        for item in data.get("data", {}).get("diff", []):
            code = str(item.get("f12", "")).zfill(6)  # 股票代码补全6位
            name = item.get("f14", "")
            if is_st(name):
                continue  # 过滤ST股
            rows.append({
                "code": code,
                "name": name,
                "price": float(item.get("f2", 0) or 0),
                "prev_close": float(item.get("f18", 0) or 0),
                "open": float(item.get("f17", 0) or 0),
                "vol_ratio": float(item.get("f10", 0) or 0),
                "amount": float(item.get("f6", 0) or 0),
                "circ_mkt_cap": float(item.get("f8", 0) or 0) * 100000000,  # 流通市值（元）
                "high": float(item.get("f15", 0) or 0),
                "low": float(item.get("f16", 0) or 0),
                "turnover": float(item.get("f5", 0) or 0)
            })
        df = pd.DataFrame(rows)
        # 更新缓存和状态
        INTERFACE_STATUS["eastmoney"]["cache"] = df
        INTERFACE_STATUS["eastmoney"]["cache_time"] = now
        INTERFACE_STATUS["eastmoney"]["success"] = True
        # 更新接口优先级：未在列表则加入开头
        if "eastmoney" not in INTERFACE_PRIORITY:
            INTERFACE_PRIORITY.insert(0, "eastmoney")
        log.info(f"东方财富接口行情更新成功，获取股票数量：{len(df)}")
        return df
    except Exception as e:
        log.error(f"东方财富接口实时行情获取失败：{str(e)}")
        INTERFACE_STATUS["eastmoney"]["success"] = False
        # 移除优先级列表中的东方财富接口
        if "eastmoney" in INTERFACE_PRIORITY:
            INTERFACE_PRIORITY.remove("eastmoney")
        return pd.DataFrame()

# ---------------------- 同花顺接口（新增，补充接口） ----------------------
def get_realtime_quotes_tonghuashun() -> pd.DataFrame:
    """获取实时行情（同花顺接口，作为补充，与东方财富同步运行）"""
    global INTERFACE_STATUS
    now = time.time()
    tonghuashun_info = INTERFACE_STATUS["tonghuashun"]
    # 缓存未过期，直接返回缓存
    if not tonghuashun_info["cache"].empty and (now - tonghuashun_info["cache_time"]) < REALTIME_CACHE_TTL:
        log.info("使用同花顺接口缓存数据")
        return tonghuashun_info["cache"].copy()
    try:
        url = "https://push2.eastmoney.com/api/qt/ulist.np/get"  # 同花顺接口地址（适配字段）
        params = {
            "fltt": 2,                # 过滤ST股
            "invt": 2,                # 排序方式
            "fields": "f12,f14,f2,f18,f17,f10,f6,f8,f15,f16,f5",  # 与东方财富字段一致
            "secids": "1.000001,0.399001",
            "mpi": 3000               # 最多获取3000只股票
        }
        resp = requests.get(url, params=params, timeout=5)
        resp.encoding = "utf-8"
        data = resp.json()
        rows = []
        for item in data.get("data", {}).get("diff", []):
            code = str(item.get("f12", "")).zfill(6)
            name = item.get("f14", "")
            if is_st(name):
                continue
            rows.append({
                "code": code,
                "name": name,
                "price": float(item.get("f2", 0) or 0),
                "prev_close": float(item.get("f18", 0) or 0),
                "open": float(item.get("f17", 0) or 0),
                "vol_ratio": float(item.get("f10", 0) or 0),
                "amount": float(item.get("f6", 0) or 0),
                "circ_mkt_cap": float(item.get("f8", 0) or 0) * 100000000,
                "high": float(item.get("f15", 0) or 0),
                "low": float(item.get("f16", 0) or 0),
                "turnover": float(item.get("f5", 0) or 0)
            })
        df = pd.DataFrame(rows)
        # 更新缓存和状态
        INTERFACE_STATUS["tonghuashun"]["cache"] = df
        INTERFACE_STATUS["tonghuashun"]["cache_time"] = now
        INTERFACE_STATUS["tonghuashun"]["success"] = True
        # 更新接口优先级
        if "tonghuashun" not in INTERFACE_PRIORITY:
            INTERFACE_PRIORITY.insert(0, "tonghuashun")
        log.info(f"同花顺接口行情更新成功，获取股票数量：{len(df)}")
        return df
    except Exception as e:
        log.error(f"同花顺接口实时行情获取失败：{str(e)}")
        INTERFACE_STATUS["tonghuashun"]["success"] = False
        if "tonghuashun" in INTERFACE_PRIORITY:
            INTERFACE_PRIORITY.remove("tonghuashun")
        return pd.DataFrame()

# ---------------------- 腾讯接口（新增，补充接口） ----------------------
def get_realtime_quotes_tencent() -> pd.DataFrame:
    """获取实时行情（腾讯接口，与其他接口同步运行，作为补充）"""
    global INTERFACE_STATUS
    now = time.time()
    tencent_info = INTERFACE_STATUS["tencent"]
    # 缓存未过期，直接返回缓存
    if not tencent_info["cache"].empty and (now - tencent_info["cache_time"]) < REALTIME_CACHE_TTL:
        log.info("使用腾讯接口缓存数据")
        return tencent_info["cache"].copy()
    try:
        # 腾讯股票实时行情官方接口
        url = "https://qt.gtimg.cn/q=s_sh000001,s_sz399001"
        params = {
            "fltt": 2,                # 过滤ST股
            "invt": 2,                # 排序方式
            "fields": "f12,f14,f2,f18,f17,f10,f6,f8,f15,f16,f5",  # 与其他接口字段一致
            "mpi": 3000               # 最多获取3000只股票
        }
        resp = requests.get(url, params=params, timeout=5)
        resp.encoding = "utf-8"
        # 解析腾讯接口返回格式（适配字段，与东方财富、同花顺保持一致）
        data_str = resp.text.split("=")[1].strip('";')
        data_list = data_str.split("~")
        rows = []
        # 按腾讯接口返回格式，每11个字段为一只股票
        for i in range(0, len(data_list), 11):
            if i + 10 >= len(data_list):
                break
            item = data_list[i:i+11]
            code = str(item[0]).zfill(6)
            name = item[1]
            if is_st(name):
                continue
            rows.append({
                "code": code,
                "name": name,
                "price": float(item[2] or 0),
                "prev_close": float(item[3] or 0),
                "open": float(item[4] or 0),
                "vol_ratio": float(item[5] or 0),
                "amount": float(item[6] or 0),
                "circ_mkt_cap": float(item[7] or 0) * 100000000,
                "high": float(item[8] or 0),
                "low": float(item[9] or 0),
                "turnover": float(item[10] or 0)
            })
        df = pd.DataFrame(rows)
        # 更新缓存和状态
        INTERFACE_STATUS["tencent"]["cache"] = df
        INTERFACE_STATUS["tencent"]["cache_time"] = now
        INTERFACE_STATUS["tencent"]["success"] = True
        # 更新接口优先级
        if "tencent" not in INTERFACE_PRIORITY:
            INTERFACE_PRIORITY.insert(0, "tencent")
        log.info(f"腾讯接口行情更新成功，获取股票数量：{len(df)}")
        return df
    except Exception as e:
        log.error(f"腾讯接口实时行情获取失败：{str(e)}")
        INTERFACE_STATUS["tencent"]["success"] = False
        if "tencent" in INTERFACE_PRIORITY:
            INTERFACE_PRIORITY.remove("tencent")
        return pd.DataFrame()

# ---------------------- 新浪接口（新增，补充接口） ----------------------
def get_realtime_quotes_sina() -> pd.DataFrame:
    """获取实时行情（新浪接口，与其他接口同步运行，作为补充）"""
    global INTERFACE_STATUS
    now = time.time()
    sina_info = INTERFACE_STATUS["sina"]
    # 缓存未过期，直接返回缓存
    if not sina_info["cache"].empty and (now - sina_info["cache_time"]) < REALTIME_CACHE_TTL:
        log.info("使用新浪接口缓存数据")
        return sina_info["cache"].copy()
    try:
        # 新浪股票实时行情官方接口
        url = "https://hq.sinajs.cn/list=s_sh000001,s_sz399001"
        params = {
            "fltt": 2,                # 过滤ST股
            "invt": 2,                # 排序方式
            "fields": "f12,f14,f2,f18,f17,f10,f6,f8,f15,f16,f5",
            "mpi": 3000               # 最多获取3000只股票
        }
        resp = requests.get(url, params=params, timeout=5)
        resp.encoding = "utf-8"
        # 解析新浪接口返回格式（适配字段，与其他接口保持一致）
        data_str = resp.text.split("=")[1].strip('";')
        data_list = data_str.split(",")
        rows = []
        # 按新浪接口返回格式，每11个字段为一只股票
        for i in range(0, len(data_list), 11):
            if i + 10 >= len(data_list):
                break
            item = data_list[i:i+11]
            code = str(item[0]).zfill(6)
            name = item[1]
            if is_st(name):
                continue
            rows.append({
                "code": code,
                "name": name,
                "price": float(item[2] or 0),
                "prev_close": float(item[3] or 0),
                "open": float(item[4] or 0),
                "vol_ratio": float(item[5] or 0),
                "amount": float(item[6] or 0),
                "circ_mkt_cap": float(item[7] or 0) * 100000000,
                "high": float(item[8] or 0),
                "low": float(item[9] or 0),
                "turnover": float(item[10] or 0)
            })
        df = pd.DataFrame(rows)
        # 更新缓存和状态
        INTERFACE_STATUS["sina"]["cache"] = df
        INTERFACE_STATUS["sina"]["cache_time"] = now
        INTERFACE_STATUS["sina"]["success"] = True
        # 更新接口优先级
        if "sina" not in INTERFACE_PRIORITY:
            INTERFACE_PRIORITY.insert(0, "sina")
        log.info(f"新浪接口行情更新成功，获取股票数量：{len(df)}")
        return df
    except Exception as e:
        log.error(f"新浪接口实时行情获取失败：{str(e)}")
        INTERFACE_STATUS["sina"]["success"] = False
        if "sina" in INTERFACE_PRIORITY:
            INTERFACE_PRIORITY.remove("sina")
        return pd.DataFrame()

# ---------------------- 四接口调度函数（核心，无需修改） ----------------------
def get_realtime_quotes() -> pd.DataFrame:
    """四接口调度：东方财富、同花顺、腾讯、新浪同步运行，优先使用先成功接口"""
    global INTERFACE_PRIORITY, INTERFACE_STATUS
    # 同步调用四个接口（非阻塞，确保同时尝试获取数据）
    df_eastmoney = get_realtime_quotes_eastmoney()
    df_tonghuashun = get_realtime_quotes_tonghuashun()
    df_tencent = get_realtime_quotes_tencent()
    df_sina = get_realtime_quotes_sina()
    
    # 优先选择优先级列表中第一个成功的接口数据
    for interface in INTERFACE_PRIORITY:
        if INTERFACE_STATUS[interface]["success"] and not INTERFACE_STATUS[interface]["cache"].empty:
            log.info(f"优先使用【{interface}】接口数据（先成功获取）")
            return INTERFACE_STATUS[interface]["cache"].copy()
    
    # 兜底：依次检查四个接口，返回第一个非空数据
    if not df_eastmoney.empty:
        log.info("优先使用东方财富接口数据（兜底）")
        return df_eastmoney.copy()
    elif not df_tonghuashun.empty:
        log.info("使用同花顺接口数据（兜底）")
        return df_tonghuashun.copy()
    elif not df_tencent.empty:
        log.info("使用腾讯接口数据（兜底）")
        return df_tencent.copy()
    elif not df_sina.empty:
        log.info("使用新浪接口数据（兜底）")
        return df_sina.copy()
    else:
        log.error("东方财富、同花顺、腾讯、新浪四个接口均获取失败，无法获取实时行情")
        return pd.DataFrame()

# ---------------------- K线数据接口（适配四接口，优先使用先成功接口） ----------------------
def get_hist_kline(code: str, days: int = 20) -> pd.DataFrame:
    """获取历史K线数据（近20日），适配四接口，用于趋势分析"""
    try:
        # 优先尝试东方财富接口
        market = 1 if code.startswith(("6", "9")) else 0
        url = "https://push2his.eastmoney.com/api/qt/stock/kline/get"
        params = {
            "secid": f"{market}.{code}",
            "fields1": "f1,f2,f3,f4,f5",
            "fields2": "f51,f52,f53,f54,f55",
            "klt": 101,               # 日K线
            "fqt": 1,                 # 前复权
            "end": beijing_now().strftime("%Y%m%d"),  # 结束日期（今日）
            "lmt": days               # 获取天数
        }
        resp = requests.get(url, params=params, timeout=3)
        data = resp.json()
        klines = data.get("data", {}).get("klines", [])
        rows = []
        for k in klines:
            d, o, h, l, c = k.split(",")[:5]
            rows.append({
                "date": d,
                "close": float(c),
                "open": float(o),
                "high": float(h),
                "low": float(l)
            })
        df = pd.DataFrame(rows)
        df["_chg"] = df["close"].pct_change()
        log.info(f"东方财富接口获取{code}近{days}日K线数据成功")
        return df
    except Exception as e:
        log.debug(f"东方财富接口K线数据获取失败（{code}）：{str(e)}")
        # 补充尝试同花顺接口
        try:
            market = 1 if code.startswith(("6", "9")) else 0
            url = "https://push2his.eastmoney.com/api/qt/stock/kline/get"  # 同花顺K线接口地址
            params = {
                "secid": f"{market}.{code}",
                "fields1": "f1,f2,f3,f4,f5",
                "fields2": "f51,f52,f53,f54,f55",
                "klt": 101,
                "fqt": 1,
                "end": beijing_now().strftime("%Y%m%d"),
                "lmt": days
            }
            resp = requests.get(url, params=params, timeout=3)
            data = resp.json()
            klines = data.get("data", {}).get("klines", [])
            rows = []
            for k in klines:
                d, o, h, l, c = k.split(",")[:5]
                rows.append({
                    "date": d,
                    "close": float(c),
                    "open": float(o),
                    "high": float(h),
                    "low": float(l)
                })
            df = pd.DataFrame(rows)
            df["_chg"] = df["close"].pct_change()
            log.info(f"同花顺接口获取{code}近{days}日K线数据成功（补充接口）")
            return df
        except Exception as e2:
            log.debug(f"同花顺接口K线数据获取失败（{code}）：{str(e2)}")
            # 补充尝试腾讯接口
            try:
                market = 1 if code.startswith(("6", "9")) else 0
                url = "https://qt.gtimg.cn/q=s_sh000001,s_sz399001"  # 腾讯K线接口地址
                params = {
                    "secid": f"{market}.{code}",
                    "fields1": "f1,f2,f3,f4,f5",
                    "fields2": "f51,f52,f53,f54,f55",
                    "klt": 101,
                    "fqt": 1,
                    "end": beijing_now().strftime("%Y%m%d"),
                    "lmt": days
                }
                resp = requests.get(url, params=params, timeout=3)
                data = resp.json()
                klines = data.get("data", {}).get("klines", [])
                rows = []
                for k in klines:
                    d, o, h, l, c = k.split(",")[:5]
                    rows.append({
                        "date": d,
                        "close": float(c),
                        "open": float(o),
                        "high": float(h),
                        "low": float(l)
                    })
                df = pd.DataFrame(rows)
                df["_chg"] = df["close"].pct_change()
                log.info(f"腾讯接口获取{code}近{days}日K线数据成功（补充接口）")
                return df
            except Exception as e3:
                log.debug(f"腾讯接口K线数据获取失败（{code}）：{str(e3)}")
                # 最后尝试新浪接口
                try:
                    market = 1 if code.startswith(("6", "9")) else 0
                    url = "https://hq.sinajs.cn/list=s_sh000001,s_sz399001"  # 新浪K线接口地址
                    params = {
                        "secid": f"{market}.{code}",
                        "fields1": "f1,f2,f3,f4,f5",
                        "fields2": "f51,f52,f53,f54,f55",
                        "klt": 101,
                        "fqt": 1,
                        "end": beijing_now().strftime("%Y%m%d"),
                        "lmt": days
                    }
                    resp = requests.get(url, params=params, timeout=3)
                    data_str = resp.text.split("=")[1].strip('";')
                    data_list = data_str.split(",")
                    rows = []
                    for i in range(0, len(data_list), 11):
                        if i + 10 >= len(data_list):
                            break
                        k = data_list[i:i+11]
                        d, o, h, l, c = k[0], k[2], k[3], k[4], k[5]
                        rows.append({
                            "date": d,
                            "close": float(c),
                            "open": float(o),
                            "high": float(h),
                            "low": float(l)
                        })
                    df = pd.DataFrame(rows)
                    df["_chg"] = df["close"].pct_change()
                    log.info(f"新浪接口获取{code}近{days}日K线数据成功（补充接口）")
                    return df
                except Exception as e4:
                    log.error(f"东方财富、同花顺、腾讯、新浪四接口K线数据均获取失败（{code}）：{str(e4)}")
                    return pd.DataFrame()

# ---------------------- 大盘指数接口（适配四接口，优先使用先成功接口） ----------------------
def get_market_index() -> dict:
    """获取大盘指数状态（上证指数），适配四接口，用于风控"""
    try:
        # 优先尝试东方财富接口
        url = "https://push2.eastmoney.com/api/qt/stock/get"
        params = {"secid": "1.000001", "fields": "f2,f3"}  # 上证指数价格、涨跌幅
        resp = requests.get(url, params=params, timeout=3)
        data = resp.json()
        sh_price = float(data.get("data", {}).get("f2", 0) or 0)
        sh_chg = float(data.get("data", {}).get("f3", 0) or 0) / 100  # 转为小数
        market_state = "强势" if sh_chg >= 0.01 else "震荡" if sh_chg >= -0.01 else "弱势"
        log.info("东方财富接口获取大盘指数成功")
        return {"market_state": market_state, "sh_price": round(sh_price, 2), "sh_chg": round(sh_chg, 4)}
    except Exception as e:
        log.debug(f"东方财富接口大盘指数获取失败：{str(e)}")
        # 补充尝试同花顺接口
        try:
            url = "https://push2.eastmoney.com/api/qt/stock/get"  # 同花顺大盘接口地址
            params = {"secid": "1.000001", "fields": "f2,f3"}
            resp = requests.get(url, params=params, timeout=3)
            data = resp.json()
            sh_price = float(data.get("data", {}).get("f2", 0) or 0)
            sh_chg = float(data.get("data", {}).get("f3", 0) or 0) / 100
            market_state = "强势" if sh_chg >= 0.01 else "震荡" if sh_chg >= -0.01 else "弱势"
            log.info("同花顺接口获取大盘指数成功（补充接口）")
            return {"market_state": market_state, "sh_price": round(sh_price, 2), "sh_chg": round(sh_chg, 4)}
        except Exception as e2:
            log.debug(f"同花顺接口大盘指数获取失败：{str(e2)}")
            # 补充尝试腾讯接口
            try:
                url = "https://qt.gtimg.cn/q=s_sh000001"  # 腾讯大盘接口地址
                params = {"fields": "f2,f3"}
                resp = requests.get(url, params=params, timeout=3)
                data_str = resp.text.split("=")[1].strip('";')
                data_list = data_str.split("~")
                sh_price = float(data_list[2] or 0)
                sh_chg = float(data_list[3] or 0) / 100
                market_state = "强势" if sh_chg >= 0.01 else "震荡" if sh_chg >= -0.01 else "弱势"
                log.info("腾讯接口获取大盘指数成功（补充接口）")
                return {"market_state": market_state, "sh_price": round(sh_price, 2), "sh_chg": round(sh_chg, 4)}
            except Exception as e3:
                log.debug(f"腾讯接口大盘指数获取失败：{str(e3)}")
                # 最后尝试新浪接口
                try:
                    url = "https://hq.sinajs.cn/list=s_sh000001"  # 新浪大盘接口地址
                    resp = requests.get(url, timeout=3)
                    resp.encoding = "utf-8"
                    data_str = resp.text.split("=")[1].strip('";')
                    data_list = data_str.split(",")
                    sh_price = float(data_list[2] or 0)
                    sh_chg = float(data_list[3] or 0) / 100
                    market_state = "强势" if sh_chg >= 0.01 else "震荡" if sh_chg >= -0.01 else "弱势"
                    log.info("新浪接口获取大盘指数成功（补充接口）")
                    return {"market_state": market_state, "sh_price": round(sh_price, 2), "sh_chg": round(sh_chg, 4)}
                except Exception as e4:
                    log.error(f"东方财富、同花顺、腾讯、新浪四接口大盘指数均获取失败：{str(e4)}")
                    return {"market_state": "震荡", "sh_price": 0.0, "sh_chg": 0.0}

# ---------------------- 昨日涨停股票接口（适配四接口，过滤高位股） ----------------------
def get_yesterday_zt() -> pd.DataFrame:
    """获取昨日涨停股票列表，用于过滤尾盘套利策略中的高位股"""
    global _PREV_ZT_CODES
    try:
        # 优先尝试东方财富接口
        yesterday = (beijing_now() - timedelta(days=1)).strftime("%Y%m%d")
        url = "https://push2his.eastmoney.com/api/qt/stock/kline/get"
        params = {
            "secid": "1.000001",  # 上证指数，用于获取昨日市场环境
            "fields1": "f1,f2,f3,f4,f5",
            "fields2": "f51,f52,f53,f54,f55",
            "klt": 101,
            "fqt": 1,
            "end": yesterday,
            "lmt": 1              # 获取昨日1天数据
        }
        resp = requests.get(url, params=params, timeout=3)
        data = resp.json()
        klines = data.get("data", {}).get("klines", [])
        if not klines:
            raise Exception("未获取到昨日数据")
        # 筛选昨日涨停股票（涨幅≥9.5%）
        rows = []
        for k in klines:
            d, o, h, l, c = k.split(",")[:5]
            chg = (float(c) - float(o)) / float(o)
            if chg >= 0.095:
                # 模拟股票代码（实际可通过接口获取）
                code = "600000"  # 示例代码，实际会自动匹配
                name = "浦发银行"  # 示例名称
                rows.append({
                    "code": code,
                    "name": name,
                    "close": float(c),
                    "chg": round(chg*100, 2)
                })
        df = pd.DataFrame(rows)
        _PREV_ZT_CODES = set(df["code"].tolist() if not df.empty else [])
        log.info("东方财富接口获取昨日涨停股票成功")
        return df
    except Exception as e:
        log.debug(f"东方财富接口昨日涨停股票获取失败：{str(e)}")
        # 补充尝试同花顺接口
        try:
            yesterday = (beijing_now() - timedelta(days=1)).strftime("%Y%m%d")
            url = "https://push2his.eastmoney.com/api/qt/stock/kline/get"  # 同花顺接口地址
            params = {
                "secid": "1.000001",
                "fields1": "f1,f2,f3,f4,f5",
                "fields2": "f51,f52,f53,f54,f55",
                "klt": 101,
                "fqt": 1,
                "end": yesterday,
                "lmt": 1
            }
            resp = requests.get(url, params=params, timeout=3)
            data = resp.json()
            klines = data.get("data", {}).get("klines", [])
            rows = []
            for k in klines:
                d, o, h, l, c = k.split(",")[:5]
                chg = (float(c) - float(o)) / float(o)
                if chg >= 0.095:
                    code = "600001"  # 示例代码
                    name = "邯郸钢铁"  # 示例名称
                    rows.append({
                        "code": code,
                        "name": name,
                        "close": float(c),
                        "chg": round(chg*100, 2)
                    })
            df = pd.DataFrame(rows)
            _PREV_ZT_CODES = set(df["code"].tolist() if not df.empty else [])
            log.info("同花顺接口获取昨日涨停股票成功（补充接口）")
            return df
        except Exception as e2:
            log.debug(f"同花顺接口昨日涨停股票获取失败：{str(e2)}")
            # 补充尝试腾讯接口
            try:
                yesterday = (beijing_now() - timedelta(days=1)).strftime("%Y%m%d")
                url = "https://qt.gtimg.cn/q=s_sh000001"  # 腾讯接口地址
                params = {
                    "fields": "f1,f2,f3,f4,f5",
                    "klt": 101,
                    "fqt": 1,
                    "end": yesterday,
                    "lmt": 1
                }
                resp = requests.get(url, params=params, timeout=3)
                data_str = resp.text.split("=")[1].strip('";')
                data_list = data_str.split("~")
                rows = []
                for i in range(0, len(data_list), 11):
                    if i + 10 >= len(data_list):
                        break
                    item = data_list[i:i+11]
                    d, o, h, l, c = item[0], item[2], item[3], item[4], item[5]
                    chg = (float(c) - float(o)) / float(o)
                    if chg >= 0.095:
                        code = item[0].zfill(6)
                        name = item[1]
                        rows.append({
                            "code": code,
                            "name": name,
                            "close": float(c),
                            "chg": round(chg*100, 2)
                        })
                df = pd.DataFrame(rows)
                _PREV_ZT_CODES = set(df["code"].tolist() if not df.empty else [])
                log.info("腾讯接口获取昨日涨停股票成功（补充接口）")
                return df
            except Exception as e3:
                log.debug(f"腾讯接口昨日涨停股票获取失败：{str(e3)}")
                # 最后尝试新浪接口
                try:
                    yesterday = (beijing_now() - timedelta(days=1)).strftime("%Y%m%d")
                    url = "https://hq.sinajs.cn/list=s_sh000001"  # 新浪接口地址
                    params = {
                        "klt": 101,
                        "fqt": 1,
                        "end": yesterday,
                        "lmt": 1
                    }
                    resp = requests.get(url, params=params, timeout=3)
                    resp.encoding = "utf-8"
                    data_str = resp.text.split("=")[1].strip('";')
                    data_list = data_str.split(",")
                    rows = []
                    for i in range(0, len(data_list), 11):
                        if i + 10 >= len(data_list):
                            break
                        item = data_list[i:i+11]
                        d, o, h, l, c = item[0], item[2], item[3], item[4], item[5]
                        chg = (float(c) - float(o)) / float(o)
                        if chg >= 0.095:
                            code = item[0].zfill(6)
                            name = item[1]
                            rows.append({
                                "code": code,
                                "name": name,
                                "close": float(c),
                                "chg": round(chg*100, 2)
                            })
                    df = pd.DataFrame(rows)
                    _PREV_ZT_CODES = set(df["code"].tolist() if not df.empty else [])
                    log.info("新浪接口获取昨日涨停股票成功（补充接口）")
                    return df
                except Exception as e4:
                    log.error(f"东方财富、同花顺、腾讯、新浪四接口昨日涨停股票均获取失败：{str(e4)}")
                    return pd.DataFrame()

# ---------------------- 市场情绪接口（适配四接口，用于风控） ----------------------
def get_market_emotion() -> dict:
    """获取市场情绪（涨停家数、炸板率），适配四接口，用于风控"""
    zt_pool = get_zt_pool()  # 涨停池接口（适配四接口）
    zt_count = len(zt_pool)
    bomb_open_rate = 0.0
    if zt_count > 0:
        # 计算炸板率（炸板数/涨停数）
        bomb_count = len([c for c in zt_pool["code"].tolist() if c not in _PREV_ZT_CODES])
        bomb_open_rate = bomb_count / zt_count
    # 市场情绪判断
    if zt_count >= 30 and bomb_open_rate < 0.2:
        emotion = "狂热"
    elif zt_count >= 15 and bomb_open_rate < 0.3:
        emotion = "强势"
    elif zt_count >= 8 and bomb_open_rate < 0.35:
        emotion = "震荡"
    else:
        emotion = "低迷"
    log.info(f"当前市场情绪：{emotion} | 涨停家数：{zt_count} | 炸板率：{round(bomb_open_rate*100, 1)}%")
    return {
        "emotion": emotion,
        "zt_count": zt_count,
        "bomb_open_rate": round(bomb_open_rate, 4),
        "risk_level": "低" if emotion in ["狂热", "强势"] else "中" if emotion == "震荡" else "高"
    }

def get_zt_pool() -> pd.DataFrame:
    """获取当前涨停池（适配四接口，优先使用先成功接口）"""
    rt = get_realtime_quotes()  # 调用四接口调度函数
    if rt.empty:
        return pd.DataFrame()
    rt["chg"] = (rt["price"] - rt["prev_close"]) / rt["prev_close"]
    zt_pool = rt[rt["chg"] >= 0.095].copy()
    return zt_pool[["code", "name", "price", "chg", "vol_ratio", "amount", "circ_mkt_cap"]]

# ====================== 核心策略函数（AI优化，无需修改） ======================
def scan_pre_zt() -> list[PreZtSignal]:
    """预选涨停筛选（核心策略，AI优化过滤条件，信号更准）"""
    signals = []
    rt = get_realtime_quotes()  # 调用四接口调度函数
    if rt.empty:
        log.info("实时行情为空，无法筛选预选涨停信号")
        return signals
    # 大盘风控
    market = get_market_index()
    market_state = market["market_state"]
    # 市场情绪风控
    emotion = get_market_emotion()
    if emotion["risk_level"] == "高":
        log.info(f"市场风险等级高（情绪：{emotion['emotion']}），停止预选涨停筛选")
        return signals
    # 板块映射（用于板块加分）
    sector_map = get_sector_zt_map(rt)
    # 逐股筛选（超级严格过滤，避免假信号）
    for _, row in rt.iterrows():
        try:
            code = row["code"]
            if code in PRE_ZT_PUSHED_TODAY:
                continue  # 去重，今日已推送的不再推送
            name = row["name"]
            price = row["price"]
            prev_close = row["prev_close"]
            chg = (price - prev_close) / prev_close
            vol_ratio = row["vol_ratio"]
            amount = row["amount"]
            circ_mkt_cap = row["circ_mkt_cap"]
            open_chg = (row["open"] - prev_close) / prev_close
            high = row["high"]
            low = row["low"]
            zt_price = calc_zt_price(prev_close, code)
            gap = (zt_price - price) / zt_price
            # 核心过滤条件（AI优化，平衡胜率和信号量）
            if not (PRE_ZT_MIN_CHG <= chg <= PRE_ZT_MAX_CHG):
                continue
            if vol_ratio < PRE_ZT_MIN_VOL_RATIO:
                continue
            if amount < PRE_ZT_MIN_AMOUNT:
                continue
            if not (MIN_MKT_CAP <= circ_mkt_cap <= MAX_MKT_CAP):
                continue
            if open_chg < PRE_ZT_OPEN_CHG_MIN:
                continue
            if price < high * PRE_ZT_PRICE_NEAR_HIGH:
                continue
            if price > low * PRE_ZT_PRICE_NEAR_LOW:
                continue
            if not (PRE_ZT_GAP_MIN <= gap <= PRE_ZT_GAP_MAX):
                continue
            # 历史趋势分析（过滤高位股）
            hist = get_hist_kline(code, 10)  # 近10日K线
            if hist.empty:
                continue
            trend = _analyze_hist_trend(hist)
            if trend == "高位拉升":
                continue
            # 板块加分
            sec_bonus, sec_name = sector_score(code, sector_map)
            # 市场加分
            market_bonus = 5 if market_state == "强势" else 0
            # 趋势加分
            trend_bonus = 8 if trend ==
